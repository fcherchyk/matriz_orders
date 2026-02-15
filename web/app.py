import asyncio
import json
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from ws_listener.matriz_order_listener import MatrizOrderListener
from ws_listener.message_processor.order_message_processor import OrderMessageProcessor
from service.order_service import OrderService
from service.matriz_session import MatrizSession
from redis_client import get_redis, close_redis, RedisMarketDataSubscriber, RedisOrderEventPublisher

load_dotenv()

# Estado global
connected_clients: list[WebSocket] = []
order_message_processor: OrderMessageProcessor = None
order_listener: MatrizOrderListener = None
order_service: OrderService = None
matriz_session: MatrizSession = None
redis_market_subscriber: RedisMarketDataSubscriber = None
redis_order_publisher: RedisOrderEventPublisher = None

# Modo fijo desde env (no cambiable en runtime)
app_mode: str = os.getenv('MODE', 'paper')
order_task: Optional[asyncio.Task] = None

# Configuracion de ping
current_ping_interval: int = 10
current_ping_timeout: int = 8


async def broadcast_state(state: dict):
    """Envia estado a todos los clientes conectados"""
    for client in connected_clients.copy():
        try:
            await client.send_json({'type': 'state_update', 'data': state})
        except:
            connected_clients.remove(client)


async def on_order_status_change(status: str, message_count: int):
    """Callback cuando cambia el estado del OrderListener"""
    await broadcast_state({
        'order_listener': {
            'status': status,
            'message_count': message_count
        }
    })


async def on_order_message(message_data: dict):
    """Callback cuando llega un mensaje del OrderListener"""
    await broadcast_state({'order_message': message_data})


async def run_order_listener():
    """
    Ejecuta el order listener con logica de reintentos.
    Secuencia para cada intento:
    1. Crear listener y conectar (usando sesion compartida)
    2. Ejecutar test_order (place + cancel) para activar el listener
    3. Esperar respuesta del broker
    4. Validar receiving == True
    5. Si receiving == False, reintentar (hasta max_retries)
    6. Si receiving == True, mantener conexion y reconectar si se cae
    """
    global order_listener, order_message_processor, order_service, matriz_session
    from websockets.exceptions import ConnectionClosedError

    import sys
    print(f"[OrderListener] ========================================", flush=True)
    print(f"[OrderListener] Iniciando run_order_listener...", flush=True)
    print(f"[OrderListener] app_mode: {app_mode}", flush=True)
    print(f"[OrderListener] Usando sesion compartida: {matriz_session.session_id[:20] if matriz_session and matriz_session.session_id else 'None'}...", flush=True)
    print(f"[OrderListener] ========================================", flush=True)

    max_retries = 7

    # Loop externo: reconexion cuando la conexion se cae
    while True:
        retry_count = 0

        # Loop de reintentos hasta que receiving == True
        while retry_count < max_retries:
            listener_task = None
            try:
                print(f"[OrderListener] --- Intento {retry_count + 1}/{max_retries} ---", flush=True)

                # Refrescar sesion en cada intento
                print(f"[OrderListener] Refrescando sesion para intento {retry_count + 1}...", flush=True)
                matriz_session.refresh()
                print(f"[OrderListener] Sesion refrescada - session_id: {matriz_session.session_id[:20]}...", flush=True)

                # Crear nuevo listener con la sesion compartida
                print(f"[OrderListener] Creando MatrizOrderListener con sesion compartida...", flush=True)
                order_listener = MatrizOrderListener(
                    order_message_processor,
                    session=matriz_session,
                    ping_interval=current_ping_interval,
                    ping_timeout=current_ping_timeout
                )

                # Iniciar conexion en background
                print(f"[OrderListener] Iniciando listener_task...", flush=True)
                listener_task = asyncio.create_task(order_listener.run())

                # Esperar a que se conecte
                print(f"[OrderListener] Esperando conexion (1s)...", flush=True)
                await asyncio.sleep(1)

                # Verificar que el listener esta corriendo
                if listener_task.done():
                    print(f"[OrderListener] ERROR: listener_task termino prematuramente", flush=True)
                    exc = listener_task.exception()
                    if exc:
                        print(f"[OrderListener] Excepcion: {exc}", flush=True)
                    retry_count += 1
                    await asyncio.sleep(2)
                    continue

                # Ejecutar test_order para activar el listener
                print(f"[OrderListener] ---- PRE TEST_ORDER CHECK ----", flush=True)
                print(f"[OrderListener] order_service.mode: {order_service.mode}", flush=True)
                print(f"[OrderListener] order_listener.status_message: {order_listener.status_message}", flush=True)
                print(f"[OrderListener] ---- EJECUTANDO TEST_ORDER ----", flush=True)
                try:
                    result = order_service.test_order()
                    print(f"[OrderListener] test_order resultado: {result}", flush=True)
                except Exception as e:
                    print(f"[OrderListener] Error en test_order: {e}", flush=True)
                    import traceback
                    traceback.print_exc()

                # Esperar respuesta del broker
                print(f"[OrderListener] Esperando respuesta del broker (3s)...", flush=True)
                await asyncio.sleep(3)

                # Validar receiving
                print(f"[OrderListener] Verificando receiving: {order_listener.receiving}", flush=True)
                if order_listener.receiving:
                    print(f"[OrderListener] ========================================", flush=True)
                    print(f"[OrderListener] CONECTADO EXITOSAMENTE en intento {retry_count + 1}", flush=True)
                    print(f"[OrderListener] order_service en modo: {order_service.mode}", flush=True)
                    print(f"[OrderListener] ========================================", flush=True)

                    # Mantener el listener corriendo
                    await listener_task
                    # Si llegamos aqui, la conexion se cerro
                    print(f"[OrderListener] Conexion cerrada, reconectando...", flush=True)
                    await order_message_processor.update_status("Disconnected")
                    break  # Salir del loop de reintentos para reconectar

                else:
                    print(f"[OrderListener] receiving=False, cancelando y reintentando...", flush=True)
                    listener_task.cancel()
                    try:
                        await asyncio.wait_for(asyncio.shield(listener_task), timeout=2.0)
                    except (asyncio.CancelledError, asyncio.TimeoutError):
                        pass
                    retry_count += 1
                    await asyncio.sleep(1)
                    continue

            except ConnectionClosedError as e:
                print(f"[OrderListener] Connection closed: {e}", flush=True)
                await order_message_processor.update_status("Disconnected")
                if listener_task and not listener_task.done():
                    listener_task.cancel()
                break

            except asyncio.CancelledError:
                print("[OrderListener] Cancelled - deteniendo...", flush=True)
                if listener_task and not listener_task.done():
                    listener_task.cancel()
                return

            except Exception as e:
                print(f"[OrderListener] Error inesperado: {e}", flush=True)
                import traceback
                traceback.print_exc()
                if listener_task and not listener_task.done():
                    listener_task.cancel()
                retry_count += 1
                await asyncio.sleep(2)

        # Si agotamos los reintentos
        if retry_count >= max_retries:
            print(f"[OrderListener] ========================================", flush=True)
            print(f"[OrderListener] FALLO despues de {max_retries} intentos", flush=True)
            print(f"[OrderListener] ========================================", flush=True)
            await order_message_processor.update_status("Failed")
            await asyncio.sleep(30)

        # Esperar antes de reconectar
        print(f"[OrderListener] Esperando 5s antes de reconectar...", flush=True)
        await asyncio.sleep(5)


async def run_order_listener_dummy():
    """Ejecuta el order listener en modo dummy (para modo paper)"""
    global order_message_processor, order_service

    print(f"[OrderListener Dummy] Iniciando...")
    print(f"[OrderListener Dummy] order_message_processor: {order_message_processor}")
    print(f"[OrderListener Dummy] order_service: {order_service}")

    try:
        await order_message_processor.update_status("Paper")
        print(f"[OrderListener Dummy] Status actualizado a 'Paper'")
    except Exception as e:
        print(f"[OrderListener Dummy] Error actualizando status: {e}")
        import traceback
        traceback.print_exc()
        return

    while True:
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            print("[OrderListener Dummy] Cancelled")
            break
        except Exception as e:
            print(f"[OrderListener Dummy] Error inesperado: {e}")
            break

    print(f"[OrderListener Dummy] Finalizado")


async def on_paper_order_async(event_type: str, order):
    """
    Callback asincrono para ordenes paper.
    Simula mensajes O: del broker con formato JSON real.
    Para ordenes market, simula ejecucion inmediata usando precios del cache.
    """
    global order_message_processor, redis_market_subscriber

    if event_type == 'placed':
        # Para ordenes market (order_type == "1"), simular ejecucion inmediata
        if order.order_type == "1":
            # Obtener precio de ejecucion desde el subscriber Redis
            exec_price = redis_market_subscriber.get_execution_price(order.instrument, order.side)

            if exec_price:
                # Simular ejecucion completa (ordStatus="2", execType="F")
                order_json = {
                    "id": order.broker_order_id,
                    "ordStatus": "2",
                    "execInst": "paper",
                    "transactTime": "paper",
                    "parties": "paper",
                    "username": "paper",
                    "fixSession": "paper",
                    "securityId": order.instrument,
                    "account": order.account,
                    "symbol": "paper",
                    "origClOrdId": "paper",
                    "clOrdId": order.front_id,
                    "ordType": order.order_type,
                    "side": order.side,
                    "timeInForce": "paper",
                    "expireDate": "paper",
                    "text": "paper - Market Order Filled",
                    "orderId": "paper",
                    "displayMethod": "paper",
                    "execId": "paper",
                    "execType": "F",
                    "securityExchange": "paper",
                    "contraBroker": "paper",
                    "partyOrigTrader": "paper",
                    "tradingSessionId": "paper",
                    "ordStatusReqId": "paper",
                    "massStatusReqId": "paper",
                    "frontId": order.front_id,
                    "orderQty": str(order.quantity),
                    "price": str(exec_price),
                    "stopPx": "paper",
                    "avgPx": str(exec_price),
                    "lastPx": str(exec_price),
                    "lastQty": str(order.quantity),
                    "cumQty": str(order.quantity),
                    "displayQty": "paper",
                    "leavesQty": "0",
                    "totNumReports": "paper",
                    "lastRptRequested": "paper"
                }

                message = f"O:{json.dumps(order_json)}"
                await order_message_processor.process_direct(message)
                print(f"[PaperOrder] Market order {order.id} simulada: ejecutada a {exec_price}")
            else:
                print(f"[PaperOrder] WARNING: No hay precio disponible para {order.instrument}, orden market no ejecutada")
                # Enviar solo confirmacion sin ejecucion
                order_json = {
                    "id": order.broker_order_id,
                    "ordStatus": "0",
                    "execInst": "paper",
                    "transactTime": "paper",
                    "parties": "paper",
                    "username": "paper",
                    "fixSession": "paper",
                    "securityId": order.instrument,
                    "account": order.account,
                    "symbol": "paper",
                    "origClOrdId": "paper",
                    "clOrdId": order.front_id,
                    "ordType": order.order_type,
                    "side": order.side,
                    "timeInForce": "paper",
                    "expireDate": "paper",
                    "text": "paper - No market data available",
                    "orderId": "paper",
                    "displayMethod": "paper",
                    "execId": "paper",
                    "execType": "0",
                    "securityExchange": "paper",
                    "contraBroker": "paper",
                    "partyOrigTrader": "paper",
                    "tradingSessionId": "paper",
                    "ordStatusReqId": "paper",
                    "massStatusReqId": "paper",
                    "frontId": order.front_id,
                    "orderQty": str(order.quantity),
                    "price": "paper",
                    "stopPx": "paper",
                    "avgPx": "paper",
                    "lastPx": "paper",
                    "lastQty": "paper",
                    "cumQty": "paper",
                    "displayQty": "paper",
                    "leavesQty": str(order.quantity),
                    "totNumReports": "paper",
                    "lastRptRequested": "paper"
                }
                message = f"O:{json.dumps(order_json)}"
                await order_message_processor.process_direct(message)
        else:
            # Para ordenes limit/caution, solo enviar confirmacion
            order_json = {
                "id": order.broker_order_id,
                "ordStatus": "0",
                "execInst": "paper",
                "transactTime": "paper",
                "parties": "paper",
                "username": "paper",
                "fixSession": "paper",
                "securityId": order.instrument,
                "account": order.account,
                "symbol": "paper",
                "origClOrdId": "paper",
                "clOrdId": order.front_id,
                "ordType": order.order_type,
                "side": order.side,
                "timeInForce": "paper",
                "expireDate": "paper",
                "text": "paper",
                "orderId": "paper",
                "displayMethod": "paper",
                "execId": "paper",
                "execType": "0",
                "securityExchange": "paper",
                "contraBroker": "paper",
                "partyOrigTrader": "paper",
                "tradingSessionId": "paper",
                "ordStatusReqId": "paper",
                "massStatusReqId": "paper",
                "frontId": order.front_id,
                "orderQty": str(order.quantity),
                "price": str(order.price) if order.price else "paper",
                "stopPx": "paper",
                "avgPx": "paper",
                "lastPx": "paper",
                "lastQty": "paper",
                "cumQty": "paper",
                "displayQty": "paper",
                "leavesQty": str(order.quantity),
                "totNumReports": "paper",
                "lastRptRequested": "paper"
            }
            message = f"O:{json.dumps(order_json)}"
            await order_message_processor.process_direct(message)

    elif event_type == 'canceled':
        order_json = {
            "id": order.broker_order_id,
            "ordStatus": "4",
            "execInst": "paper",
            "transactTime": "paper",
            "parties": "paper",
            "username": "paper",
            "fixSession": "paper",
            "securityId": order.instrument,
            "account": order.account,
            "symbol": "paper",
            "origClOrdId": "paper",
            "clOrdId": order.front_id,
            "ordType": order.order_type,
            "side": order.side,
            "timeInForce": "paper",
            "expireDate": "paper",
            "text": "paper - Cancelled by Owner",
            "orderId": "paper",
            "displayMethod": "paper",
            "execId": "paper",
            "execType": "4",
            "securityExchange": "paper",
            "contraBroker": "paper",
            "partyOrigTrader": "paper",
            "tradingSessionId": "paper",
            "ordStatusReqId": "paper",
            "massStatusReqId": "paper",
            "frontId": order.front_id,
            "orderQty": str(order.quantity),
            "price": str(order.price) if order.price else "paper",
            "stopPx": "paper",
            "avgPx": "paper",
            "lastPx": "paper",
            "lastQty": "paper",
            "cumQty": "paper",
            "displayQty": "paper",
            "leavesQty": str(order.quantity),
            "totNumReports": "paper",
            "lastRptRequested": "paper"
        }
        message = f"O:{json.dumps(order_json)}"
        await order_message_processor.process_direct(message)


def on_paper_order(event_type: str, order):
    """Callback sincrono que programa el asincrono"""
    asyncio.create_task(on_paper_order_async(event_type, order))


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Maneja el ciclo de vida de la aplicacion"""
    global order_message_processor, order_task, order_service
    global matriz_session, redis_market_subscriber, redis_order_publisher

    # Crear sesion unica compartida
    print(f"[Startup] Creando sesion unica de Matriz...")
    matriz_session = MatrizSession()
    print(f"[Startup] Sesion creada - session_id: {matriz_session.session_id[:20] if matriz_session.session_id else 'None'}...")

    # Crear conexion Redis
    print(f"[Startup] Conectando a Redis...")
    redis_client = await get_redis()
    try:
        await redis_client.ping()
        print(f"[Startup] Redis conectado")
    except Exception as e:
        print(f"[Startup] WARNING: Redis no disponible: {e}")

    # Crear publisher y subscriber Redis
    redis_order_publisher = RedisOrderEventPublisher()
    redis_order_publisher.set_redis(redis_client)

    redis_market_subscriber = RedisMarketDataSubscriber()

    # Inicializar order message processor
    order_message_processor = OrderMessageProcessor(
        on_status_change=on_order_status_change,
        on_message=on_order_message
    )
    order_message_processor.set_redis_publisher(redis_order_publisher)

    # Crear order_service con la sesion compartida
    print(f"[Startup] Creando order_service...")
    order_service = OrderService(matriz_session)
    if app_mode == "prod":
        order_service.set_mode("prod")
    else:
        order_service.set_mode("paper")
        order_service.set_on_paper_order(on_paper_order)

    # Conectar dependencias
    order_message_processor.set_order_service(order_service)
    redis_market_subscriber.set_order_service(order_service)
    redis_market_subscriber.set_order_message_processor(order_message_processor)

    # Iniciar Redis market data subscriber
    redis_sub_task = asyncio.create_task(redis_market_subscriber.run(redis_client))

    # Iniciar order listener segun modo
    print(f"[Startup] Iniciando order listener en modo: {app_mode}")
    if app_mode == "prod":
        print(f"[Startup] Creando order_task con run_order_listener...")
        order_task = asyncio.create_task(run_order_listener())
    else:
        print(f"[Startup] Creando order_task con run_order_listener_dummy...")
        order_task = asyncio.create_task(run_order_listener_dummy())

    print(f"[Startup] Aplicacion iniciada exitosamente en modo: {app_mode}")

    yield

    # Cleanup
    print(f"[Shutdown] Deteniendo servicios...")
    if order_task:
        order_task.cancel()
    redis_sub_task.cancel()
    try:
        await redis_sub_task
    except asyncio.CancelledError:
        pass
    await close_redis()
    print(f"[Shutdown] Servicios detenidos")


app = FastAPI(title="Matriz Orders", lifespan=lifespan)

# Servir archivos estaticos
static_path = Path(__file__).parent / "static"
if static_path.exists():
    app.mount("/static", StaticFiles(directory=str(static_path)), name="static")


@app.get("/", response_class=HTMLResponse)
async def index():
    """Pagina principal"""
    template_path = Path(__file__).parent / "templates" / "monitor.html"
    return template_path.read_text()


@app.get("/api/status")
async def get_status():
    """Obtener estado del sistema"""
    return {
        'order_processor': order_message_processor.get_state() if order_message_processor else {},
        'mode': app_mode
    }


@app.get("/api/orders")
async def get_orders():
    """Obtener ordenes del dia filtradas por modo actual"""
    global order_service

    if not order_service:
        return {'orders': [], 'mode': app_mode}

    mode_filter = 'prod' if app_mode == 'prod' else 'paper'
    orders = order_service.repository.get_today_orders(mode=mode_filter)

    orders_data = []
    for order in orders:
        orders_data.append({
            'id': order.id,
            'broker_order_id': order.broker_order_id,
            'front_id': order.front_id,
            'account': order.account,
            'side': order.side,
            'side_text': 'COMPRA' if order.side == '1' else 'VENTA',
            'quantity': order.quantity,
            'instrument': order.instrument,
            'order_type': order.order_type,
            'order_type_text': 'MARKET' if order.order_type == '1' else 'LIMIT' if order.order_type == '2' else 'CAUTION',
            'price': order.price,
            'total_amount': order.total_amount,
            'status': order.status,
            'mode': order.mode,
            'operated_size': order.operated_size,
            'operated_volume': order.operated_volume,
            'placed': order.placed.isoformat() if order.placed else None,
            'confirmed': order.confirmed.isoformat() if order.confirmed else None,
            'partial': order.partial.isoformat() if order.partial else None,
            'filled': order.filled.isoformat() if order.filled else None,
            'canceled': order.canceled.isoformat() if order.canceled else None,
            'confirmed_canceled': order.confirmed_canceled.isoformat() if order.confirmed_canceled else None,
        })

    return {
        'orders': orders_data,
        'mode': app_mode,
        'mode_filter': mode_filter
    }


@app.get("/api/orders/config")
async def get_orders_config():
    """Obtener configuracion para el formulario de ordenes"""
    tickers = ['AL30']
    terms = ['CI', '24hs']

    return {
        'tickers': tickers,
        'terms': terms,
        'mode': app_mode
    }


@app.get("/api/session/config")
async def get_session_config():
    """Obtener configuracion de la sesion de Matriz"""
    global matriz_session

    if not matriz_session:
        return {'error': 'Sesion no disponible'}

    return matriz_session.get_account_info()


class UpdateSessionConfigRequest(BaseModel):
    min_caucion_days: int


@app.post("/api/session/config")
async def update_session_config(request: UpdateSessionConfigRequest):
    """Actualizar configuracion de la sesion (min_caucion_days)"""
    global matriz_session

    if not matriz_session:
        return {'error': 'Sesion no disponible'}

    try:
        matriz_session.set_min_caucion_days(request.min_caucion_days, persist=True)
        return {
            'success': True,
            'message': f'Dias de caucion actualizados a {request.min_caucion_days}',
            'config': matriz_session.get_account_info()
        }
    except Exception as e:
        return {'error': f'Error al actualizar configuracion: {str(e)}'}


@app.get("/api/market/book")
async def get_market_book():
    """Obtener datos del book actual para todos los instrumentos"""
    global redis_market_subscriber

    if not redis_market_subscriber:
        return {'book': {}}

    book_data = {}
    for instrument in redis_market_subscriber.get_all_instruments():
        market_data = redis_market_subscriber.get_market_data(instrument)
        if market_data:
            book_levels = redis_market_subscriber.get_book_levels(instrument, num_levels=3)
            book_data[instrument] = {
                'bid': market_data.get('bid'),
                'ask': market_data.get('ask'),
                'last': market_data.get('last'),
                'bid_size': market_data.get('bid_size'),
                'ask_size': market_data.get('ask_size'),
                'book': book_levels if book_levels else {'bids': [], 'asks': []},
                'updated_at': market_data.get('updated_at').isoformat() if market_data.get('updated_at') else None
            }

    return {'book': book_data}


class PlaceOrderRequest(BaseModel):
    ticker: str
    term: str
    side: str  # "1" = buy, "2" = sell
    order_type: str  # "1" = market, "2" = limit
    quantity: int
    price: Optional[float] = None
    price_source: Optional[str] = None


@app.post("/api/orders")
async def place_order(request: PlaceOrderRequest):
    """Colocar una nueva orden"""
    global order_service, redis_market_subscriber

    if not order_service:
        return {'error': 'OrderService no disponible'}

    try:
        instrument_short = f"{request.ticker}_{request.term}"

        # Determinar precio
        final_price = None
        if request.order_type == "2":  # Limit order
            if request.price_source and request.price_source != 'manual':
                # Obtener precio del book
                instrument = f"bm_MERV_{request.ticker}_{request.term}"
                market_data = redis_market_subscriber.get_market_data(instrument)

                if not market_data:
                    return {'error': f'No hay datos de mercado para {instrument}'}

                if request.price_source.startswith('bid'):
                    level = int(request.price_source[3]) - 1
                    book = market_data.get('book', {})
                    bids = book.get('bids', [])
                    if level < len(bids):
                        final_price = bids[level][0]
                    else:
                        return {'error': f'No hay nivel {level+1} en bid'}
                elif request.price_source.startswith('offer'):
                    level = int(request.price_source[5]) - 1
                    book = market_data.get('book', {})
                    asks = book.get('asks', [])
                    if level < len(asks):
                        final_price = asks[level][0]
                    else:
                        return {'error': f'No hay nivel {level+1} en offer'}
                else:
                    return {'error': f'price_source invalido: {request.price_source}'}
            else:
                final_price = request.price

            if not final_price:
                return {'error': 'Debe especificar un precio para orden limit'}

        # Colocar orden
        order = order_service.place_order(
            n_side=request.side,
            size=request.quantity,
            instrument=instrument_short,
            order_type=request.order_type,
            limit_price=final_price
        )

        if not order:
            return {'error': 'Error al colocar orden'}

        return {
            'success': True,
            'order': {
                'id': order.id,
                'broker_order_id': order.broker_order_id,
                'front_id': order.front_id,
                'status': order.status,
                'mode': order.mode
            },
            'message': f'Orden colocada exitosamente (modo: {order.mode})'
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {'error': f'Error al colocar orden: {str(e)}'}


@app.post("/api/orders/{order_id}/cancel")
async def cancel_order(order_id: int):
    """Cancelar una orden existente"""
    global order_service

    if not order_service:
        return {'error': 'OrderService no disponible'}

    try:
        order = order_service.repository.get_by_id(order_id)
        if not order:
            return {'error': 'Orden no encontrada'}

        canceled_order = order_service.cancel_order(order_id)
        if not canceled_order:
            return {'error': 'Error al cancelar orden'}

        return {
            'success': True,
            'order': {
                'id': canceled_order.id,
                'status': canceled_order.status
            },
            'message': 'Orden cancelada exitosamente'
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {'error': f'Error al cancelar orden: {str(e)}'}


@app.get("/api/orders/messages/{order_id}")
async def get_order_messages(order_id: str):
    """
    Buscar mensajes relacionados con un order ID en los logs del dia.
    Busca en archivos order-*.txt del dia actual.
    """
    from datetime import date

    log_dir = Path('logs/ws')
    if not log_dir.exists():
        return {'messages': [], 'error': 'No hay directorio de logs'}

    today = date.today()
    today_prefix = today.strftime('%m-%d')

    order_files = sorted(log_dir.glob(f'order-{today_prefix}*.txt'))

    if not order_files:
        return {'messages': [], 'info': 'No hay archivos de log de ordenes del dia'}

    messages = []
    for log_file in order_files:
        try:
            with open(log_file, 'r') as f:
                for line in f:
                    if order_id in line:
                        parts = line.strip().split(' ', 1)
                        if len(parts) == 2:
                            messages.append({
                                'timestamp': parts[0],
                                'content': parts[1],
                                'file': log_file.name
                            })
        except Exception as e:
            print(f"Error leyendo {log_file}: {e}")

    return {
        'order_id': order_id,
        'messages': messages,
        'files_searched': [f.name for f in order_files]
    }


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket para actualizaciones en tiempo real"""
    await websocket.accept()
    connected_clients.append(websocket)

    try:
        # Enviar estado inicial
        await websocket.send_json({
            'type': 'initial_state',
            'data': {
                'order_processor': order_message_processor.get_state() if order_message_processor else {},
                'mode': app_mode
            }
        })

        # Mantener conexion abierta
        while True:
            data = await websocket.receive_text()
    except WebSocketDisconnect:
        connected_clients.remove(websocket)
    except Exception:
        if websocket in connected_clients:
            connected_clients.remove(websocket)


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv('MONITOR_PORT', '8000'))
    uvicorn.run(app, host="0.0.0.0", port=port)
