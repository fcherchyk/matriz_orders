import asyncio
import os
import requests
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from monitoring.health_checker import HealthChecker
from monitoring.disconnection_logger import DisconnectionLogger
from ws_listener.matriz_price_listener import MatrizPriceListener
from ws_listener.matriz_order_listener import MatrizOrderListener
from ws_listener.replay_source import ReplaySource
from ws_listener.message_processor.monitor_message_processor import MonitorMessageProcessor
from ws_listener.message_processor.order_message_processor import OrderMessageProcessor
from service.order_service import OrderService
from service.matriz_session import MatrizSession
from market_data.market_data_cache import MarketDataCache

load_dotenv()

# Estado global
connected_clients: list[WebSocket] = []
health_checker: HealthChecker = None
message_processor: MonitorMessageProcessor = None
order_message_processor: OrderMessageProcessor = None
matriz_listener: MatrizPriceListener = None
order_listener: MatrizOrderListener = None
order_service: OrderService = None
disconnection_logger: DisconnectionLogger = None
replay_source: ReplaySource = None
matriz_session: MatrizSession = None  # Sesión única compartida
market_cache: MarketDataCache = None  # Cache compartido de datos de mercado

# Control de modo
# prod: listener prod + service prod
# test: listener replay + service paper
# paper: listener prod + service paper
current_mode: str = "paper"
matriz_task: Optional[asyncio.Task] = None
order_task: Optional[asyncio.Task] = None
replay_task: Optional[asyncio.Task] = None


async def broadcast_state(state: dict):
    """Envía estado a todos los clientes conectados"""
    for client in connected_clients.copy():
        try:
            await client.send_json({'type': 'state_update', 'data': state})
        except:
            connected_clients.remove(client)


async def on_health_status_change(state: dict):
    """Callback cuando cambia el estado de health checks"""
    await broadcast_state({'health': state})


async def on_ws_status_change(status: str, message_count: int):
    """Callback cuando cambia el estado del WebSocket de Matriz"""
    health_checker.update_websocket_status(status)
    await broadcast_state({
        'websocket': {
            'status': status,
            'mode': current_mode
        }
    })


async def on_ws_message(message_data: dict):
    """Callback cuando llega un mensaje del WebSocket de Matriz"""
    await broadcast_state({'message': message_data})


async def on_replay_message(message: str):
    """Callback cuando el replay envia un mensaje"""
    await message_processor.execute(message)


async def on_replay_status_change(status: str, message_count: int):
    """Callback cuando cambia el estado del replay"""
    await message_processor.update_status(status)
    await broadcast_state({
        'replay': replay_source.get_state() if replay_source else {},
        'mode': current_mode
    })


async def run_matriz_listener():
    """Ejecuta el listener de Matriz con reconexión automática"""
    global matriz_listener, message_processor
    from websockets.exceptions import ConnectionClosedError

    while True:
        try:
            await matriz_listener.run()
        except ConnectionClosedError as e:
            print(f"Connection closed: {e}. Reconnecting in 5s...")
            await message_processor.update_status("Disconnected")
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            print("Matriz listener cancelled")
            break
        except Exception as e:
            print(f"Error: {e}. Reconnecting in 5s...")
            await message_processor.update_status("Disconnected")
            await asyncio.sleep(5)


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
    Ejecuta el order listener con lógica de reintentos.
    Secuencia para cada intento:
    1. Crear listener y conectar (usando sesión compartida)
    2. Ejecutar test_order (place + cancel) para activar el listener
    3. Esperar respuesta del broker
    4. Validar receiving == True
    5. Si receiving == False, reintentar (hasta max_retries)
    6. Si receiving == True, mantener conexión y reconectar si se cae
    """
    global order_listener, order_message_processor, order_service, matriz_session
    from websockets.exceptions import ConnectionClosedError

    import sys
    print(f"[OrderListener] ========================================", flush=True)
    print(f"[OrderListener] Iniciando run_order_listener...", flush=True)
    print(f"[OrderListener] current_mode: {current_mode}", flush=True)
    print(f"[OrderListener] Usando sesión compartida: {matriz_session.session_id[:20] if matriz_session and matriz_session.session_id else 'None'}...", flush=True)
    print(f"[OrderListener] ========================================", flush=True)

    max_retries = 7

    # Loop externo: reconexión cuando la conexión se cae
    while True:
        retry_count = 0

        # Loop de reintentos hasta que receiving == True
        while retry_count < max_retries:
            listener_task = None
            try:
                print(f"[OrderListener] --- Intento {retry_count + 1}/{max_retries} ---", flush=True)

                # Refrescar sesión en cada intento (login + profile para obtener nuevas credenciales)
                print(f"[OrderListener] Refrescando sesión para intento {retry_count + 1}...", flush=True)
                matriz_session.refresh()
                print(f"[OrderListener] Sesión refrescada - session_id: {matriz_session.session_id[:20]}...", flush=True)

                # Crear nuevo listener con la sesión compartida
                print(f"[OrderListener] Creando MatrizOrderListener con sesión compartida...", flush=True)
                order_listener = MatrizOrderListener(
                    order_message_processor,
                    session=matriz_session,
                    ping_interval=current_ping_interval,
                    ping_timeout=current_ping_timeout
                )

                # Iniciar conexión en background
                print(f"[OrderListener] Iniciando listener_task...", flush=True)
                listener_task = asyncio.create_task(order_listener.run())

                # Esperar a que se conecte
                print(f"[OrderListener] Esperando conexión (1s)...", flush=True)
                await asyncio.sleep(1)

                # Verificar que el listener está corriendo
                if listener_task.done():
                    print(f"[OrderListener] ERROR: listener_task terminó prematuramente", flush=True)
                    exc = listener_task.exception()
                    if exc:
                        print(f"[OrderListener] Excepción: {exc}", flush=True)
                    retry_count += 1
                    await asyncio.sleep(2)
                    continue

                # Ejecutar test_order para activar el listener
                print(f"[OrderListener] ---- PRE TEST_ORDER CHECK ----", flush=True)
                print(f"[OrderListener] order_service.mode: {order_service.mode}", flush=True)
                print(f"[OrderListener] order_listener.status_message: {order_listener.status_message}", flush=True)
                print(f"[OrderListener] matriz_session.session_id: {matriz_session.session_id[:20] if matriz_session.session_id else 'None'}...", flush=True)
                print(f"[OrderListener] matriz_session.requester.csrf_token: {matriz_session.requester.csrf_token[:20] if matriz_session.requester.csrf_token else 'None'}...", flush=True)
                print(f"[OrderListener] matriz_session.requester.cookies: {list(matriz_session.requester.cookies.keys())}", flush=True)
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
                    # Si llegamos aquí, la conexión se cerró
                    print(f"[OrderListener] Conexión cerrada, reconectando...", flush=True)
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
                break  # Salir del loop de reintentos para reconectar

            except asyncio.CancelledError:
                print("[OrderListener] Cancelled - deteniendo...", flush=True)
                if listener_task and not listener_task.done():
                    listener_task.cancel()
                return  # Salir completamente

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
            print(f"[OrderListener] FALLÓ después de {max_retries} intentos", flush=True)
            print(f"[OrderListener] ========================================", flush=True)
            await order_message_processor.update_status("Failed")
            # Esperar antes de reintentar todo el proceso
            await asyncio.sleep(30)

        # Esperar antes de reconectar
        print(f"[OrderListener] Esperando 5s antes de reconectar...", flush=True)
        await asyncio.sleep(5)


async def run_order_listener_dummy():
    """Ejecuta el order listener en modo dummy (para modos test/paper)"""
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
    Para ordenes market, simula ejecución inmediata usando precios del cache.
    """
    global order_message_processor, market_cache
    import json

    if event_type == 'placed':
        # Para ordenes market (order_type == "1"), simular ejecución inmediata
        if order.order_type == "1":
            # Obtener precio de ejecución desde el cache
            exec_price = market_cache.get_execution_price(order.instrument, order.side)

            if exec_price:
                # Simular ejecución completa (ordStatus="2", execType="F")
                order_json = {
                    "id": order.broker_order_id,
                    "ordStatus": "2",  # Filled
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
                    "execType": "F",  # Final execution
                    "securityExchange": "paper",
                    "contraBroker": "paper",
                    "partyOrigTrader": "paper",
                    "tradingSessionId": "paper",
                    "ordStatusReqId": "paper",
                    "massStatusReqId": "paper",
                    "frontId": order.front_id,
                    "orderQty": str(order.quantity),
                    "price": str(exec_price),  # Precio de ejecución
                    "stopPx": "paper",
                    "avgPx": str(exec_price),
                    "lastPx": str(exec_price),  # Precio de esta ejecución
                    "lastQty": str(order.quantity),  # Cantidad ejecutada
                    "cumQty": str(order.quantity),  # Total ejecutado
                    "displayQty": "paper",
                    "leavesQty": "0",  # Nada queda por ejecutar
                    "totNumReports": "paper",
                    "lastRptRequested": "paper"
                }

                message = f"O:{json.dumps(order_json)}"
                await order_message_processor.process_direct(message)
                print(f"[PaperOrder] Market order {order.id} simulada: ejecutada a {exec_price}")
            else:
                print(f"[PaperOrder] WARNING: No hay precio disponible para {order.instrument}, orden market no ejecutada")
                # Enviar solo confirmación sin ejecución
                order_json = {
                    "id": order.broker_order_id,
                    "ordStatus": "0",  # Confirmada pero no ejecutada
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
            # Para ordenes limit/caution, solo enviar confirmación
            order_json = {
                "id": order.broker_order_id,
                "ordStatus": "0",  # Confirmada
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
        # ordStatus "4" indica que la cancelacion fue confirmada por el broker
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


async def on_disconnection_event(event):
    """Callback cuando ocurre un evento de desconexion/reconexion"""
    await broadcast_state({
        'disconnection_event': event.to_dict(),
        'disconnection_events': disconnection_logger.get_all_recent(2)
    })


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Maneja el ciclo de vida de la aplicación"""
    global health_checker, message_processor, order_message_processor, matriz_listener
    global disconnection_logger, replay_source, matriz_task, order_task, order_service
    global matriz_session, market_cache

    # Crear sesión única compartida
    print(f"[Startup] Creando sesión única de Matriz...")
    matriz_session = MatrizSession()
    print(f"[Startup] Sesión creada - session_id: {matriz_session.session_id[:20] if matriz_session.session_id else 'None'}...")

    # Crear market data cache compartido
    print(f"[Startup] Creando market data cache...")
    market_cache = MarketDataCache()

    # Inicializar componentes
    disconnection_logger = DisconnectionLogger(on_event=on_disconnection_event)
    health_checker = HealthChecker(
        on_status_change=on_health_status_change,
        disconnection_logger=disconnection_logger
    )
    message_processor = MonitorMessageProcessor(
        on_status_change=on_ws_status_change,
        on_message=on_ws_message
    )
    order_message_processor = OrderMessageProcessor(
        on_status_change=on_order_status_change,
        on_message=on_order_message
    )
    matriz_listener = MatrizPriceListener(
        message_processor,
        session=matriz_session,
        ping_interval=current_ping_interval,
        ping_timeout=current_ping_timeout
    )
    replay_source = ReplaySource(
        on_message=on_replay_message,
        on_status_change=on_replay_status_change
    )

    # Conectar market cache a los processors
    message_processor.set_market_cache(market_cache)
    order_message_processor.set_market_cache(market_cache)

    # Iniciar consumidor del price message processor
    await message_processor.start_consumer()

    # Iniciar tareas en background
    health_task = asyncio.create_task(health_checker.start())
    matriz_task = asyncio.create_task(run_matriz_listener())

    # Crear order_service con la sesión compartida
    print(f"[Startup] Creando order_service con sesión compartida...")
    order_service = OrderService(matriz_session)
    if current_mode == "prod":
        order_service.set_mode("prod")
    else:
        order_service.set_mode("paper")
        order_service.set_on_paper_order(on_paper_order)

    # Conectar order_service con order_message_processor
    order_message_processor.set_order_service(order_service)

    # Conectar order_service y order_message_processor con message_processor (para simular ejecuciones limit)
    message_processor.set_order_service(order_service)
    message_processor.set_order_message_processor(order_message_processor)

    # Iniciar order listener segun modo
    print(f"[Startup] Iniciando order listener en modo: {current_mode}")
    if current_mode == "prod":
        print(f"[Startup] Creando order_task con run_order_listener...")
        order_task = asyncio.create_task(run_order_listener())
    else:
        print(f"[Startup] Creando order_task con run_order_listener_dummy...")
        order_task = asyncio.create_task(run_order_listener_dummy())
    print(f"[Startup] Aplicación iniciada exitosamente en modo: {current_mode}")

    yield

    # Cleanup
    await message_processor.stop_consumer()
    health_checker.stop()
    health_task.cancel()
    if matriz_task:
        matriz_task.cancel()
    if order_task:
        order_task.cancel()
    if replay_task:
        replay_task.cancel()


app = FastAPI(title="Matriz Twin Monitor", lifespan=lifespan)

# Servir archivos estáticos
static_path = Path(__file__).parent / "static"
if static_path.exists():
    app.mount("/static", StaticFiles(directory=str(static_path)), name="static")


@app.get("/", response_class=HTMLResponse)
async def index():
    """Página principal del monitor"""
    template_path = Path(__file__).parent / "templates" / "monitor.html"
    return template_path.read_text()


@app.get("/api/status")
async def get_status():
    """Obtener estado completo del sistema"""
    return {
        'health': health_checker.get_state() if health_checker else {},
        'price_processor': message_processor.get_state() if message_processor else {},
        'order_processor': order_message_processor.get_state() if order_message_processor else {},
        'disconnection_events': disconnection_logger.get_all_recent(2) if disconnection_logger else {},
        'mode': current_mode,
        'replay': replay_source.get_state() if replay_source else {}
    }


@app.get("/api/disconnections")
async def get_disconnections():
    """Obtener eventos de desconexion recientes"""
    return disconnection_logger.get_all_recent(2) if disconnection_logger else {}


@app.get("/api/health")
async def get_health():
    """Obtener solo estado de health checks"""
    return health_checker.get_state() if health_checker else {}


@app.get("/api/messages")
async def get_messages():
    """Obtener mensajes recientes"""
    return {
        'messages': message_processor.recent_messages if message_processor else []
    }


@app.get("/api/orders")
async def get_orders():
    """Obtener órdenes del día filtradas por modo actual"""
    global order_service, current_mode

    if not order_service:
        return {'orders': [], 'mode': current_mode}

    # Filtrar por modo: prod si estamos en prod, paper si estamos en test o paper
    mode_filter = 'prod' if current_mode == 'prod' else 'paper'
    orders = order_service.repository.get_today_orders(mode=mode_filter)

    # Convertir a dict
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
        'mode': current_mode,
        'mode_filter': mode_filter
    }


@app.get("/api/orders/config")
async def get_orders_config():
    """Obtener configuración para el formulario de órdenes"""
    global message_processor

    # Obtener tickers de configuración (default: AL30)
    tickers = message_processor.tickers if message_processor else ['AL30']
    terms = ['CI', '24hs']  # Términos soportados

    return {
        'tickers': tickers,
        'terms': terms,
        'mode': current_mode
    }


@app.get("/api/session/config")
async def get_session_config():
    """Obtener configuración de la sesión de Matriz (account_id, min_caucion_days)"""
    global matriz_session

    if not matriz_session:
        return {'error': 'Sesión no disponible'}

    return matriz_session.get_account_info()


class UpdateSessionConfigRequest(BaseModel):
    min_caucion_days: int


@app.post("/api/session/config")
async def update_session_config(request: UpdateSessionConfigRequest):
    """Actualizar configuración de la sesión (min_caucion_days)"""
    global matriz_session

    if not matriz_session:
        return {'error': 'Sesión no disponible'}

    try:
        # Validar y actualizar
        matriz_session.set_min_caucion_days(request.min_caucion_days, persist=True)
        return {
            'success': True,
            'message': f'Días de caución actualizados a {request.min_caucion_days}',
            'config': matriz_session.get_account_info()
        }
    except Exception as e:
        return {'error': f'Error al actualizar configuración: {str(e)}'}


@app.get("/api/market/book")
async def get_market_book():
    """Obtener datos del book actual para todos los instrumentos"""
    global market_cache

    if not market_cache:
        return {'book': {}}

    book_data = {}
    for instrument in market_cache.get_all_instruments():
        market_data = market_cache.get_market_data(instrument)
        if market_data:
            book_levels = market_cache.get_book_levels(instrument, num_levels=3)
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
    price: Optional[float] = None  # Para limit orders
    price_source: Optional[str] = None  # "bid1", "bid2", "bid3", "offer1", "offer2", "offer3", o "manual"


@app.post("/api/orders")
async def place_order(request: PlaceOrderRequest):
    """Colocar una nueva orden"""
    global order_service, market_cache, current_mode

    if not order_service:
        return {'error': 'OrderService no disponible'}

    try:
        # Construir instrumento
        instrument_short = f"{request.ticker}_{request.term}"

        # Determinar precio
        final_price = None
        if request.order_type == "2":  # Limit order
            if request.price_source and request.price_source != 'manual':
                # Obtener precio del book
                instrument = f"bm_MERV_{request.ticker}_{request.term}"
                market_data = market_cache.get_market_data(instrument)

                if not market_data:
                    return {'error': f'No hay datos de mercado para {instrument}'}

                # Mapear price_source a precio
                if request.price_source.startswith('bid'):
                    level = int(request.price_source[3]) - 1  # bid1 -> 0, bid2 -> 1, bid3 -> 2
                    book = market_data.get('book', {})
                    bids = book.get('bids', [])
                    if level < len(bids):
                        final_price = bids[level][0]
                    else:
                        return {'error': f'No hay nivel {level+1} en bid'}
                elif request.price_source.startswith('offer'):
                    level = int(request.price_source[5]) - 1  # offer1 -> 0, offer2 -> 1, offer3 -> 2
                    book = market_data.get('book', {})
                    asks = book.get('asks', [])
                    if level < len(asks):
                        final_price = asks[level][0]
                    else:
                        return {'error': f'No hay nivel {level+1} en offer'}
                else:
                    return {'error': f'price_source inválido: {request.price_source}'}
            else:
                # Precio manual
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
        # Obtener orden
        order = order_service.repository.get_by_id(order_id)
        if not order:
            return {'error': 'Orden no encontrada'}

        # Cancelar orden
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
    import re

    log_dir = Path('logs/ws')
    if not log_dir.exists():
        return {'messages': [], 'error': 'No hay directorio de logs'}

    today = date.today()
    today_prefix = today.strftime('%m-%d')

    # Buscar archivos de orden del dia actual
    order_files = sorted(log_dir.glob(f'order-{today_prefix}*.txt'))

    if not order_files:
        return {'messages': [], 'info': 'No hay archivos de log de ordenes del dia'}

    messages = []
    for log_file in order_files:
        try:
            with open(log_file, 'r') as f:
                for line in f:
                    # Buscar el order_id en la linea (puede estar como front_id o broker_order_id)
                    if order_id in line:
                        # Parsear timestamp y mensaje
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


# ============ Control API ============

@app.get("/api/mode")
async def get_mode():
    """Obtener modo actual"""
    return {
        'mode': current_mode,
        'replay': replay_source.get_state() if replay_source else {}
    }


class SetModeRequest(BaseModel):
    mode: str  # "prod", "test" o "paper"


@app.post("/api/mode")
async def set_mode(request: SetModeRequest):
    """
    Cambiar modo de operación:
    - prod: price listener prod + order listener prod + service prod
    - test: price listener replay + order listener dummy + service paper
    - paper: price listener prod + order listener dummy + service paper

    Optimizaciones:
    - prod <-> paper: NO reinicia price listener, solo cambia order listener
    - paper <-> test: NO reinicia order listener dummy, solo cambia price listener
    """
    global current_mode, matriz_task, order_task, replay_task
    global matriz_listener, message_processor, order_message_processor, order_service, order_listener
    global matriz_session

    old_mode = current_mode
    print(f"[ModeSwitch] Iniciando cambio de modo: {old_mode} -> {request.mode}")

    if request.mode not in ["prod", "test", "paper"]:
        return {"error": "Modo inválido. Use 'prod', 'test' o 'paper'"}

    if request.mode == old_mode:
        return {"mode": current_mode, "message": "Ya está en este modo"}

    # Determinar qué componentes necesitan cambiar
    need_price_listener_change = not (
        (old_mode == "prod" and request.mode == "paper") or
        (old_mode == "paper" and request.mode == "prod")
    )
    need_order_listener_change = not (
        (old_mode == "paper" and request.mode == "test") or
        (old_mode == "test" and request.mode == "paper")
    )

    print(f"[ModeSwitch] need_price_listener_change: {need_price_listener_change}")
    print(f"[ModeSwitch] need_order_listener_change: {need_order_listener_change}")

    # === Detener componentes que necesitan cambiar ===

    # Siempre detener replay si está corriendo
    if replay_task and not replay_task.done():
        print(f"[ModeSwitch] Cancelando replay_task...")
        replay_source.stop()
        replay_task.cancel()
        try:
            await asyncio.wait_for(asyncio.shield(replay_task), timeout=2.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass
        replay_task = None
        print(f"[ModeSwitch] replay_task cancelado")

    # Price listener: solo cambiar si es necesario
    if need_price_listener_change:
        if matriz_task and not matriz_task.done():
            print(f"[ModeSwitch] Cancelando matriz_task...")
            matriz_task.cancel()
            try:
                await asyncio.wait_for(asyncio.shield(matriz_task), timeout=2.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            matriz_task = None
            print(f"[ModeSwitch] matriz_task cancelado")
        # Resetear solo el price processor
        message_processor.message_count = 0
        message_processor.recent_messages = []
    else:
        print(f"[ModeSwitch] Manteniendo price listener (no requiere cambio)")

    # Order listener: solo cambiar si es necesario
    if need_order_listener_change:
        if order_task and not order_task.done():
            print(f"[ModeSwitch] Cancelando order_task...")
            order_task.cancel()
            try:
                await asyncio.wait_for(asyncio.shield(order_task), timeout=2.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            order_task = None
            print(f"[ModeSwitch] order_task cancelado")

        # Cerrar websocket del order_listener si existe
        if order_listener:
            print(f"[ModeSwitch] Cerrando order_listener...")
            try:
                if hasattr(order_listener, 'close'):
                    await order_listener.close()
                elif hasattr(order_listener, 'websocket') and order_listener.websocket:
                    await order_listener.websocket.close()
                print(f"[ModeSwitch] order_listener cerrado")
            except Exception as e:
                print(f"[ModeSwitch] Error cerrando order_listener: {e}")
            order_listener = None

        # Resetear solo el order processor
        order_message_processor.message_count = 0
        order_message_processor.recent_messages = []
    else:
        print(f"[ModeSwitch] Manteniendo order listener dummy (no requiere cambio)")

    # === Configurar nuevo modo ===
    print(f"[ModeSwitch] Configurando modo {request.mode}...")

    # Actualizar modo del order_service
    if order_service:
        if request.mode == "prod":
            # Refrescar sesión antes de cambiar a prod para asegurar credenciales válidas
            print(f"[ModeSwitch] Refrescando sesión antes de cambiar a prod...")
            try:
                matriz_session.refresh()
            except requests.exceptions.Timeout:
                print(f"[ModeSwitch] ERROR: Timeout conectando al broker")
                return {
                    "error": "No se pudo conectar al broker: Timeout (10s). Verifique su conexión a internet y que el broker esté disponible.",
                    "mode": current_mode
                }
            except requests.exceptions.ConnectionError as e:
                print(f"[ModeSwitch] ERROR: Error de conexión: {e}")
                return {
                    "error": f"No se pudo conectar al broker: Error de conexión. Verifique su conexión a internet.",
                    "mode": current_mode
                }
            except Exception as e:
                print(f"[ModeSwitch] ERROR: No se pudo refrescar la sesión: {e}")
                import traceback
                traceback.print_exc()
                return {
                    "error": f"No se pudo conectar al broker: {str(e)}",
                    "mode": current_mode
                }
            order_service.set_mode("prod")
            order_service.set_on_paper_order(None)
        else:
            order_service.set_mode("paper")
            order_service.set_on_paper_order(on_paper_order)

    if request.mode == "prod":
        # Price listener: iniciar si es necesario
        if need_price_listener_change:
            print(f"[ModeSwitch] Creando MatrizPriceListener con sesión compartida...")
            matriz_listener = MatrizPriceListener(
                message_processor,
                session=matriz_session,
                ping_interval=current_ping_interval,
                ping_timeout=current_ping_timeout
            )
            print(f"[ModeSwitch] Iniciando matriz_task...")
            matriz_task = asyncio.create_task(run_matriz_listener())

        # Order listener: iniciar real
        if need_order_listener_change:
            print(f"[ModeSwitch] Iniciando order_task (run_order_listener)...")
            order_task = asyncio.create_task(run_order_listener())

        print(f"[ModeSwitch] Modo prod configurado exitosamente")

    elif request.mode == "test":
        # Price listener: modo replay (esperando archivos)
        if need_price_listener_change:
            print(f"[ModeSwitch] Configurando price listener para replay...")
            await message_processor.update_status("Waiting")

        # Order listener: iniciar dummy si es necesario
        if need_order_listener_change:
            print(f"[ModeSwitch] Iniciando order_task (dummy)...")
            order_task = asyncio.create_task(run_order_listener_dummy())

        print(f"[ModeSwitch] Modo test configurado exitosamente")

    else:  # paper
        # Price listener: iniciar si es necesario
        if need_price_listener_change:
            print(f"[ModeSwitch] Creando MatrizPriceListener con sesión compartida...")
            matriz_listener = MatrizPriceListener(
                message_processor,
                session=matriz_session,
                ping_interval=current_ping_interval,
                ping_timeout=current_ping_timeout
            )
            print(f"[ModeSwitch] Iniciando matriz_task...")
            matriz_task = asyncio.create_task(run_matriz_listener())

        # Order listener: iniciar dummy si es necesario
        if need_order_listener_change:
            print(f"[ModeSwitch] Iniciando order_task (dummy)...")
            order_task = asyncio.create_task(run_order_listener_dummy())

        print(f"[ModeSwitch] Modo paper configurado exitosamente")

    current_mode = request.mode
    print(f"[ModeSwitch] Cambio de modo completado: {current_mode}")
    print(f"[ModeSwitch] Estado final - matriz_task: {matriz_task}, order_task: {order_task}")
    await broadcast_state({'mode': current_mode})

    return {"mode": current_mode, "message": f"Modo cambiado a {current_mode}"}


@app.get("/api/replay/files")
async def get_replay_files():
    """Obtener archivos disponibles para replay"""
    return {
        'files': replay_source.get_available_files() if replay_source else []
    }


class StartReplayRequest(BaseModel):
    files: list[str] = []
    filename: str = None  # Mantener compatibilidad con versión anterior


@app.post("/api/replay/start")
async def start_replay(request: StartReplayRequest):
    """Iniciar reproducción de uno o más archivos"""
    global replay_task, current_mode

    if current_mode != "test":
        return {"error": "Debe estar en modo test para iniciar reproducción"}

    if replay_task and not replay_task.done():
        return {"error": "Ya hay una reproducción en curso"}

    # Determinar archivos a reproducir (soporta ambos formatos)
    files = request.files if request.files else ([request.filename] if request.filename else [])
    if not files:
        return {"error": "No se especificaron archivos"}

    # Resetear el processor
    message_processor.message_count = 0
    message_processor.recent_messages = []

    # Iniciar replay con los archivos ordenados
    replay_task = asyncio.create_task(replay_source.start_replay(files))

    return {"message": f"Reproducción iniciada: {len(files)} archivo(s)"}


@app.post("/api/replay/stop")
async def stop_replay():
    """Detener reproducción actual"""
    global replay_task

    if replay_source:
        replay_source.stop()
    if replay_task and not replay_task.done():
        replay_task.cancel()
        replay_task = None

    await message_processor.update_status("Waiting")
    return {"message": "Reproducción detenida"}


# ============ Config API ============

class ReloadConfigRequest(BaseModel):
    check_interval: int = 5
    ping_interval: int = 10
    ping_timeout: int = 8
    logging_enabled: bool = True
    tickers: list[str] = ["AL30"]
    terms: list[str] = ["CI", "24hs"]


# Configuracion global de ping
current_ping_interval: int = 10
current_ping_timeout: int = 8


@app.post("/api/config/reload")
async def reload_config(request: ReloadConfigRequest):
    """Recargar configuración y reiniciar listener"""
    global matriz_task, matriz_listener, message_processor, current_mode
    global current_ping_interval, current_ping_timeout, matriz_session

    if current_mode != "prod":
        return {"error": "Solo se puede recargar en modo prod"}

    try:
        # Actualizar configuraciones
        health_checker.check_interval = request.check_interval
        current_ping_interval = request.ping_interval
        current_ping_timeout = request.ping_timeout
        message_processor.tickers = request.tickers

        # Reiniciar el listener de Matriz
        if matriz_task and not matriz_task.done():
            matriz_task.cancel()
            matriz_task = None

        # Detener consumidor actual
        await message_processor.stop_consumer()

        # Resetear processor y crear nuevo archivo de log
        message_processor.message_count = 0
        message_processor.recent_messages = []
        message_processor.tickers = request.tickers
        message_processor.logging_enabled = request.logging_enabled
        if request.logging_enabled:
            message_processor.start_new_log_file()

        # Reiniciar consumidor
        await message_processor.start_consumer()

        # Crear nuevo listener con nueva config y sesión compartida
        matriz_listener = MatrizPriceListener(
            message_processor,
            session=matriz_session,
            ping_interval=current_ping_interval,
            ping_timeout=current_ping_timeout
        )
        matriz_listener.message_processor.tickers = request.tickers

        # Reiniciar
        matriz_task = asyncio.create_task(run_matriz_listener())

        await broadcast_state({
            'config_reloaded': True,
            'config': {
                'check_interval': request.check_interval,
                'ping_interval': request.ping_interval,
                'ping_timeout': request.ping_timeout,
                'logging_enabled': request.logging_enabled,
                'tickers': request.tickers,
                'terms': request.terms
            }
        })

        return {"message": "Configuración recargada exitosamente"}
    except Exception as e:
        return {"error": str(e)}


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
                'health': health_checker.get_state() if health_checker else {},
                'price_processor': message_processor.get_state() if message_processor else {},
                'order_processor': order_message_processor.get_state() if order_message_processor else {},
                'disconnection_events': disconnection_logger.get_all_recent(2) if disconnection_logger else {},
                'mode': current_mode,
                'replay': replay_source.get_state() if replay_source else {}
            }
        })

        # Mantener conexión abierta
        while True:
            data = await websocket.receive_text()
            # Podrías manejar comandos del cliente aquí
    except WebSocketDisconnect:
        connected_clients.remove(websocket)
    except Exception:
        if websocket in connected_clients:
            connected_clients.remove(websocket)


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv('MONITOR_PORT', '8000'))
    uvicorn.run(app, host="0.0.0.0", port=port)
