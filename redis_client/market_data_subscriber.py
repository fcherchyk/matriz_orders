import asyncio
import json
import os
from typing import Optional

import redis.asyncio as redis
from dotenv import load_dotenv

from redis_client.market_data_cache import MarketDataCache

load_dotenv()

REDIS_RECONNECT_INTERVAL = 5  # seconds between reconnection attempts


class RedisMarketDataSubscriber:
    """
    Suscribe a canales Redis del price listener (prices:{TICKER}) y actualiza el cache local.
    Tambien simula ejecuciones de ordenes limit en paper mode.

    El price listener publica mensajes JSON con formato:
    {
        "instrument": "bm_MERV_AL30_CI",
        "ticker": "AL30",
        "term": "CI",
        "data": {
            "bid": 100.5, "ask": 101.0, "last": 100.7,
            "bid_size": 1000, "ask_size": 500,
            "book": {"bids": [[100.5, 1000], ...], "asks": [[101.0, 500], ...]},
            "updated_at": "..."
        },
        "timestamp": "..."
    }
    """

    def __init__(self, tickers: list[str] = None, on_message_count=None):
        self.tickers = tickers or [t.strip() for t in os.getenv('TICKERS', 'AL30').split(',')]
        self.price_listener_url = os.getenv('PRICE_LISTENER_URL', 'http://localhost:8000')
        self.cache = MarketDataCache()
        self.order_service = None
        self.order_message_processor = None
        self.on_message_count = on_message_count
        self.message_count = 0
        self.connected = False
        self._running = False
        self._pubsub: Optional[redis.client.PubSub] = None
        self._channels: list[str] = [f"prices:{ticker}" for ticker in self.tickers]

    def set_order_service(self, order_service):
        """Configura el order service para simular ejecuciones de ordenes limit."""
        self.order_service = order_service

    def set_order_message_processor(self, order_message_processor):
        """Configura el order message processor para enviar mensajes O: simulados."""
        self.order_message_processor = order_message_processor

    def get_market_data(self, instrument: str) -> Optional[dict]:
        """Obtiene datos de mercado desde el cache."""
        return self.cache.get_market_data(instrument)

    def get_book_levels(self, instrument: str, num_levels: int = 3) -> Optional[dict]:
        """Obtiene niveles del libro desde el cache."""
        return self.cache.get_book_levels(instrument, num_levels)

    def get_execution_price(self, instrument: str, side: str) -> Optional[float]:
        """Obtiene precio de ejecucion desde el cache."""
        return self.cache.get_execution_price(instrument, side)

    def get_all_instruments(self) -> list[str]:
        """Retorna lista de instrumentos con datos en cache."""
        return self.cache.get_all_instruments()

    async def register_with_price_listener(self):
        """
        Registra este servicio como subscriber del price listener via REST.
        POST http://{PRICE_LISTENER_URL}/api/subscribers
        Body: {"alias": "matriz_orders", "tickers": ["AL30", ...]}
        """
        import aiohttp

        url = f"{self.price_listener_url}/api/subscribers"
        payload = {
            "alias": "matriz_orders",
            "tickers": self.tickers
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload) as resp:
                    if resp.status == 200:
                        result = await resp.json()
                        print(f"[RedisMarketDataSubscriber] Registrado en price listener: {result}")
                    else:
                        text = await resp.text()
                        print(f"[RedisMarketDataSubscriber] Error registrando en price listener ({resp.status}): {text}")
        except Exception as e:
            print(f"[RedisMarketDataSubscriber] No se pudo registrar en price listener: {e}")

    async def _connect_redis(self) -> bool:
        """Intenta conectar a Redis y suscribir a los canales. Retorna True si tuvo exito."""
        from redis_client import get_redis

        try:
            redis_client = await get_redis()
            await redis_client.ping()

            self._pubsub = redis_client.pubsub()
            for channel in self._channels:
                await self._pubsub.subscribe(channel)

            self.connected = True
            print(f"[RedisMarketDataSubscriber] Conectado y suscrito a: {self._channels}")
            return True
        except Exception as e:
            self.connected = False
            if self._pubsub:
                try:
                    await self._pubsub.aclose()
                except Exception:
                    pass
                self._pubsub = None
            print(f"[RedisMarketDataSubscriber] Redis no disponible: {e}")
            return False

    async def run(self):
        """
        Inicia la suscripcion a los canales Redis del price listener.
        Corre indefinidamente, reintentando conexion si Redis no esta disponible.
        """
        self._running = True

        # Registrar con price listener (independiente de Redis)
        await self.register_with_price_listener()

        while self._running:
            # Intentar conectar si no estamos conectados
            if not self.connected:
                if not await self._connect_redis():
                    await asyncio.sleep(REDIS_RECONNECT_INTERVAL)
                    continue

            # Leer mensajes
            try:
                message = await self._pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=1.0
                )
                if message and message['type'] == 'message':
                    raw = message['data']
                    if isinstance(raw, bytes):
                        raw = raw.decode('utf-8')
                    await self._process_message(raw)
                else:
                    await asyncio.sleep(0.01)
            except asyncio.CancelledError:
                print("[RedisMarketDataSubscriber] Cancelled")
                break
            except Exception as e:
                print(f"[RedisMarketDataSubscriber] Error leyendo Redis, reconectando: {e}")
                self.connected = False
                if self._pubsub:
                    try:
                        await self._pubsub.aclose()
                    except Exception:
                        pass
                    self._pubsub = None
                await asyncio.sleep(REDIS_RECONNECT_INTERVAL)

        await self.stop()

    async def stop(self):
        """Detiene la suscripcion."""
        self._running = False
        if self._pubsub:
            try:
                for channel in self._channels:
                    await self._pubsub.unsubscribe(channel)
                await self._pubsub.aclose()
            except Exception:
                pass
            self._pubsub = None
        self.connected = False
        print("[RedisMarketDataSubscriber] Detenido")

    async def _process_message(self, raw_message: str):
        """
        Procesa un mensaje del price listener.
        Formato JSON: {"instrument", "ticker", "term", "data": {...}, "timestamp"}
        """
        try:
            msg = json.loads(raw_message)
            instrument = msg.get('instrument')
            data = msg.get('data')

            if not instrument or not data:
                return

            # Actualizar cache y contador
            self.cache.update_from_price_listener(instrument, data)
            self.message_count += 1
            if self.on_message_count:
                await self.on_message_count(self.message_count)

            # Simular ejecuciones de ordenes limit en paper mode
            if self.order_service and self.order_message_processor:
                if self.order_service.mode == 'paper':
                    last_price = data.get('last')
                    if last_price is not None:
                        await self._simulate_limit_order_executions(
                            instrument, float(last_price), 1
                        )

        except json.JSONDecodeError:
            pass
        except Exception as e:
            print(f"[RedisMarketDataSubscriber] Error procesando mensaje: {e}")

    async def _simulate_limit_order_executions(self, instrument: str, last_price: float, last_qty: int):
        """
        Simula ejecuciones de ordenes limit en paper mode cuando el precio operado cruza el limite.
        """
        try:
            active_orders = self.order_service.repository.get_active_orders_by_instrument(
                instrument,
                statuses=['confirmed', 'partial']
            )

            for order in active_orders:
                # Solo procesar ordenes limit (no market)
                if order.order_type == "1":
                    continue

                # Verificar si el precio cruza el limite
                should_execute = False
                if order.side == "1":  # Buy
                    should_execute = last_price <= order.price
                elif order.side == "2":  # Sell
                    should_execute = last_price >= order.price

                if should_execute:
                    leaves_qty = order.quantity - order.operated_size
                    exec_qty = min(last_qty, leaves_qty)
                    is_filled = (exec_qty >= leaves_qty)

                    order_json = {
                        "id": order.broker_order_id,
                        "ordStatus": "2" if is_filled else "1",
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
                        "text": f"paper - Limit Order {'Filled' if is_filled else 'Partial'} @ {last_price}",
                        "orderId": "paper",
                        "displayMethod": "paper",
                        "execId": "paper",
                        "execType": "F" if is_filled else "1",
                        "securityExchange": "paper",
                        "contraBroker": "paper",
                        "partyOrigTrader": "paper",
                        "tradingSessionId": "paper",
                        "ordStatusReqId": "paper",
                        "massStatusReqId": "paper",
                        "frontId": order.front_id,
                        "orderQty": str(order.quantity),
                        "price": str(order.price),
                        "stopPx": "paper",
                        "avgPx": str(last_price),
                        "lastPx": str(last_price),
                        "lastQty": str(exec_qty),
                        "cumQty": str(order.operated_size + exec_qty),
                        "displayQty": "paper",
                        "leavesQty": str(leaves_qty - exec_qty),
                        "totNumReports": "paper",
                        "lastRptRequested": "paper"
                    }

                    message = f"O:{json.dumps(order_json)}"
                    await self.order_message_processor.process_direct(message)

                    print(f"[PaperLimitExec] Order {order.id} ({'FILLED' if is_filled else 'PARTIAL'}): "
                          f"{exec_qty} @ {last_price} (leaves: {leaves_qty - exec_qty})")

        except Exception as e:
            print(f"[RedisMarketDataSubscriber] Error simulando ejecuciones limit: {e}")
            import traceback
            traceback.print_exc()
