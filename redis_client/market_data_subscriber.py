import asyncio
import json
import os
from typing import Optional

import redis.asyncio as redis
from dotenv import load_dotenv

from redis_client.market_data_cache import MarketDataCache

load_dotenv()


class RedisMarketDataSubscriber:
    """
    Suscribe al canal Redis de market data y actualiza el cache local.
    Tambien simula ejecuciones de ordenes limit en paper mode.
    """

    def __init__(self):
        self.channel = os.getenv('REDIS_MARKET_DATA_CHANNEL', 'market_data')
        self.cache = MarketDataCache()
        self.order_service = None
        self.order_message_processor = None
        self._running = False
        self._pubsub: Optional[redis.client.PubSub] = None
        self._redis: Optional[redis.Redis] = None

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

    async def run(self, redis_client: redis.Redis):
        """
        Inicia la suscripcion al canal Redis de market data.
        Corre indefinidamente procesando mensajes.
        """
        self._redis = redis_client
        self._running = True
        self._pubsub = redis_client.pubsub()

        await self._pubsub.subscribe(self.channel)
        print(f"[RedisMarketDataSubscriber] Suscrito al canal: {self.channel}")

        try:
            while self._running:
                message = await self._pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=1.0
                )
                if message and message['type'] == 'message':
                    await self._process_message(message['data'])
                else:
                    await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            print("[RedisMarketDataSubscriber] Cancelled")
        except Exception as e:
            print(f"[RedisMarketDataSubscriber] Error: {e}")
        finally:
            await self.stop()

    async def stop(self):
        """Detiene la suscripcion."""
        self._running = False
        if self._pubsub:
            await self._pubsub.unsubscribe(self.channel)
            await self._pubsub.aclose()
            self._pubsub = None
        print("[RedisMarketDataSubscriber] Detenido")

    async def _process_message(self, raw_message: str):
        """Procesa un mensaje del canal Redis (formato M:{json} o B:{json})."""
        try:
            if raw_message.startswith('M:'):
                json_str = raw_message[2:]
                data = json.loads(json_str)
                instrument = data.get('securityId') or data.get('SYMB')
                if instrument:
                    self.cache.update_from_message(instrument, data)

                    # Simular ejecuciones de ordenes limit en paper mode
                    if self.order_service and self.order_message_processor:
                        if self.order_service.mode == 'paper':
                            last_price_str = data.get('LA')
                            last_qty_str = data.get('LQT')
                            if last_price_str:
                                try:
                                    last_price = float(last_price_str)
                                    last_qty = int(last_qty_str) if last_qty_str else 1
                                    await self._simulate_limit_order_executions(
                                        instrument, last_price, last_qty
                                    )
                                except (ValueError, TypeError):
                                    pass

            elif raw_message.startswith('B:'):
                json_str = raw_message[2:]
                data = json.loads(json_str)
                instrument = data.get('securityId') or data.get('SYMB')
                if instrument:
                    self.cache.update_from_message(instrument, data)

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
