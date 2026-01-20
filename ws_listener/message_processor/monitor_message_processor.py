from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Callable
import json

from ws_listener.message_processor.base_message_processor import BaseMessageProcessor


class MonitorMessageProcessor(BaseMessageProcessor):
    """
    Message processor para precios en la app web de monitoreo.
    Guarda mensajes en archivo de log con prefijo 'price-' y notifica a la UI.
    Actualiza el MarketDataCache con datos de mercado.
    """

    def __init__(self, on_status_change: Optional[Callable] = None, on_message: Optional[Callable] = None):
        super().__init__()
        self.on_status_change = on_status_change
        self.on_message = on_message
        self.last_message_time: Optional[datetime] = None
        self.recent_messages: list[dict] = []
        self.max_recent_messages = 50
        self.market_cache = None  # Will be set by web app
        self.order_service = None  # Will be set by web app
        self.order_message_processor = None  # Will be set by web app

        # Tickers a monitorear (para filtrado si es necesario)
        self.tickers = ['AL30']

        # Logging a archivo
        self.logging_enabled = True
        self.log_dir = Path('logs/ws')
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.current_file = None
        self.current_file_start_time = datetime.now()
        self._open_new_file()

    def _generate_filename(self) -> str:
        return f"price-{self.current_file_start_time.strftime('%m-%d-%H:%M:%S')}.txt"

    def _open_new_file(self):
        if self.current_file:
            self.current_file.close()
        self.current_file_start_time = datetime.now()
        filename = self.log_dir / self._generate_filename()
        self.current_file = open(filename, 'a')
        print(f"[MonitorMessageProcessor] New log file: {filename}")

    def start_new_log_file(self):
        """Inicia un nuevo archivo de log (llamado al recargar config)"""
        self._open_new_file()

    def set_market_cache(self, market_cache):
        """Configura el market data cache para actualizar precios"""
        self.market_cache = market_cache

    def set_order_service(self, order_service):
        """Configura el order service para simular ejecuciones de órdenes limit"""
        self.order_service = order_service

    def set_order_message_processor(self, order_message_processor):
        """Configura el order message processor para enviar mensajes O: simulados"""
        self.order_message_processor = order_message_processor

    async def update_status(self, status: str):
        self.status = status
        print(f"[WS Status] {status}")
        if self.on_status_change:
            await self.on_status_change(status, self.message_count)

    async def process_message(self, message: str):
        """Procesa un mensaje: lo guarda en archivo (si habilitado) y notifica a la UI"""
        self.last_message_time = datetime.now()

        # Escribir a archivo solo si logging esta habilitado
        if self.logging_enabled and self.current_file:
            # Rotar archivo cada hora
            if datetime.now() - self.current_file_start_time >= timedelta(hours=1):
                self._open_new_file()

            relative_time = datetime.now() - self.current_file_start_time
            print(f'{relative_time} {message}', file=self.current_file)
            self.current_file.flush()

        # Guardar para UI
        message_data = {
            'timestamp': self.last_message_time.isoformat(),
            'type': message[0] if message else 'U',
            'content': message,
            'count': self.message_count
        }

        self.recent_messages.append(message_data)
        if len(self.recent_messages) > self.max_recent_messages:
            self.recent_messages.pop(0)

        # Notificar a la UI
        if self.on_message:
            await self.on_message(message_data)

        # Actualizar market cache si está configurado
        if self.market_cache and message:
            await self._update_market_cache(message)

    async def _update_market_cache(self, message: str):
        """
        Parsea mensajes M: (market data) y B: (book) y actualiza el cache.
        Si es un mensaje M: con último precio operado, simula ejecuciones de órdenes limit en paper mode.
        """
        try:
            if message.startswith('M:'):
                # Mensaje de market data: M:{json}
                json_str = message[2:]
                data = json.loads(json_str)

                # Extraer instrumento
                instrument = data.get('securityId') or data.get('SYMB')
                if instrument:
                    self.market_cache.update_from_message(instrument, data)

                    # Simular ejecuciones de órdenes limit si estamos en paper mode
                    # y hay último precio operado
                    if self.order_service and self.order_message_processor:
                        if self.order_service.mode == 'paper':
                            last_price_str = data.get('LA')  # Last price
                            last_qty_str = data.get('LQT')  # Last quantity traded

                            if last_price_str:
                                try:
                                    last_price = float(last_price_str)
                                    last_qty = int(last_qty_str) if last_qty_str else 1
                                    await self._simulate_limit_order_executions(instrument, last_price, last_qty)
                                except (ValueError, TypeError):
                                    pass

            elif message.startswith('B:'):
                # Mensaje de book: B:{json}
                json_str = message[2:]
                data = json.loads(json_str)

                # Extraer instrumento
                instrument = data.get('securityId') or data.get('SYMB')
                if instrument:
                    self.market_cache.update_from_message(instrument, data)

        except json.JSONDecodeError:
            # Ignorar mensajes que no sean JSON válido
            pass
        except Exception as e:
            print(f"[MonitorMessageProcessor] Error actualizando market cache: {e}")

    async def _simulate_limit_order_executions(self, instrument: str, last_price: float, last_qty: int):
        """
        Simula ejecuciones de órdenes limit en paper mode cuando el precio operado cruza el límite.

        Args:
            instrument: Instrumento operado (ej: "bm_MERV_AL30_CI")
            last_price: Último precio operado
            last_qty: Cantidad operada en esta transacción
        """
        try:
            # Buscar órdenes limit activas (confirmed) para este instrumento
            # Solo procesamos órdenes en status "confirmed" (ya confirmadas pero no ejecutadas)
            # o "partial" (parcialmente ejecutadas)
            active_orders = self.order_service.repository.get_active_orders_by_instrument(
                instrument,
                statuses=['confirmed', 'partial']
            )

            for order in active_orders:
                # Solo procesar órdenes limit (order_type != "1" que es market)
                if order.order_type == "1":
                    continue

                # Verificar si el precio cruza el límite de la orden
                should_execute = False

                if order.side == "1":  # Buy order
                    # Se ejecuta si el último precio operado es <= al precio límite
                    should_execute = last_price <= order.price
                elif order.side == "2":  # Sell order
                    # Se ejecuta si el último precio operado es >= al precio límite
                    should_execute = last_price >= order.price

                if should_execute:
                    # Calcular cuánto queda por ejecutar
                    leaves_qty = order.quantity - order.operated_size

                    # Determinar cantidad a ejecutar en esta transacción
                    exec_qty = min(last_qty, leaves_qty)

                    # Determinar si es ejecución parcial o completa
                    is_filled = (exec_qty >= leaves_qty)

                    # Generar mensaje O: simulado
                    order_json = {
                        "id": order.broker_order_id,
                        "ordStatus": "2" if is_filled else "1",  # 2=filled, 1=partial
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
                        "execType": "F" if is_filled else "1",  # F=final, 1=partial
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
                        "lastPx": str(last_price),  # Precio de esta ejecución
                        "lastQty": str(exec_qty),  # Cantidad ejecutada
                        "cumQty": str(order.operated_size + exec_qty),  # Total ejecutado acumulado
                        "displayQty": "paper",
                        "leavesQty": str(leaves_qty - exec_qty),  # Queda por ejecutar
                        "totNumReports": "paper",
                        "lastRptRequested": "paper"
                    }

                    message = f"O:{json.dumps(order_json)}"
                    await self.order_message_processor.process_direct(message)

                    print(f"[PaperLimitExec] Order {order.id} ({'FILLED' if is_filled else 'PARTIAL'}): "
                          f"{exec_qty} @ {last_price} (leaves: {leaves_qty - exec_qty})")

        except Exception as e:
            print(f"[MonitorMessageProcessor] Error simulando ejecuciones limit: {e}")
            import traceback
            traceback.print_exc()

    def get_state(self) -> dict:
        base_state = super().get_state()
        base_state.update({
            'last_message_time': self.last_message_time.isoformat() if self.last_message_time else None,
            'recent_messages': self.recent_messages[-10:],
            'current_log_file': str(self.log_dir / self._generate_filename()),
            'logging_enabled': self.logging_enabled
        })
        return base_state

    def __del__(self):
        if self.current_file:
            self.current_file.close()
