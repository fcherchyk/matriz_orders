from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Callable
import json

from ws_listener.message_processor.base_message_processor import BaseMessageProcessor


class OrderMessageProcessor(BaseMessageProcessor):
    """
    Message processor para ordenes en la app web de monitoreo.
    Guarda mensajes en archivo de log con prefijo 'order-' y notifica a la UI.
    Maneja mensajes O: para actualizar estados de ordenes en la base de datos.
    """

    def __init__(self, on_status_change: Optional[Callable] = None, on_message: Optional[Callable] = None):
        super().__init__()
        self.on_status_change = on_status_change
        self.on_message = on_message
        self.last_message_time: Optional[datetime] = None
        self.recent_messages: list[dict] = []
        self.max_recent_messages = 50
        self.order_service = None  # Will be set by web app after creation
        self.redis_publisher = None  # Will be set by web app after creation

        # Logging a archivo
        self.logging_enabled = True
        self.log_dir = Path('logs/ws')
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.current_file = None
        self.current_file_start_time = datetime.now()
        self._open_new_file()

    def _generate_filename(self) -> str:
        return f"order-{self.current_file_start_time.strftime('%m-%d-%H:%M:%S')}.txt"

    def _open_new_file(self):
        if self.current_file:
            self.current_file.close()
        self.current_file_start_time = datetime.now()
        filename = self.log_dir / self._generate_filename()
        self.current_file = open(filename, 'a')
        print(f"[OrderMessageProcessor] New log file: {filename}")

    def start_new_log_file(self):
        """Inicia un nuevo archivo de log"""
        self._open_new_file()

    def set_order_service(self, order_service):
        """Configura el order service para actualizar ordenes en la base de datos"""
        self.order_service = order_service

    def set_redis_publisher(self, publisher):
        """Configura el publisher Redis para publicar eventos de ordenes"""
        self.redis_publisher = publisher

    async def update_status(self, status: str):
        self.status = status
        print(f"[Order WS Status] {status}")
        if self.on_status_change:
            await self.on_status_change(status, self.message_count)

    async def process_message(self, message: str):
        """Procesa un mensaje: lo guarda en archivo y notifica a la UI"""
        self.last_message_time = datetime.now()

        # Escribir a archivo
        if self.logging_enabled and self.current_file:
            # Rotar archivo cada hora
            if datetime.now() - self.current_file_start_time >= timedelta(hours=1):
                self._open_new_file()

            relative_time = datetime.now() - self.current_file_start_time
            # Reemplazar saltos de línea para que el JSON quede en una sola línea
            message_single_line = message.replace('\n', ' ').replace('\r', '')
            print(f'{relative_time} {message_single_line}', file=self.current_file)
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

    async def process_direct(self, message: str):
        """
        Procesa un mensaje directamente sin pasar por la cola.
        Util para mensajes de orden que no necesitan el sistema de buffering.
        Maneja mensajes O: para actualizar estados de ordenes en la base de datos.
        """
        self.message_count += 1
        await self.process_message(message)

        # Publicar a Redis si hay publisher configurado
        if self.redis_publisher:
            await self.redis_publisher.publish(message)

        # Si es un mensaje O:, actualizar el estado de la orden en la base de datos
        if message.startswith('O:') and self.order_service:
            try:
                # Extraer el JSON del mensaje O:
                json_str = message[2:]  # Remover "O:" del inicio
                order_data = json.loads(json_str)

                broker_order_id = order_data.get('id')
                ord_status = order_data.get('ordStatus')

                if not broker_order_id or ord_status is None:
                    print(f"[OrderMessageProcessor] O: mensaje sin id u ordStatus: {message[:100]}")
                    return

                # Buscar la orden en la base de datos por broker_order_id
                # En paper mode: broker_order_id es "paper_id_123"
                # En prod mode: broker_order_id es el ID del broker real
                order = self.order_service.repository.get_by_broker_order_id(broker_order_id)

                if not order:
                    print(f"[OrderMessageProcessor] Orden no encontrada: broker_order_id={broker_order_id}")
                    return

                # Actualizar estado segun ordStatus
                if ord_status == "0":
                    # ordStatus 0 = orden confirmada (placed -> confirmed)
                    print(f"[OrderMessageProcessor] Orden confirmada: id={order.id}, broker_order_id={broker_order_id}")
                    self.order_service.update_order_status(order.id, 'confirmed')

                elif ord_status == "1":
                    # ordStatus 1 = ejecucion parcial
                    self._handle_execution(order, order_data, is_partial=True)

                elif ord_status == "2":
                    # ordStatus 2 = puede ser ejecucion completa o parcial
                    exec_type = order_data.get('execType')
                    if exec_type == "F":
                        # execType F = ejecucion final (filled)
                        self._handle_execution(order, order_data, is_partial=False)
                    else:
                        print(f"[OrderMessageProcessor] ordStatus=2 con execType={exec_type} (esperaba F)")

                elif ord_status == "4":
                    # ordStatus 4 = cancelacion confirmada (canceled -> confirmed_canceled)
                    print(f"[OrderMessageProcessor] Cancelacion confirmada: id={order.id}, broker_order_id={broker_order_id}")
                    self.order_service.update_order_status(order.id, 'confirmed_canceled')

                else:
                    print(f"[OrderMessageProcessor] ordStatus no manejado: {ord_status} para orden id={order.id}")

            except json.JSONDecodeError as e:
                print(f"[OrderMessageProcessor] Error parseando O: mensaje: {e}")
            except Exception as e:
                print(f"[OrderMessageProcessor] Error procesando O: mensaje: {e}")
                import traceback
                traceback.print_exc()

    def _handle_execution(self, order, order_data: dict, is_partial: bool):
        """
        Maneja la ejecución (parcial o total) de una orden.

        Args:
            order: Orden de la base de datos
            order_data: JSON del mensaje O:
            is_partial: True si es ejecución parcial (ordStatus=1), False si es filled (ordStatus=2, execType=F)
        """
        try:
            # Extraer datos de la ejecución
            last_qty_str = order_data.get('lastQty')
            last_px_str = order_data.get('lastPx')
            leaves_qty_str = order_data.get('leavesQty')

            # Validar que tengamos los datos necesarios
            if not last_qty_str or not last_px_str:
                print(f"[OrderMessageProcessor] Ejecución sin lastQty o lastPx: order_id={order.id}")
                return

            # Convertir a números
            try:
                last_qty = int(last_qty_str)
                last_px = float(last_px_str)
                leaves_qty = int(leaves_qty_str) if leaves_qty_str else 0
            except (ValueError, TypeError) as e:
                print(f"[OrderMessageProcessor] Error convirtiendo valores de ejecución: {e}")
                return

            # Actualizar campos de ejecución acumulada
            order.operated_size += last_qty
            order.operated_volume += (last_qty * last_px)

            # Calcular precio promedio ponderado
            if order.operated_size > 0:
                weighted_avg_price = order.operated_volume / order.operated_size
                order.price = weighted_avg_price

            # Actualizar estado y timestamp
            now = datetime.now()
            if is_partial:
                order.status = 'partial'
                order.partial = now
                print(f"[OrderMessageProcessor] Ejecución parcial: order_id={order.id}, "
                      f"lastQty={last_qty}, lastPx={last_px}, "
                      f"operated_size={order.operated_size}/{order.quantity}, "
                      f"leavesQty={leaves_qty}, "
                      f"avg_price={order.price:.2f}")
            else:
                order.status = 'filled'
                order.filled = now
                print(f"[OrderMessageProcessor] Ejecución completa (filled): order_id={order.id}, "
                      f"lastQty={last_qty}, lastPx={last_px}, "
                      f"total_operated={order.operated_size}/{order.quantity}, "
                      f"avg_price={order.price:.2f}")

            # Guardar cambios en la base de datos
            self.order_service.repository.update(order)

        except Exception as e:
            print(f"[OrderMessageProcessor] Error procesando ejecución: {e}")
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
