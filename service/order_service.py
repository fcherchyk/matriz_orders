import os
import time
from datetime import datetime
from typing import List, Optional, Callable

from dotenv import load_dotenv

from model.order_model import Order, ClosingOrder
from repository.order_repository import OrderRepository
from repository.closing_order_repository import ClosingOrderRepository

load_dotenv()


class OrderService:

    def __init__(self, session, on_paper_order: Optional[Callable] = None):
        self.requester = session.requester
        self.session = session
        self.repository = OrderRepository()
        self.closing_repository = ClosingOrderRepository()
        # Modo de operacion: "prod" o "paper" (desde config o env)
        self.mode = os.getenv('ORDER_MODE', 'paper')
        # Callback para notificar ordenes paper (para simulacion en modos no-prod)
        self.on_paper_order = on_paper_order

    def set_mode(self, mode: str):
        """Cambia el modo de operacion"""
        if mode not in ['prod', 'paper']:
            raise ValueError(f"Modo invalido: {mode}. Use 'prod' o 'paper'")
        self.mode = mode

    def set_on_paper_order(self, callback: Optional[Callable]):
        """Configura el callback para ordenes paper"""
        self.on_paper_order = callback

    def buy_market(self, size, instrument):
        return self.place_order("1", size, instrument, "1")

    def sell_market(self, size, instrument):
        return self.place_order("2", size, instrument, "1")

    def buy_limit(self, size, instrument, price):
        return self.place_order("1", size, instrument, "2", price)

    def sell_limit(self, size, instrument, price):
        return self.place_order("2", size, instrument, "2", price)

    def caution_market(self, size, instrument):
        return self.place_order("2", size, instrument, "K")

    def caution_limit(self, size, instrument, price):
        return self.place_order("2", size, instrument, "K", price)

    def place_order(self, n_side, size, instrument, order_type, limit_price=None) -> Order:
        """
        Coloca una orden en el broker (si modo prod) y la guarda en la base de datos.
        Si modo es paper, solo crea y persiste el objeto Order sin enviar al broker.
        Para ordenes market en paper mode, el precio se setea en 0.
        """
        front_id = str(int(time.time() * 1000))

        # Para paper mode en ordenes market, precio es 0
        if self.mode == 'paper' and order_type == '1':
            limit_price = 0

        # Calcular total_amount
        if order_type != '1' and limit_price:
            total_amount = (int(size) * limit_price / 100)
        else:
            total_amount = 0

        # Crear objeto Order
        order = Order(
            front_id=front_id,
            account=self.requester.ACCOUNT if self.mode == 'prod' else 'PAPER',
            side=n_side,
            quantity=int(size),
            instrument=f"bm_MERV_{instrument}",
            order_type=order_type,
            price=limit_price,
            total_amount=total_amount,
            status="placed",
            mode=self.mode,
            placed=datetime.now()
        )

        # Guardar en base de datos primero para obtener el ID
        order = self.repository.save(order)

        # En paper mode, generar broker_order_id simulado
        if self.mode == 'paper':
            order.broker_order_id = f"paper_id_{order.id}"
            self.repository.update(order)

        # Solo enviar al broker si estamos en modo prod
        if self.mode == 'prod':
            # Construir payload para el broker
            payload = f'"price":{int(limit_price)},' if order_type != '1' and limit_price else ''
            payload += f'"totalAmount":{total_amount:.0f},' if order_type != '1' and limit_price else '"totalAmount":0,'
            payload = (
                f'{{"account":"{self.requester.ACCOUNT}",'
                f'{payload}'
                f'"quantity":{int(size)},"securityId":"bm_MERV_{instrument}",'
                f'"side":"{n_side}","ordType":"{order_type}","execInst":[],'
                f'"canReplacePreviousOrders":true,"timeInForce":"0","isQuantity":true,'
                f'"sent":true,"ignoredAlertPrice":false,"visibleAlertPrice":false,'
                f'"frontId":"{front_id}"}}'
            )

            # Enviar al broker
            print(f"[OrderService.place_order] Enviando orden al broker...")
            print(f"[OrderService.place_order] Payload: {payload[:200]}...")
            try:
                response = self.requester.http_post('orders/single', payload)
                print(f"[OrderService.place_order] Respuesta del broker: {response}")

                # Guardar ID del broker si viene en la respuesta
                if response and 'id' in response:
                    order.broker_order_id = response['id']
                    print(f"[OrderService.place_order] broker_order_id: {order.broker_order_id}")
                    # Actualizar en la base de datos
                    self.repository.update(order)
                else:
                    print(f"[OrderService.place_order] ERROR: No se recibió broker_order_id - la orden no llegó al broker")
                    # No devolver la orden si el broker rechazó la solicitud
                    return None
            except Exception as e:
                print(f"[OrderService.place_order] ERROR enviando al broker: {e}")
                import traceback
                traceback.print_exc()
                raise

        # Notificar callback si estamos en modo paper
        if self.mode == 'paper' and self.on_paper_order:
            self.on_paper_order('placed', order)

        return order

    def cancel_order(self, order_id: int) -> Order:
        """
        Cancela una orden existente.
        Recupera la orden por ID, actualiza su estado a 'canceled' y la hora de cancelacion.
        Solo envia al broker si la orden fue creada en modo prod.
        """
        print(f"[OrderService.cancel_order] Cancelando orden id={order_id}...")

        # Recuperar orden de la base
        order = self.repository.get_by_id(order_id)
        if not order:
            raise ValueError(f"Orden con id {order_id} no encontrada")

        print(f"[OrderService.cancel_order] Orden encontrada: mode={order.mode}, broker_order_id={order.broker_order_id}")

        # Enviar cancelacion al broker solo si es modo prod y tiene broker_order_id
        if order.mode == 'prod' and order.broker_order_id:
            resource = f"orders/cancel/{order.broker_order_id}"
            print(f"[OrderService.cancel_order] Enviando cancelación al broker: {resource}")
            try:
                response = self.requester.http_post(resource, '')
                print(f"[OrderService.cancel_order] Respuesta del broker: {response}")
            except Exception as e:
                print(f"[OrderService.cancel_order] ERROR enviando cancelación: {e}")
                import traceback
                traceback.print_exc()
                raise
        else:
            print(f"[OrderService.cancel_order] No se envía al broker (mode={order.mode}, broker_order_id={order.broker_order_id})")

        # Actualizar estado y timestamp
        order.status = "canceled"
        order.canceled = datetime.now()

        # Guardar cambios
        self.repository.update(order)

        # Notificar callback si la orden era paper
        if order.mode == 'paper' and self.on_paper_order:
            self.on_paper_order('canceled', order)

        return order

    def update_order_status(self, order_id: int, new_status: str) -> Order:
        """
        Actualiza el estado de una orden y su timestamp correspondiente.
        Estados validos: placed, confirmed, partial, filled, canceled, confirmed_canceled
        """
        valid_statuses = ['placed', 'confirmed', 'partial', 'filled', 'canceled', 'confirmed_canceled']
        if new_status not in valid_statuses:
            raise ValueError(f"Estado invalido: {new_status}. Estados validos: {valid_statuses}")

        # Recuperar orden
        order = self.repository.get_by_id(order_id)
        if not order:
            raise ValueError(f"Orden con id {order_id} no encontrada")

        # Actualizar estado y timestamp correspondiente
        order.status = new_status
        now = datetime.now()

        if new_status == 'placed':
            order.placed = now
        elif new_status == 'confirmed':
            order.confirmed = now
        elif new_status == 'partial':
            order.partial = now
        elif new_status == 'filled':
            order.filled = now
        elif new_status == 'canceled':
            order.canceled = now
        elif new_status == 'confirmed_canceled':
            order.confirmed_canceled = now

        # Guardar cambios
        self.repository.update(order)

        return order

    def get_today_orders(self) -> List[Order]:
        """Retorna todas las ordenes del dia actual filtradas por el modo actual"""
        return self.repository.get_today_orders(mode=self.mode)

    def get_active_orders(self) -> List[Order]:
        """Retorna ordenes del dia excluyendo las confirmed_canceled, filtradas por el modo actual"""
        return self.repository.get_active_orders(mode=self.mode)

    def get_pending_orders(self) -> List[Order]:
        """Retorna ordenes del dia excluyendo confirmed_canceled y filled, filtradas por el modo actual"""
        return self.repository.get_pending_orders(mode=self.mode)

    def get_order(self, order_id: int) -> Optional[Order]:
        """Recupera una orden por su ID"""
        return self.repository.get_by_id(order_id)

    def cancel_all_pending(self) -> List[Order]:
        """
        Cancela todas las ordenes pendientes del modo actual.
        Retorna la lista de ordenes canceladas.
        """
        pending_orders = self.get_pending_orders()
        canceled_orders = []

        for order in pending_orders:
            try:
                canceled_order = self.cancel_order(order.id)
                canceled_orders.append(canceled_order)
            except Exception as e:
                print(f"Error cancelando orden {order.id}: {e}")

        return canceled_orders

    def test_order(self):
        """
        Prueba de colocar y cancelar una orden.
        Esta orden sirve para activar el order listener y verificar que está recibiendo eventos.
        IMPORTANTE: Debe ejecutarse en modo prod para que el broker procese la orden
        y envíe los eventos al websocket.
        """
        print(f"[OrderService.test_order] ========================================", flush=True)
        print(f"[OrderService.test_order] Iniciando test_order", flush=True)
        print(f"[OrderService.test_order] mode: {self.mode}", flush=True)
        print(f"[OrderService.test_order] requester.csrf_token: {self.requester.csrf_token[:20] if self.requester.csrf_token else 'None'}...", flush=True)
        print(f"[OrderService.test_order] requester.cookies: {list(self.requester.cookies.keys())}", flush=True)
        print(f"[OrderService.test_order] ========================================", flush=True)

        if self.mode != 'prod':
            print(f"[OrderService.test_order] ERROR: modo={self.mode}, no se puede ejecutar test_order en modo no-prod", flush=True)
            return None

        try:
            print(f"[OrderService.test_order] Colocando orden de prueba (buy_limit 1 AL30_CI @ $1)...", flush=True)
            order = self.buy_limit(1, 'AL30_CI', 1)

            if not order:
                print(f"[OrderService.test_order] ERROR: buy_limit retornó None - la orden no llegó al broker", flush=True)
                return None

            print(f"[OrderService.test_order] Orden creada: id={order.id}, front_id={order.front_id}, broker_order_id={order.broker_order_id}", flush=True)

            if not order.broker_order_id:
                print(f"[OrderService.test_order] ERROR: No se recibió broker_order_id - la orden no llegó al broker", flush=True)
                return None

            # Esperar 1 segundo para que el broker confirme la orden antes de cancelar
            print(f"[OrderService.test_order] Esperando 1s para confirmación del broker...", flush=True)
            time.sleep(1)

            print(f"[OrderService.test_order] Cancelando orden de prueba...", flush=True)
            canceled_order = self.cancel_order(order.id)
            print(f"[OrderService.test_order] Orden cancelada: id={canceled_order.id}, status={canceled_order.status}", flush=True)

            return canceled_order
        except Exception as e:
            print(f"[OrderService.test_order] ERROR: {e}", flush=True)
            import traceback
            traceback.print_exc()
            raise

    # ============ Closing Orders ============

    def close_market(self, order_id: int, ticker: str, term: str) -> ClosingOrder:
        """
        Crea una orden de cierre de tipo market para una orden existente.

        Args:
            order_id: ID de la orden original que se quiere cerrar
            ticker: Ticker para compensar (puede diferir del original para compensacion por monto)
            term: Plazo (CI, 24hs, etc.)

        Returns:
            ClosingOrder: El objeto de cierre creado y persistido

        La logica de compensacion (generar ordenes inversas cuando la original
        se opere parcial o totalmente) se implementara posteriormente.
        - Si ticker es igual al original: se compensara por cantidad operada
        - Si ticker es diferente: se compensara por monto aproximado
        """
        # Verificar que la orden original existe
        original_order = self.repository.get_by_id(order_id)
        if not original_order:
            raise ValueError(f"Orden con id {order_id} no encontrada")

        # Verificar que no exista ya un closing order para esta orden
        existing = self.closing_repository.get_by_original_order_id(order_id)
        if existing:
            raise ValueError(f"Ya existe un closing order para la orden {order_id}")

        # Crear el closing order
        closing_order = ClosingOrder(
            original_order_id=order_id,
            ticker=ticker,
            term=term,
            closing_type="market",
            status="pending",
            created_at=datetime.now()
        )

        # Persistir
        closing_order = self.closing_repository.save(closing_order)

        return closing_order

    def get_closing_order(self, closing_order_id: int) -> Optional[ClosingOrder]:
        """Recupera un closing order por su ID"""
        return self.closing_repository.get_by_id(closing_order_id)

    def get_closing_order_for_order(self, order_id: int) -> Optional[ClosingOrder]:
        """Recupera el closing order asociado a una orden original"""
        return self.closing_repository.get_by_original_order_id(order_id)

    def get_pending_closing_orders(self) -> List[ClosingOrder]:
        """Retorna todos los closing orders pendientes de compensar"""
        return self.closing_repository.get_pending()

    def get_today_closing_orders(self) -> List[ClosingOrder]:
        """Retorna todos los closing orders del dia"""
        return self.closing_repository.get_today_closing_orders()

    # ============ Procesamiento de Ejecuciones ============

    def process_execution(self, order_id: int, last_qty: int, last_px: float) -> Order:
        """
        Procesa una ejecucion (parcial o total) de una orden.

        Args:
            order_id: ID de la orden ejecutada
            last_qty: Cantidad ejecutada en esta ejecucion
            last_px: Precio de ejecucion

        Returns:
            Order: La orden actualizada

        Esta funcion:
        1. Actualiza operated_size y operated_volume de la orden
        2. Actualiza el status a 'partial' o 'filled' segun corresponda
        3. Si existe un ClosingOrder tipo market para esta orden, genera la orden compensatoria
        """
        order = self.repository.get_by_id(order_id)
        if not order:
            raise ValueError(f"Orden con id {order_id} no encontrada")

        # Actualizar campos de ejecucion
        order.operated_size += last_qty
        order.operated_volume += last_qty * last_px

        # Determinar nuevo estado
        now = datetime.now()
        if order.operated_size >= order.quantity:
            order.status = 'filled'
            order.filled = now
        else:
            order.status = 'partial'
            order.partial = now

        # Guardar cambios
        self.repository.update(order)

        # Verificar si existe un ClosingOrder tipo market y generar orden compensatoria
        closing_order = self.closing_repository.get_by_original_order_id(order_id)
        if closing_order and closing_order.closing_type == 'market' and closing_order.status in ['pending', 'partial']:
            self._place_compensatory_order(order, closing_order, last_qty, last_px)

        return order

    def process_execution_by_broker_id(self, broker_order_id: str, last_qty: int, last_px: float) -> Optional[Order]:
        """
        Procesa una ejecucion buscando la orden por broker_order_id.

        Args:
            broker_order_id: ID de la orden en el broker
            last_qty: Cantidad ejecutada en esta ejecucion
            last_px: Precio de ejecucion

        Returns:
            Order: La orden actualizada, o None si no se encuentra
        """
        order = self.repository.get_by_broker_order_id(broker_order_id)
        if not order:
            return None
        return self.process_execution(order.id, last_qty, last_px)

    def _place_compensatory_order(self, original_order: Order, closing_order: ClosingOrder, exec_qty: int, exec_px: float):
        """
        Genera una orden compensatoria para un ClosingOrder de tipo market.

        La orden compensatoria tiene el lado inverso a la orden original.
        """
        # Determinar el lado inverso
        compensatory_side = "2" if original_order.side == "1" else "1"

        # Construir el instrumento con el ticker/term del closing order
        instrument = f"{closing_order.ticker}_{closing_order.term}"

        # Crear la orden compensatoria como market
        compensatory_order = Order(
            front_id=str(int(time.time() * 1000)),
            account=self.requester.ACCOUNT if self.mode == 'prod' else 'PAPER',
            side=compensatory_side,
            quantity=exec_qty,
            instrument=f"bm_MERV_{instrument}",
            order_type="1",  # Market
            price=None,
            total_amount=0,
            status="placed",
            mode=self.mode,
            original_order_id=original_order.id,  # Referencia a la orden original
            placed=datetime.now()
        )

        # Enviar al broker si estamos en modo prod
        if self.mode == 'prod':
            payload = (
                f'{{"account":"{self.requester.ACCOUNT}",'
                f'"totalAmount":0,'
                f'"quantity":{exec_qty},"securityId":"bm_MERV_{instrument}",'
                f'"side":"{compensatory_side}","ordType":"1","execInst":[],'
                f'"canReplacePreviousOrders":true,"timeInForce":"0","isQuantity":true,'
                f'"sent":true,"ignoredAlertPrice":false,"visibleAlertPrice":false,'
                f'"frontId":"{compensatory_order.front_id}"}}'
            )
            response = self.requester.http_post('orders/single', payload)
            if response and 'id' in response:
                compensatory_order.broker_order_id = response['id']

        # Guardar la orden compensatoria
        self.repository.save(compensatory_order)

        # Actualizar estado del closing order
        if closing_order.status == 'pending':
            closing_order.status = 'partial'
        self.closing_repository.update(closing_order)

        # Notificar callback si estamos en modo paper
        if self.mode == 'paper' and self.on_paper_order:
            self.on_paper_order('placed', compensatory_order)

    def process_compensatory_execution(self, order_id: int, last_qty: int, last_px: float) -> Optional[ClosingOrder]:
        """
        Procesa la ejecucion de una orden compensatoria y actualiza el ClosingOrder correspondiente.

        Args:
            order_id: ID de la orden compensatoria ejecutada
            last_qty: Cantidad ejecutada en esta ejecucion
            last_px: Precio de ejecucion

        Returns:
            ClosingOrder: El closing order actualizado, o None si la orden no es compensatoria
        """
        order = self.repository.get_by_id(order_id)
        if not order or not order.original_order_id:
            return None

        # Actualizar la orden compensatoria
        order.operated_size += last_qty
        order.operated_volume += last_qty * last_px

        now = datetime.now()
        if order.operated_size >= order.quantity:
            order.status = 'filled'
            order.filled = now
        else:
            order.status = 'partial'
            order.partial = now

        self.repository.update(order)

        # Buscar y actualizar el closing order
        closing_order = self.closing_repository.get_by_original_order_id(order.original_order_id)
        if closing_order:
            closing_order.operated_size += last_qty
            closing_order.operated_volume += last_qty * last_px

            # Verificar si el closing order esta completo
            original_order = self.repository.get_by_id(order.original_order_id)
            if original_order and closing_order.operated_size >= original_order.operated_size:
                closing_order.status = 'completed'
            else:
                closing_order.status = 'partial'

            self.closing_repository.update(closing_order)
            return closing_order

        return None

    def process_compensatory_execution_by_broker_id(self, broker_order_id: str, last_qty: int, last_px: float) -> Optional[ClosingOrder]:
        """
        Procesa la ejecucion de una orden compensatoria buscando por broker_order_id.

        Args:
            broker_order_id: ID de la orden en el broker
            last_qty: Cantidad ejecutada en esta ejecucion
            last_px: Precio de ejecucion

        Returns:
            ClosingOrder: El closing order actualizado, o None si no se encuentra
        """
        order = self.repository.get_by_broker_order_id(broker_order_id)
        if not order:
            return None
        return self.process_compensatory_execution(order.id, last_qty, last_px)
