import sqlite3
from datetime import datetime, date
from pathlib import Path
from typing import Optional, List

from model.order_model import Order


class OrderRepository:
    """Repositorio para persistir ordenes en SQLite"""

    def __init__(self, db_path: str = "data/orders.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        """Crea la tabla de ordenes si no existe"""
        with self._get_connection() as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    broker_order_id TEXT,
                    front_id TEXT NOT NULL,
                    account TEXT NOT NULL,
                    side TEXT NOT NULL,
                    quantity INTEGER NOT NULL,
                    instrument TEXT NOT NULL,
                    order_type TEXT NOT NULL,
                    price REAL,
                    total_amount REAL NOT NULL,
                    status TEXT NOT NULL DEFAULT 'placed',
                    mode TEXT NOT NULL DEFAULT 'prod',
                    original_order_id INTEGER,
                    placed TIMESTAMP,
                    confirmed TIMESTAMP,
                    partial TIMESTAMP,
                    filled TIMESTAMP,
                    canceled TIMESTAMP,
                    confirmed_canceled TIMESTAMP
                )
            ''')
            # Agregar columnas si no existen (para migracion de DBs existentes)
            try:
                conn.execute('ALTER TABLE orders ADD COLUMN original_order_id INTEGER')
            except sqlite3.OperationalError:
                pass  # Columna ya existe
            try:
                conn.execute('ALTER TABLE orders ADD COLUMN operated_size INTEGER DEFAULT 0')
            except sqlite3.OperationalError:
                pass  # Columna ya existe
            try:
                conn.execute('ALTER TABLE orders ADD COLUMN operated_volume REAL DEFAULT 0')
            except sqlite3.OperationalError:
                pass  # Columna ya existe
            conn.commit()

    def _row_to_order(self, row: sqlite3.Row) -> Order:
        """Convierte una fila de la BD a un objeto Order"""
        return Order(
            id=row['id'],
            broker_order_id=row['broker_order_id'],
            front_id=row['front_id'],
            account=row['account'],
            side=row['side'],
            quantity=row['quantity'],
            instrument=row['instrument'],
            order_type=row['order_type'],
            price=row['price'],
            total_amount=row['total_amount'],
            status=row['status'],
            mode=row['mode'] if 'mode' in row.keys() else 'prod',
            original_order_id=row['original_order_id'] if 'original_order_id' in row.keys() else None,
            operated_size=row['operated_size'] if 'operated_size' in row.keys() and row['operated_size'] else 0,
            operated_volume=row['operated_volume'] if 'operated_volume' in row.keys() and row['operated_volume'] else 0.0,
            placed=datetime.fromisoformat(row['placed']) if row['placed'] else None,
            confirmed=datetime.fromisoformat(row['confirmed']) if row['confirmed'] else None,
            partial=datetime.fromisoformat(row['partial']) if row['partial'] else None,
            filled=datetime.fromisoformat(row['filled']) if row['filled'] else None,
            canceled=datetime.fromisoformat(row['canceled']) if row['canceled'] else None,
            confirmed_canceled=datetime.fromisoformat(row['confirmed_canceled']) if row['confirmed_canceled'] else None,
        )

    def save(self, order: Order) -> Order:
        """Guarda una orden nueva y retorna con su ID asignado"""
        with self._get_connection() as conn:
            cursor = conn.execute('''
                INSERT INTO orders (
                    broker_order_id, front_id, account, side, quantity,
                    instrument, order_type, price, total_amount, status, mode,
                    original_order_id, operated_size, operated_volume,
                    placed, confirmed, partial, filled, canceled, confirmed_canceled
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                order.broker_order_id,
                order.front_id,
                order.account,
                order.side,
                order.quantity,
                order.instrument,
                order.order_type,
                order.price,
                order.total_amount,
                order.status,
                order.mode,
                order.original_order_id,
                order.operated_size,
                order.operated_volume,
                order.placed.isoformat() if order.placed else None,
                order.confirmed.isoformat() if order.confirmed else None,
                order.partial.isoformat() if order.partial else None,
                order.filled.isoformat() if order.filled else None,
                order.canceled.isoformat() if order.canceled else None,
                order.confirmed_canceled.isoformat() if order.confirmed_canceled else None,
            ))
            conn.commit()
            order.id = cursor.lastrowid
            return order

    def update(self, order: Order) -> Order:
        """Actualiza una orden existente"""
        with self._get_connection() as conn:
            conn.execute('''
                UPDATE orders SET
                    broker_order_id = ?,
                    status = ?,
                    operated_size = ?,
                    operated_volume = ?,
                    placed = ?,
                    confirmed = ?,
                    partial = ?,
                    filled = ?,
                    canceled = ?,
                    confirmed_canceled = ?
                WHERE id = ?
            ''', (
                order.broker_order_id,
                order.status,
                order.operated_size,
                order.operated_volume,
                order.placed.isoformat() if order.placed else None,
                order.confirmed.isoformat() if order.confirmed else None,
                order.partial.isoformat() if order.partial else None,
                order.filled.isoformat() if order.filled else None,
                order.canceled.isoformat() if order.canceled else None,
                order.confirmed_canceled.isoformat() if order.confirmed_canceled else None,
                order.id,
            ))
            conn.commit()
            return order

    def get_by_id(self, order_id: int) -> Optional[Order]:
        """Recupera una orden por su ID"""
        with self._get_connection() as conn:
            cursor = conn.execute('SELECT * FROM orders WHERE id = ?', (order_id,))
            row = cursor.fetchone()
            return self._row_to_order(row) if row else None

    def get_by_broker_order_id(self, broker_order_id: str) -> Optional[Order]:
        """Recupera una orden por su ID del broker"""
        with self._get_connection() as conn:
            cursor = conn.execute('SELECT * FROM orders WHERE broker_order_id = ?', (broker_order_id,))
            row = cursor.fetchone()
            return self._row_to_order(row) if row else None

    def get_today_orders(self, mode: Optional[str] = None) -> List[Order]:
        """Retorna todas las ordenes del dia actual, opcionalmente filtradas por modo"""
        today = date.today()
        today_start = datetime.combine(today, datetime.min.time())
        with self._get_connection() as conn:
            if mode:
                cursor = conn.execute(
                    'SELECT * FROM orders WHERE placed >= ? AND mode = ? ORDER BY placed DESC',
                    (today_start.isoformat(), mode)
                )
            else:
                cursor = conn.execute(
                    'SELECT * FROM orders WHERE placed >= ? ORDER BY placed DESC',
                    (today_start.isoformat(),)
                )
            return [self._row_to_order(row) for row in cursor.fetchall()]

    def get_active_orders(self, mode: Optional[str] = None) -> List[Order]:
        """Retorna ordenes del dia excluyendo confirmed_canceled, opcionalmente filtradas por modo"""
        today = date.today()
        today_start = datetime.combine(today, datetime.min.time())
        with self._get_connection() as conn:
            if mode:
                cursor = conn.execute(
                    '''SELECT * FROM orders
                       WHERE placed >= ? AND status != 'confirmed_canceled' AND mode = ?
                       ORDER BY placed DESC''',
                    (today_start.isoformat(), mode)
                )
            else:
                cursor = conn.execute(
                    '''SELECT * FROM orders
                       WHERE placed >= ? AND status != 'confirmed_canceled'
                       ORDER BY placed DESC''',
                    (today_start.isoformat(),)
                )
            return [self._row_to_order(row) for row in cursor.fetchall()]

    def get_pending_orders(self, mode: Optional[str] = None) -> List[Order]:
        """Retorna ordenes del dia excluyendo confirmed_canceled y filled, opcionalmente filtradas por modo"""
        today = date.today()
        today_start = datetime.combine(today, datetime.min.time())
        with self._get_connection() as conn:
            if mode:
                cursor = conn.execute(
                    '''SELECT * FROM orders
                       WHERE placed >= ? AND status NOT IN ('confirmed_canceled', 'filled') AND mode = ?
                       ORDER BY placed DESC''',
                    (today_start.isoformat(), mode)
                )
            else:
                cursor = conn.execute(
                    '''SELECT * FROM orders
                       WHERE placed >= ? AND status NOT IN ('confirmed_canceled', 'filled')
                       ORDER BY placed DESC''',
                    (today_start.isoformat(),)
                )
            return [self._row_to_order(row) for row in cursor.fetchall()]

    def get_active_orders_by_instrument(self, instrument: str, statuses: list[str]) -> List[Order]:
        """Busca órdenes activas por instrumento y lista de estados"""
        with self._get_connection() as conn:
            placeholders = ','.join(['?'] * len(statuses))
            query = f'SELECT * FROM orders WHERE instrument = ? AND status IN ({placeholders}) ORDER BY id ASC'
            cursor = conn.execute(query, (instrument, *statuses))
            return [self._row_to_order(row) for row in cursor.fetchall()]
