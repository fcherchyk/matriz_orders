import sqlite3
from datetime import datetime, date
from pathlib import Path
from typing import Optional, List

from model.order_model import ClosingOrder


class ClosingOrderRepository:
    """Repositorio para persistir closing orders en SQLite"""

    def __init__(self, db_path: str = "data/orders.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        """Crea la tabla de closing_orders si no existe"""
        with self._get_connection() as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS closing_orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    original_order_id INTEGER NOT NULL,
                    ticker TEXT NOT NULL,
                    term TEXT NOT NULL,
                    closing_type TEXT NOT NULL DEFAULT 'market',
                    status TEXT NOT NULL DEFAULT 'pending',
                    created_at TIMESTAMP NOT NULL
                )
            ''')
            # Agregar columnas si no existen (para migracion de DBs existentes)
            try:
                conn.execute('ALTER TABLE closing_orders ADD COLUMN operated_size INTEGER DEFAULT 0')
            except sqlite3.OperationalError:
                pass  # Columna ya existe
            try:
                conn.execute('ALTER TABLE closing_orders ADD COLUMN operated_volume REAL DEFAULT 0')
            except sqlite3.OperationalError:
                pass  # Columna ya existe
            conn.commit()

    def _row_to_closing_order(self, row: sqlite3.Row) -> ClosingOrder:
        """Convierte una fila de la BD a un objeto ClosingOrder"""
        return ClosingOrder(
            id=row['id'],
            original_order_id=row['original_order_id'],
            ticker=row['ticker'],
            term=row['term'],
            closing_type=row['closing_type'],
            status=row['status'],
            created_at=datetime.fromisoformat(row['created_at']) if row['created_at'] else None,
            operated_size=row['operated_size'] if 'operated_size' in row.keys() and row['operated_size'] else 0,
            operated_volume=row['operated_volume'] if 'operated_volume' in row.keys() and row['operated_volume'] else 0.0,
        )

    def save(self, closing_order: ClosingOrder) -> ClosingOrder:
        """Guarda un closing order nuevo y retorna con su ID asignado"""
        with self._get_connection() as conn:
            cursor = conn.execute('''
                INSERT INTO closing_orders (
                    original_order_id, ticker, term, closing_type, status,
                    operated_size, operated_volume, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                closing_order.original_order_id,
                closing_order.ticker,
                closing_order.term,
                closing_order.closing_type,
                closing_order.status,
                closing_order.operated_size,
                closing_order.operated_volume,
                closing_order.created_at.isoformat() if closing_order.created_at else None,
            ))
            conn.commit()
            closing_order.id = cursor.lastrowid
            return closing_order

    def update(self, closing_order: ClosingOrder) -> ClosingOrder:
        """Actualiza un closing order existente"""
        with self._get_connection() as conn:
            conn.execute('''
                UPDATE closing_orders SET
                    status = ?,
                    operated_size = ?,
                    operated_volume = ?
                WHERE id = ?
            ''', (
                closing_order.status,
                closing_order.operated_size,
                closing_order.operated_volume,
                closing_order.id,
            ))
            conn.commit()
            return closing_order

    def get_by_id(self, closing_order_id: int) -> Optional[ClosingOrder]:
        """Recupera un closing order por su ID"""
        with self._get_connection() as conn:
            cursor = conn.execute('SELECT * FROM closing_orders WHERE id = ?', (closing_order_id,))
            row = cursor.fetchone()
            return self._row_to_closing_order(row) if row else None

    def get_by_original_order_id(self, original_order_id: int) -> Optional[ClosingOrder]:
        """Recupera un closing order por el ID de la orden original"""
        with self._get_connection() as conn:
            cursor = conn.execute(
                'SELECT * FROM closing_orders WHERE original_order_id = ?',
                (original_order_id,)
            )
            row = cursor.fetchone()
            return self._row_to_closing_order(row) if row else None

    def get_pending(self) -> List[ClosingOrder]:
        """Retorna todos los closing orders pendientes"""
        with self._get_connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM closing_orders WHERE status = 'pending' ORDER BY created_at ASC"
            )
            return [self._row_to_closing_order(row) for row in cursor.fetchall()]

    def get_today_closing_orders(self) -> List[ClosingOrder]:
        """Retorna todos los closing orders del dia actual"""
        today = date.today()
        today_start = datetime.combine(today, datetime.min.time())
        with self._get_connection() as conn:
            cursor = conn.execute(
                'SELECT * FROM closing_orders WHERE created_at >= ? ORDER BY created_at DESC',
                (today_start.isoformat(),)
            )
            return [self._row_to_closing_order(row) for row in cursor.fetchall()]