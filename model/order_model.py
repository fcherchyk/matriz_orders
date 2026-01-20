from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class Order:
    """Modelo de orden con estados y timestamps de transicion"""

    # Datos de la orden
    id: Optional[int] = None
    broker_order_id: Optional[str] = None
    front_id: str = ""
    account: str = ""
    side: str = ""  # "1" = buy, "2" = sell
    quantity: int = 0
    instrument: str = ""
    order_type: str = ""  # "1" = market, "2" = limit, "K" = caution
    price: Optional[float] = None
    total_amount: float = 0

    # Estado actual
    status: str = "placed"

    # Modo de ejecucion: "prod" o "paper"
    mode: str = "prod"

    # Vinculo con orden original (para ordenes compensatorias)
    original_order_id: Optional[int] = None

    # Campos de ejecucion acumulada
    operated_size: int = 0  # Cantidad total ejecutada
    operated_volume: float = 0.0  # Volumen total ejecutado (size * price)

    # Timestamps de cada estado (datetime de cuando paso a ese estado)
    placed: Optional[datetime] = None
    confirmed: Optional[datetime] = None
    partial: Optional[datetime] = None
    filled: Optional[datetime] = None
    canceled: Optional[datetime] = None
    confirmed_canceled: Optional[datetime] = None

    def to_dict(self) -> dict:
        """Convierte la orden a diccionario"""
        return {
            'id': self.id,
            'broker_order_id': self.broker_order_id,
            'front_id': self.front_id,
            'account': self.account,
            'side': self.side,
            'quantity': self.quantity,
            'instrument': self.instrument,
            'order_type': self.order_type,
            'price': self.price,
            'total_amount': self.total_amount,
            'status': self.status,
            'mode': self.mode,
            'original_order_id': self.original_order_id,
            'operated_size': self.operated_size,
            'operated_volume': self.operated_volume,
            'placed': self.placed.isoformat() if self.placed else None,
            'confirmed': self.confirmed.isoformat() if self.confirmed else None,
            'partial': self.partial.isoformat() if self.partial else None,
            'filled': self.filled.isoformat() if self.filled else None,
            'canceled': self.canceled.isoformat() if self.canceled else None,
            'confirmed_canceled': self.confirmed_canceled.isoformat() if self.confirmed_canceled else None,
        }


@dataclass
class ClosingOrder:
    """
    Modelo para ordenes de cierre.
    Representa la intencion de cerrar una posicion originada por una orden.
    Las ordenes compensatorias generadas tendran original_order_id apuntando a la orden original.
    """

    id: Optional[int] = None
    original_order_id: int = 0  # ID de la orden que se quiere cerrar
    ticker: str = ""  # Ticker para compensar (puede diferir del original)
    term: str = ""  # Plazo (CI, 24hs, etc.)
    closing_type: str = "market"  # Tipo de cierre: "market", "limit", etc.
    status: str = "pending"  # pending, partial, completed, canceled
    created_at: Optional[datetime] = None

    # Campos de ejecucion acumulada de las ordenes compensatorias
    operated_size: int = 0  # Cantidad total ejecutada en ordenes compensatorias
    operated_volume: float = 0.0  # Volumen total ejecutado en ordenes compensatorias

    def to_dict(self) -> dict:
        """Convierte el closing order a diccionario"""
        return {
            'id': self.id,
            'original_order_id': self.original_order_id,
            'ticker': self.ticker,
            'term': self.term,
            'closing_type': self.closing_type,
            'status': self.status,
            'operated_size': self.operated_size,
            'operated_volume': self.operated_volume,
            'created_at': self.created_at.isoformat() if self.created_at else None,
        }
