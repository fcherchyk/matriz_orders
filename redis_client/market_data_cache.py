from typing import Optional, Dict
from datetime import datetime


class MarketDataCache:
    """
    Cache de datos de mercado en tiempo real.
    Recibe datos pre-parseados del price listener via Redis.
    Formato esperado: {"bid", "ask", "last", "bid_size", "ask_size", "book": {"bids": [[p,q],...], "asks": [[p,q],...]}}
    """

    def __init__(self):
        self.data: Dict[str, dict] = {}

    def update_from_price_listener(self, instrument: str, data: dict):
        """
        Actualiza el cache desde un mensaje del price listener.
        El price listener ya envia los datos pre-parseados.

        Args:
            instrument: Identificador del instrumento (ej: "bm_MERV_AL30_CI")
            data: Datos del campo "data" del mensaje del price listener
        """
        self.data[instrument] = {
            "bid": data.get("bid"),
            "ask": data.get("ask"),
            "last": data.get("last"),
            "bid_size": data.get("bid_size"),
            "ask_size": data.get("ask_size"),
            "book": data.get("book", {"bids": [], "asks": []}),
            "updated_at": data.get("updated_at") or datetime.now().isoformat()
        }

    def get_market_data(self, instrument: str) -> Optional[dict]:
        """Obtiene los datos de mercado para un instrumento."""
        return self.data.get(instrument)

    def get_execution_price(self, instrument: str, side: str) -> Optional[float]:
        """
        Obtiene el precio al que se ejecutaria una orden market.
        Ask para compra (side=1), bid para venta (side=2).
        """
        market_data = self.get_market_data(instrument)
        if not market_data:
            return None

        if side == "1":
            return market_data.get("ask")
        elif side == "2":
            return market_data.get("bid")

        return None

    def get_book_levels(self, instrument: str, num_levels: int = 3) -> Optional[dict]:
        """Obtiene los niveles del libro de ordenes."""
        market_data = self.get_market_data(instrument)
        if not market_data or "book" not in market_data:
            return None

        book = market_data["book"]
        return {
            "bids": book.get("bids", [])[:num_levels],
            "asks": book.get("asks", [])[:num_levels]
        }

    def get_all_instruments(self) -> list[str]:
        """Retorna lista de todos los instrumentos con datos en cache."""
        return list(self.data.keys())

    def clear(self):
        """Limpia todo el cache."""
        self.data.clear()
