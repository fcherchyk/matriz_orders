from typing import Optional, Dict
from datetime import datetime


class MarketDataCache:
    """
    Cache de datos de mercado en tiempo real.
    Mantiene precios de puntas (bid/ask), ultimo precio operado y niveles del libro.
    """

    def __init__(self):
        self.data: Dict[str, dict] = {}

    def update_from_message(self, instrument: str, message_data: dict):
        """
        Actualiza el cache desde un mensaje de market data (M: o B:).

        Args:
            instrument: Identificador del instrumento (ej: "bm_MERV_AL30_CI")
            message_data: Datos parseados del mensaje
        """
        if instrument not in self.data:
            self.data[instrument] = {
                "bid": None,
                "ask": None,
                "last": None,
                "bid_size": None,
                "ask_size": None,
                "book": {"bids": [], "asks": []},
                "updated_at": None
            }

        market_data = self.data[instrument]

        # Actualizar desde mensaje M: (market data simple)
        if "LA" in message_data:
            try:
                market_data["last"] = float(message_data["LA"])
            except (ValueError, TypeError):
                pass

        if "BI" in message_data:
            try:
                market_data["bid"] = float(message_data["BI"])
            except (ValueError, TypeError):
                pass

        if "OF" in message_data:
            try:
                market_data["ask"] = float(message_data["OF"])
            except (ValueError, TypeError):
                pass

        if "BIDS" in message_data:
            try:
                market_data["bid_size"] = int(message_data["BIDS"])
            except (ValueError, TypeError):
                pass

        if "OFFS" in message_data:
            try:
                market_data["ask_size"] = int(message_data["OFFS"])
            except (ValueError, TypeError):
                pass

        # Actualizar desde mensaje B: (book con niveles)
        if "bids" in message_data:
            try:
                bids = []
                for bid in message_data["bids"]:
                    price = float(bid.get("price", 0))
                    size = int(bid.get("size", 0))
                    if price > 0 and size > 0:
                        bids.append([price, size])
                market_data["book"]["bids"] = sorted(bids, key=lambda x: x[0], reverse=True)

                if bids:
                    market_data["bid"] = bids[0][0]
                    market_data["bid_size"] = bids[0][1]
            except (ValueError, TypeError, KeyError):
                pass

        if "asks" in message_data:
            try:
                asks = []
                for ask in message_data["asks"]:
                    price = float(ask.get("price", 0))
                    size = int(ask.get("size", 0))
                    if price > 0 and size > 0:
                        asks.append([price, size])
                market_data["book"]["asks"] = sorted(asks, key=lambda x: x[0])

                if asks:
                    market_data["ask"] = asks[0][0]
                    market_data["ask_size"] = asks[0][1]
            except (ValueError, TypeError, KeyError):
                pass

        market_data["updated_at"] = datetime.now()

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
            "bids": book["bids"][:num_levels],
            "asks": book["asks"][:num_levels]
        }

    def get_all_instruments(self) -> list[str]:
        """Retorna lista de todos los instrumentos con datos en cache."""
        return list(self.data.keys())

    def clear(self):
        """Limpia todo el cache."""
        self.data.clear()
