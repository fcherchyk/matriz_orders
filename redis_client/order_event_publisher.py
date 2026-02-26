import os
from typing import Optional

import redis.asyncio as redis
from dotenv import load_dotenv

load_dotenv()


class RedisOrderEventPublisher:
    """
    Publica mensajes de eventos de ordenes (O:/E:) al canal Redis.
    Reconecta automaticamente si Redis no estaba disponible al inicio.
    """

    def __init__(self):
        self.channel = os.getenv('REDIS_ORDER_EVENTS_CHANNEL', 'order_events')
        self._redis: Optional[redis.Redis] = None

    def set_redis(self, redis_client: redis.Redis):
        """Configura la conexion Redis."""
        self._redis = redis_client

    async def _ensure_redis(self) -> bool:
        """Intenta obtener conexion Redis si no hay una."""
        if self._redis is not None:
            try:
                await self._redis.ping()
                return True
            except Exception:
                self._redis = None

        try:
            from redis_client import get_redis
            self._redis = await get_redis()
            await self._redis.ping()
            return True
        except Exception:
            self._redis = None
            return False

    async def publish(self, message: str):
        """
        Publica un mensaje al canal de eventos de ordenes.

        Args:
            message: Mensaje crudo (ej: "O:{json}" o "E:{json}")
        """
        if not await self._ensure_redis():
            return

        try:
            await self._redis.publish(self.channel, message)
        except Exception as e:
            print(f"[RedisOrderEventPublisher] Error publicando: {e}")
            self._redis = None
