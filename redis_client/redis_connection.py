import os
from typing import Optional

import redis.asyncio as redis
from dotenv import load_dotenv

load_dotenv()

_redis_client: Optional[redis.Redis] = None


async def get_redis() -> redis.Redis:
    """Obtiene o crea la conexión Redis async. Recrea si la conexion esta rota."""
    global _redis_client

    if _redis_client is not None:
        try:
            await _redis_client.ping()
            return _redis_client
        except Exception:
            # Conexion rota, recrear
            try:
                await _redis_client.aclose()
            except Exception:
                pass
            _redis_client = None

    host = os.getenv('REDIS_HOST', 'localhost')
    port = int(os.getenv('REDIS_PORT', '6379'))
    password = os.getenv('REDIS_PASSWORD', None)
    db = int(os.getenv('REDIS_DB', '0'))

    _redis_client = redis.Redis(
        host=host,
        port=port,
        password=password if password else None,
        db=db,
        decode_responses=True
    )

    return _redis_client


async def close_redis():
    """Cierra la conexión Redis."""
    global _redis_client

    if _redis_client is not None:
        await _redis_client.aclose()
        _redis_client = None
