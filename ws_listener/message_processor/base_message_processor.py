import asyncio
import re
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, Dict


class BaseMessageProcessor(ABC):
    """
    Base class para procesadores de mensajes de precios.

    Usa patrón productor-consumidor donde solo se guarda el último mensaje
    por tipo+instrumento. Los consumidores procesan el mensaje más reciente
    y los anteriores se descartan (útil para datos de mercado de alta frecuencia).
    """

    def __init__(self):
        self.tickers = ['AL30']
        self.status = "Initialized"
        self.message_count = 0
        self.ignored_message_count = 0
        self.init_time = datetime.now()

        # Buffer de últimos mensajes: key = "tipo:instrumento" (ej: "M:bm_MERV_AL30_CI")
        self._message_buffer: Dict[str, Optional[str]] = {}

        # Lock para acceso seguro al buffer
        self._buffer_lock = asyncio.Lock()

        # Flag para controlar consumidores
        self._running = False
        self._consumer_tasks: list[asyncio.Task] = []

        # Tiempo de espera del consumidor cuando no hay mensaje
        self.consumer_sleep_ms = 5

    def _parse_message_key(self, message: str) -> Optional[str]:
        """Extrae la clave del mensaje: tipo:instrumento"""
        if not message or len(message) < 2:
            return None

        msg_type = message[0]  # 'M' o 'B'
        if msg_type not in ['M', 'B']:
            return None

        # Extraer instrumento del mensaje
        # Formato M: "M:bm_MERV_AL30_CI|..."
        # Formato B: "B:bm_MERV_AL30_CI!..."
        if msg_type == 'M':
            match = re.match(r'M:(bm_MERV_[^|]+)', message)
        else:
            match = re.match(r'B:(bm_MERV_[^!]+)', message)

        if match:
            instrument = match.group(1)
            return f"{msg_type}:{instrument}"

        return None

    async def execute(self, message: str):
        """
        Guarda el mensaje en el buffer y libera inmediatamente.
        No bloquea - el procesamiento real lo hacen los consumidores.
        """
        key = self._parse_message_key(message)
        if not key:
            return

        async with self._buffer_lock:
            # Si hay un mensaje sin procesar, contarlo como ignorado
            if self._message_buffer.get(key) is not None:
                self.ignored_message_count += 1
            self._message_buffer[key] = message
            self.message_count += 1

    async def _consumer_loop(self, key: str):
        """Loop de consumidor para una clave específica"""
        while self._running:
            message = None

            # Intentar tomar el mensaje
            async with self._buffer_lock:
                if key in self._message_buffer and self._message_buffer[key] is not None:
                    message = self._message_buffer[key]
                    self._message_buffer[key] = None

            if message is not None:
                # Procesar el mensaje fuera del lock
                await self.process_message(message)
            else:
                # No hay mensaje nuevo, dormir
                await asyncio.sleep(self.consumer_sleep_ms / 1000)

    @abstractmethod
    async def process_message(self, message: str):
        """
        Procesa un mensaje. Debe ser implementado por subclases.

        Args:
            message: Contenido completo del mensaje
        """
        pass

    async def update_status(self, status: str):
        """Actualiza el estado del processor"""
        self.status = status
        print(f"[{self.__class__.__name__}] Status: {status}")

    def get_active_keys(self) -> list[str]:
        """Retorna las claves activas en el buffer"""
        return list(self._message_buffer.keys())

    async def start_consumer(self):
        """Inicia los consumidores para todas las combinaciones posibles"""
        self._running = True

        # Generar claves para todas las combinaciones tipo + ticker + term
        keys = []
        terms = ['CI', '24hs']
        for msg_type in ['M', 'B']:
            for ticker in self.tickers:
                for term in terms:
                    key = f"{msg_type}:bm_MERV_{ticker}_{term}"
                    keys.append(key)
                    self._message_buffer[key] = None

        # Crear tareas de consumidores
        for key in keys:
            task = asyncio.create_task(self._consumer_loop(key))
            self._consumer_tasks.append(task)

        print(f"[{self.__class__.__name__}] Started {len(keys)} consumers")

    async def stop_consumer(self):
        """Detiene todos los consumidores"""
        self._running = False

        # Esperar que terminen las tareas
        for task in self._consumer_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        self._consumer_tasks.clear()
        print(f"[{self.__class__.__name__}] Consumers stopped")

    def get_state(self) -> dict:
        """Retorna el estado del processor"""
        return {
            'status': self.status,
            'message_count': self.message_count,
            'ignored_message_count': self.ignored_message_count,
            'uptime_seconds': (datetime.now() - self.init_time).total_seconds(),
            'active_keys': self.get_active_keys(),
            'consumer_running': self._running
        }
