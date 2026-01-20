import asyncio
import re
from datetime import timedelta
from pathlib import Path
from typing import Optional, Callable, Union, List


class ReplaySource:
    """Simula la recepción de mensajes desde un archivo de log"""

    def __init__(self, on_message: Optional[Callable] = None, on_status_change: Optional[Callable] = None):
        self.on_message = on_message
        self.on_status_change = on_status_change
        self.log_dir = Path('logs/ws')
        self.current_file: Optional[str] = None
        self.status = "Waiting"  # Waiting, Running, Finished
        self.running = False
        self.message_count = 0

    def get_available_files(self) -> list[str]:
        """Retorna lista de archivos de log disponibles (solo archivos de price)"""
        if not self.log_dir.exists():
            return []
        files = sorted([f.name for f in self.log_dir.glob('price-*.txt')])
        return files

    def parse_timedelta(self, time_str: str) -> timedelta:
        """Parsea un string de tiempo como '0:00:05.487710' a timedelta"""
        match = re.match(r'(\d+):(\d+):(\d+)\.(\d+)', time_str)
        if match:
            hours = int(match.group(1))
            minutes = int(match.group(2))
            seconds = int(match.group(3))
            microseconds = int(match.group(4))
            return timedelta(hours=hours, minutes=minutes, seconds=seconds, microseconds=microseconds)
        return timedelta(0)

    def parse_log_line(self, line: str) -> tuple[timedelta, str]:
        """Parsea una línea de log y retorna (timedelta, mensaje)"""
        parts = line.strip().split(' ', 1)
        if len(parts) == 2:
            time_delta = self.parse_timedelta(parts[0])
            message = parts[1]
            return time_delta, message
        return timedelta(0), line.strip()

    async def _notify_status(self, status: str):
        self.status = status
        if self.on_status_change:
            await self.on_status_change(status, self.message_count)

    async def start_replay(self, files: Union[List[str], str]):
        """Inicia la reproducción de uno o más archivos de log en orden"""
        # Soportar tanto un solo archivo como una lista
        if isinstance(files, str):
            files = [files]

        # Ordenar archivos cronológicamente por nombre
        files = sorted(files)

        self.running = True
        self.message_count = 0

        await self._notify_status("Running")

        for filename in files:
            if not self.running:
                break

            file_path = self.log_dir / filename
            if not file_path.exists():
                continue  # Saltar archivos no encontrados

            self.current_file = filename
            await self._notify_status("Running")

            # Leer todas las líneas del archivo
            with open(file_path, 'r') as f:
                lines = [line for line in f.readlines() if line.strip()]

            if not lines:
                continue

            # Parsear todas las líneas
            entries = [self.parse_log_line(line) for line in lines]

            # La primera entrada define el tiempo base
            prev_time = entries[0][0]

            for time_delta, message in entries:
                if not self.running:
                    break

                # Calcular el tiempo de espera respecto al mensaje anterior
                wait_time = (time_delta - prev_time).total_seconds()
                if wait_time > 0:
                    await asyncio.sleep(wait_time)

                # Procesar el mensaje
                self.message_count += 1
                if self.on_message:
                    await self.on_message(message)

                prev_time = time_delta

        self.running = False
        await self._notify_status("Waiting")

    def stop(self):
        """Detiene la reproducción actual"""
        self.running = False

    def get_state(self) -> dict:
        return {
            'status': self.status,
            'current_file': self.current_file,
            'message_count': self.message_count,
            'available_files': self.get_available_files()
        }
