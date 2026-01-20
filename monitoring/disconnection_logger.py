import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Callable
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


@dataclass
class DisconnectionEvent:
    source: str  # 'matriz_host' o 'websocket'
    event_type: str  # 'disconnected' o 'reconnected'
    timestamp: datetime
    disconnection_duration_seconds: Optional[float] = None

    def to_dict(self) -> dict:
        return {
            'source': self.source,
            'event_type': self.event_type,
            'timestamp': self.timestamp.isoformat(),
            'disconnection_duration_seconds': self.disconnection_duration_seconds
        }

    def format_log_line(self) -> str:
        time_str = self.timestamp.strftime('%Y-%m-%d %H:%M:%S')
        if self.event_type == 'disconnected':
            return f"[{time_str}] {self.source.upper()} - DESCONECTADO"
        else:
            duration = f" (desconectado {self.disconnection_duration_seconds:.1f}s)" if self.disconnection_duration_seconds else ""
            return f"[{time_str}] {self.source.upper()} - RECONECTADO{duration}"


class DisconnectionLogger:

    def __init__(self, on_event: Optional[Callable] = None):
        self.on_event = on_event
        self.events: list[DisconnectionEvent] = []
        self.max_events = 100

        # Estado actual para trackear duracion de desconexiones
        self.matriz_host_disconnected_at: Optional[datetime] = None
        self.websocket_disconnected_at: Optional[datetime] = None
        self.matriz_host_last_status: Optional[bool] = None
        self.websocket_last_status: Optional[str] = None

        # Archivo de log
        log_dir = Path(os.getenv('LOG_DIR', 'logs'))
        log_dir.mkdir(exist_ok=True)
        self.log_file = log_dir / 'disconnections.log'

    def _write_to_file(self, event: DisconnectionEvent):
        with open(self.log_file, 'a') as f:
            f.write(event.format_log_line() + '\n')

    def _add_event(self, event: DisconnectionEvent):
        self.events.append(event)
        if len(self.events) > self.max_events:
            self.events.pop(0)

        self._write_to_file(event)
        print(event.format_log_line())

        if self.on_event:
            import asyncio
            asyncio.create_task(self.on_event(event))

    def update_matriz_host_status(self, is_healthy: bool):
        now = datetime.now()

        # Primera vez, solo guardar estado
        if self.matriz_host_last_status is None:
            self.matriz_host_last_status = is_healthy
            return

        # Detectar cambio de estado
        if self.matriz_host_last_status and not is_healthy:
            # Paso de healthy a unhealthy -> desconexion
            self.matriz_host_disconnected_at = now
            event = DisconnectionEvent(
                source='matriz_host',
                event_type='disconnected',
                timestamp=now
            )
            self._add_event(event)

        elif not self.matriz_host_last_status and is_healthy:
            # Paso de unhealthy a healthy -> reconexion
            duration = None
            if self.matriz_host_disconnected_at:
                duration = (now - self.matriz_host_disconnected_at).total_seconds()
            event = DisconnectionEvent(
                source='matriz_host',
                event_type='reconnected',
                timestamp=now,
                disconnection_duration_seconds=duration
            )
            self._add_event(event)
            self.matriz_host_disconnected_at = None

        self.matriz_host_last_status = is_healthy

    def update_websocket_status(self, status: str):
        now = datetime.now()

        # Primera vez, solo guardar estado
        if self.websocket_last_status is None:
            self.websocket_last_status = status
            return

        # Detectar cambio de estado
        if self.websocket_last_status == 'Running' and status == 'Disconnected':
            # Desconexion
            self.websocket_disconnected_at = now
            event = DisconnectionEvent(
                source='websocket',
                event_type='disconnected',
                timestamp=now
            )
            self._add_event(event)

        elif self.websocket_last_status == 'Disconnected' and status == 'Running':
            # Reconexion
            duration = None
            if self.websocket_disconnected_at:
                duration = (now - self.websocket_disconnected_at).total_seconds()
            event = DisconnectionEvent(
                source='websocket',
                event_type='reconnected',
                timestamp=now,
                disconnection_duration_seconds=duration
            )
            self._add_event(event)
            self.websocket_disconnected_at = None

        self.websocket_last_status = status

    def get_recent_events(self, count: int = 2) -> list[dict]:
        """Obtiene los ultimos N eventos"""
        return [e.to_dict() for e in self.events[-count:]]

    def get_events_by_source(self, source: str, count: int = 2) -> list[dict]:
        """Obtiene los ultimos N eventos de una fuente especifica"""
        filtered = [e for e in self.events if e.source == source]
        return [e.to_dict() for e in filtered[-count:]]

    def get_all_recent(self, count_per_source: int = 2) -> dict:
        """Obtiene los ultimos eventos por cada fuente"""
        return {
            'matriz_host': self.get_events_by_source('matriz_host', count_per_source),
            'websocket': self.get_events_by_source('websocket', count_per_source)
        }
