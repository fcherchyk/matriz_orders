import asyncio
import os
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Optional
import aiohttp
from dotenv import load_dotenv

load_dotenv()


@dataclass
class HealthStatus:
    name: str
    url: str
    is_healthy: bool = False
    response_time_ms: float = 0
    last_check: Optional[datetime] = None
    error_message: Optional[str] = None


@dataclass
class MonitorState:
    network_checks: dict[str, HealthStatus] = field(default_factory=dict)
    matriz_check: Optional[HealthStatus] = None
    websocket_status: str = "Disconnected"
    websocket_last_update: Optional[datetime] = None


class HealthChecker:

    def __init__(self, on_status_change: Optional[Callable] = None, disconnection_logger=None):
        self.state = MonitorState()
        self.on_status_change = on_status_change
        self.disconnection_logger = disconnection_logger
        self.check_interval = int(os.getenv('HEALTH_CHECK_INTERVAL', '5'))
        self.matriz_host = os.getenv('MATRIZ_HOST', 'matriz.eco.xoms.com.ar')
        self.running = False

        self.network_urls = {
            'Google': 'https://www.google.com',
            'Cloudflare': 'https://1.1.1.1',
            'GitHub': 'https://github.com',
        }

    async def check_url(self, name: str, url: str) -> HealthStatus:
        status = HealthStatus(name=name, url=url)
        start_time = time.time()

        try:
            timeout = aiohttp.ClientTimeout(total=5)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as response:
                    status.is_healthy = response.status < 400
                    status.response_time_ms = (time.time() - start_time) * 1000
                    status.last_check = datetime.now()
        except asyncio.TimeoutError:
            status.is_healthy = False
            status.error_message = "Timeout"
            status.last_check = datetime.now()
        except Exception as e:
            status.is_healthy = False
            status.error_message = str(e)[:50]
            status.last_check = datetime.now()

        return status

    async def check_network(self):
        tasks = [self.check_url(name, url) for name, url in self.network_urls.items()]
        results = await asyncio.gather(*tasks)
        for result in results:
            self.state.network_checks[result.name] = result

    async def check_matriz(self):
        url = f"https://{self.matriz_host}"
        self.state.matriz_check = await self.check_url('Matriz', url)
        if self.disconnection_logger:
            self.disconnection_logger.update_matriz_host_status(self.state.matriz_check.is_healthy)

    def update_websocket_status(self, status: str):
        self.state.websocket_status = status
        self.state.websocket_last_update = datetime.now()
        if self.disconnection_logger:
            self.disconnection_logger.update_websocket_status(status)

    async def run_checks(self):
        await asyncio.gather(
            self.check_network(),
            self.check_matriz()
        )
        if self.on_status_change:
            await self.on_status_change(self.get_state())

    def get_state(self) -> dict:
        return {
            'network': {
                name: {
                    'url': check.url,
                    'healthy': check.is_healthy,
                    'response_time_ms': round(check.response_time_ms, 2),
                    'last_check': check.last_check.isoformat() if check.last_check else None,
                    'error': check.error_message
                }
                for name, check in self.state.network_checks.items()
            },
            'matriz': {
                'url': self.state.matriz_check.url if self.state.matriz_check else None,
                'healthy': self.state.matriz_check.is_healthy if self.state.matriz_check else False,
                'response_time_ms': round(self.state.matriz_check.response_time_ms, 2) if self.state.matriz_check else 0,
                'last_check': self.state.matriz_check.last_check.isoformat() if self.state.matriz_check and self.state.matriz_check.last_check else None,
                'error': self.state.matriz_check.error_message if self.state.matriz_check else None
            },
            'websocket': {
                'status': self.state.websocket_status,
                'last_update': self.state.websocket_last_update.isoformat() if self.state.websocket_last_update else None
            },
            'config': {
                'check_interval': self.check_interval
            }
        }

    async def start(self):
        self.running = True
        while self.running:
            await self.run_checks()
            await asyncio.sleep(self.check_interval)

    def stop(self):
        self.running = False
