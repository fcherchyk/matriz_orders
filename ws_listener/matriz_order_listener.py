import asyncio
import websockets
from websockets.exceptions import ConnectionClosedError
from ws_listener.matriz_websocket_listener import MatrizWebsocketListener

APP_PING_INTERVAL = 40  # seconds, same as Matriz web client


class MatrizOrderListener(MatrizWebsocketListener):

    def __init__(self, message_processor, session=None):
        print(f"[MatrizOrderListener] __init__ - session provided: {session is not None}")
        super().__init__(message_processor, session)
        self.receiving = False
        self._ping_task = None

    async def run(self):
        print(f"[MatrizOrderListener] run() iniciado")
        try:
            await self.connect()
            await self.listen()
        except Exception as e:
            print(f"[MatrizOrderListener] Error en run(): {e}")
            raise
        finally:
            if self._ping_task and not self._ping_task.done():
                self._ping_task.cancel()
            print(f"[MatrizOrderListener] run() finalizado")

    async def _app_level_ping(self):
        """
        Envia 'ping' como texto cada 40s, igual que el cliente JS de Matriz.
        El servidor espera este ping de aplicacion (no pings de protocolo WS).
        """
        try:
            while True:
                await asyncio.sleep(APP_PING_INTERVAL)
                if self.websocket:
                    await self.websocket.send("ping")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"[MatrizOrderListener] Error en app-level ping: {e}")

    async def connect(self):
        print(f"[MatrizOrderListener] connect() - Iniciando conexión...")
        print(f"[MatrizOrderListener] session_id: {self.session.session_id[:20] if self.session.session_id else 'None'}...")
        self.message_count = 0

        self.uri = f"wss://{self.session.WS_HOST}/ws?session_id={self.session.session_id}&conn_id={self.session.conn_id}"
        print(f"[MatrizOrderListener] Conectando a WebSocket...")

        try:
            self.websocket = await websockets.connect(
                self.uri,
                origin=self.session.BASE_URL,
                ping_interval=None,  # Deshabilitar pings de protocolo WS
                ping_timeout=None,
                max_size=10 * 1024 * 1024,
            )
            print(f"[MatrizOrderListener] WebSocket conectado")
        except Exception as e:
            print(f"[MatrizOrderListener] Error conectando WebSocket: {e}")
            raise

        # Iniciar ping de aplicacion (texto "ping" cada 40s, como el JS)
        self._ping_task = asyncio.create_task(self._app_level_ping())

        # Suscribir a orderevent con replace=true (como el JS)
        om = f"""{{"_req": "S", "topicType": "orderevent", "topics": ["orderevent.{self.session.requester.account}"], "replace": true}}"""

        print(f"[MatrizOrderListener] Suscribiendo a orderevent para cuenta: {self.session.requester.account}...")
        await self.send_message(om)
        await self.change_status("Running")
        print(f"[MatrizOrderListener] connect() completado - status: Running")

    async def process_message(self, message):
        """
        Procesa mensajes del WebSocket que pueden contener O:{...} o E:{...}
        Los mensajes pueden venir como:
        - JSON array: ["O:{json}","E:{json}"]
        - String plano: O:{json} o E:{json}
        - String "pong" (respuesta al ping de aplicacion)
        """
        # Ignorar pong del servidor
        if message == "pong":
            return

        # Caso 1: Mensaje plano que empieza con O: o E:
        if message.startswith('O:') or message.startswith('E:'):
            if not self.receiving:
                print(f"[MatrizOrderListener] Primer mensaje de orden recibido! receiving=True")
            self.receiving = True
            await self.message_processor.process_direct(message)
            return

        # Caso 2: Intentar parsear como JSON array
        try:
            import json
            messages_list = json.loads(message)
            if isinstance(messages_list, list):
                for m in messages_list:
                    if m and len(m) > 2 and m[0] in ['O', 'E'] and m[1] == ':':
                        if not self.receiving:
                            print(f"[MatrizOrderListener] Primer mensaje de orden recibido! receiving=True")
                        self.receiving = True
                        await self.message_processor.process_direct(m)
                return
        except (json.JSONDecodeError, ValueError):
            pass

    async def close(self):
        """Cierra la conexión WebSocket de forma limpia"""
        print(f"[MatrizOrderListener] close() - Cerrando conexión...")
        if self._ping_task and not self._ping_task.done():
            self._ping_task.cancel()
        if self.websocket:
            try:
                await self.websocket.close()
                print(f"[MatrizOrderListener] WebSocket cerrado")
            except Exception as e:
                print(f"[MatrizOrderListener] Error cerrando WebSocket: {e}")
        self.receiving = False
