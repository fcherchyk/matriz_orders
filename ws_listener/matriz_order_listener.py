import asyncio
import websockets
from websockets.exceptions import ConnectionClosedError
from ws_listener.matriz_websocket_listener import MatrizWebsocketListener


class MatrizOrderListener(MatrizWebsocketListener):

    def __init__(self, message_processor, session=None, ping_interval: int = 10, ping_timeout: int = 8):
        print(f"[MatrizOrderListener] __init__ - session provided: {session is not None}, ping_interval={ping_interval}, ping_timeout={ping_timeout}")
        super().__init__(message_processor, session)
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        self.receiving = False

    async def run(self):
        print(f"[MatrizOrderListener] run() iniciado")
        try:
            await self.connect()
            await self.listen()
        except Exception as e:
            print(f"[MatrizOrderListener] Error en run(): {e}")
            raise
        finally:
            print(f"[MatrizOrderListener] run() finalizado")

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
                ping_interval=self.ping_interval,
                ping_timeout=self.ping_timeout,
                max_size=10 * 1024 * 1024,  # 10MB max message size (default is 1MB)
            )
            print(f"[MatrizOrderListener] WebSocket conectado")
        except Exception as e:
            print(f"[MatrizOrderListener] Error conectando WebSocket: {e}")
            raise

        print(f"[MatrizOrderListener] Recibiendo mensajes iniciales...")
        for i in range(3):  # 3 initial messages
            msg = await self.websocket.recv()
            print(f"[MatrizOrderListener] Mensaje inicial {i+1}: {msg[:100] if len(msg) > 100 else msg}")

        om = f"""{{"_req": "S", "topicType": "orderevent", "topics": ["orderevent.{self.session.requester.account}"], "replace": false}}"""

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
        - String con array de M: ["M:data1","M:data2"]
        """
        # Debug: log del mensaje raw (primeros 500 chars para no llenar logs)
        # print(f"[MatrizOrderListener] Mensaje raw ({len(message)} chars): {message[:500]}")

        # Caso 1: Mensaje plano que empieza con O: o E: (mensaje completo sin array)
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
            # Si falla el parsing JSON, intentar fallback
            pass

        # Caso 3: Fallback para mensajes M: (market data) que vienen como strings
        # Solo procesamos si NO es un mensaje O/E (esos ya fueron manejados arriba)
        if not (message.startswith('O:') or message.startswith('E:')):
            # Los mensajes M pueden venir sin brackets, procesarlos directamente
            if message.startswith('M:'):
                # Es un mensaje de market data individual, no lo procesamos en order listener
                pass

    async def close(self):
        """Cierra la conexión WebSocket de forma limpia"""
        print(f"[MatrizOrderListener] close() - Cerrando conexión...")
        if self.websocket:
            try:
                await self.websocket.close()
                print(f"[MatrizOrderListener] WebSocket cerrado")
            except Exception as e:
                print(f"[MatrizOrderListener] Error cerrando WebSocket: {e}")
        self.receiving = False


