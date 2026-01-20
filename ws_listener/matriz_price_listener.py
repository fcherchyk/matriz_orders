import asyncio
import websockets
from websockets.exceptions import ConnectionClosedError

from ws_listener.matriz_websocket_listener import MatrizWebsocketListener
from ws_listener.message_processor.logger_message_processor import LoggerMessageProcessor


class MatrizPriceListener(MatrizWebsocketListener):

    def __init__(self, message_processor, session=None, ping_interval: int = 10, ping_timeout: int = 8):
        print(f"[MatrizPriceListener] init - session provided: {session is not None}")
        super().__init__(message_processor, session)
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout

    async def run(self):
        print(f"Running: {self.__class__.__name__}")
        await self.connect()
        await self.listen()

    async def connect(self):
        print("CONNECTING WEBSOCKET")
        self.message_count = 0
        self.uri = f"wss://{self.session.WS_HOST}/ws?session_id={self.session.session_id}&conn_id={self.session.conn_id}"

        self.websocket = await websockets.connect(
            self.uri,
            origin=self.session.BASE_URL,
            ping_interval=self.ping_interval,
            ping_timeout=self.ping_timeout,
        )

        print("MatrizPriceListener connected")
        for i in range(3):  # 3 initial messages
            await self.websocket.recv()
        book_instruments = []
        md_instruments = []
        for term in ['CI', '24hs']:
            for ticker in self.message_processor.tickers:
                book_instruments.append(f'"book.bm_MERV_{ticker}_{term}"')
                md_instruments.append(f'"md.bm_MERV_{ticker}_{term}"')

        # Agregar suscripción al instrumento de pesos de caución
        if self.session.min_caucion_days is not None:
            md_instruments.append(f'"md.bm_MERV_PESOS_{self.session.min_caucion_days}D"')
            print(f"[MatrizPriceListener] Subscribing to caucion instrument: md.bm_MERV_PESOS_{self.session.min_caucion_days}D")

        bm = f"""{{"_req": "S", "topicType": "book", "topics": [{", ".join(book_instruments)}], "replace": false}}"""
        md = f"""{{"_req":"S","topicType":"md","topics":[{", ".join(md_instruments)}],"replace":false}}"""
        print(f"[MatrizPriceListener] MD message to send: {md}")


        # await self.send_message(book_message)
        await self.send_message(md)
        await self.send_message(bm)
        await self.change_status("Running")

    async def process_message(self, message):
        for m in message.strip('[]').replace('"', '').split(','):
            if m and m[0] in ['M', 'B']:
                await self.message_processor.execute(m)

