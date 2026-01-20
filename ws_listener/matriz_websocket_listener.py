import asyncio
from abc import ABC, abstractmethod
from asyncio import CancelledError
from datetime import datetime
from service.matriz_session import MatrizSession


class MatrizWebsocketListener(ABC):

    def __init__(self, message_processor, session: MatrizSession = None):
        super().__init__()
        self.uri = None
        self.websocket = None
        self.listen_task = None
        self.session = session if session else MatrizSession()
        self.status_message = 'Initialized'
        self.is_handling_order_book_message = False
        self.message_processor = message_processor
        self.order_event_connected = False
        self.message_count = 1
        self.last_message_time = datetime.now()

    @abstractmethod
    async def run(self):
        pass

    @abstractmethod
    async def connect(self):
        pass

    async def send_message(self, message):
        if not self.websocket:
            print(f"connect en send message: {message}")
            await self.connect()
        try:
            await self.websocket.send(message)
            return message
        except:
            print(f"connect en except de send message: {message}")
            await self.connect()
            message = await self.websocket.send(message)
            return message

    async def change_status(self, status):
        self.status_message = status
        await self.message_processor.update_status(status)

    async def receive_message(self):
        while True:
            try:
                message = await self.websocket.recv()
                return message
            except CancelledError as c:
                print("Tarea cancelada")
                break
            except BaseException as e:
                print("connect en except receive message")
                await self.change_status('Disconnected')
                if self.websocket:
                    await self.websocket.close()
                    await asyncio.sleep(5)  # Espera antes de intentar reconectar
                try:
                    await self.connect()  # Método para reconectar el websocket
                except Exception as reconnection_error:
                    print(f"Error al reconectar: {reconnection_error}")
                    break

    async def listen(self):
        try:
            while self.status_message == "Running":
                message = await self.receive_message()
                if message:
                    await self.process_message(message)
                    self.message_count += 1
                    if (self.last_message_time - datetime.now()).total_seconds() > 3:
                        print("Take more than 3 seconds in receiving message")
                    self.last_message_time = datetime.now()
                else:
                    print('No message')
        except Exception as e:
            print(f"An unexpected error occurred: {e}. Retrying in 5 seconds...")
            pass


    @abstractmethod
    async def process_message(self, message):
        pass

