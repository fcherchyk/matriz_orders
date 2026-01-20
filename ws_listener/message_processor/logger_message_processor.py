from datetime import datetime, timedelta
from pathlib import Path

from ws_listener.message_processor.base_message_processor import BaseMessageProcessor


class LoggerMessageProcessor(BaseMessageProcessor):
    """
    Message processor simple que solo loguea mensajes a archivo.
    """

    def __init__(self):
        super().__init__()
        self.caution_days = None  # populated in listener
        self.ref_data = {}  # populated in listener
        self.operations = []

        # Logging a archivo
        self.log_dir = Path('logs/ws')
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.current_file = None
        self.current_file_start_time = datetime.now()
        self._open_new_file()

    def _generate_filename(self) -> str:
        return self.current_file_start_time.strftime('%m-%d-%H:%M:%S.txt')

    def _open_new_file(self):
        if self.current_file:
            self.current_file.close()
        self.current_file_start_time = datetime.now()
        filename = self.log_dir / self._generate_filename()
        self.current_file = open(filename, 'a')
        print(f"[LoggerMessageProcessor] New log file: {filename}")

    async def process_message(self, key: str, message: str):
        """Procesa un mensaje: lo guarda en archivo"""
        # Rotar archivo cada hora
        if datetime.now() - self.current_file_start_time >= timedelta(hours=1):
            self._open_new_file()

        # Escribir a archivo
        relative_time = datetime.now() - self.current_file_start_time
        print(f'{relative_time} {message}')
        print(f'{relative_time} {message}', file=self.current_file)
        self.current_file.flush()

    def __del__(self):
        if self.current_file:
            self.current_file.close()
