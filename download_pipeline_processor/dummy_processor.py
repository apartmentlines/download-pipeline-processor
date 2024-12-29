import random
import time
from .file_data import FileData
from .base_processor import BaseProcessor
from .constants import SIMULATE_MIN_PROCESS_TIME, SIMULATE_MAX_PROCESS_TIME


class DummyProcessor(BaseProcessor):
    """Dummy processor that simulates file processing."""

    def process(self, file_data: FileData) -> str:
        time.sleep(random.uniform(SIMULATE_MIN_PROCESS_TIME, SIMULATE_MAX_PROCESS_TIME))
        return f"Dummy processed file {file_data.name}"
