import os
import random
import time
from .file_data import FileData
from .base_processor import BaseProcessor
from .constants import (
    SIMULATE_MIN_PROCESS_TIME,
    SIMULATE_MAX_PROCESS_TIME,
    NO_SLEEP_ENV_VAR,
)


class DummyProcessor(BaseProcessor):
    """Dummy processor that simulates file processing."""

    def process(self, file_data: FileData) -> str:
        self.log.debug(f"Processing file: {file_data.name}")
        if not os.getenv(NO_SLEEP_ENV_VAR):
            time.sleep(
                random.uniform(SIMULATE_MIN_PROCESS_TIME, SIMULATE_MAX_PROCESS_TIME)
            )
        result = f"Dummy processed file {file_data.name}"
        self.log.debug(f"Processing complete: {result}")
        return result
