from typing import Any

from .base_post_processor import BasePostProcessor
from download_pipeline_processor.file_data import FileData


class DummyPostProcessor(BasePostProcessor):
    """Dummy post-processor that simulates post-processing."""

    def post_process(self, result: Any, file_data: FileData) -> None:
        self.log.debug(f"Post-processing result for {file_data.name}: {result}")
