from typing import Any

from .base_post_processor import BasePostProcessor
from download_pipeline_processor.file_data import FileData


class DummyPostProcessor(BasePostProcessor):
    """Dummy post-processor that simulates post-processing."""

    def post_process(self, result: Any, file_data: FileData) -> None:
        if file_data.has_error:
            self.log.warning(
                f"File {file_data.name} has error from {getattr(file_data.error, 'stage', 'unknown')}: {getattr(file_data.error, 'error', 'unknown error')}"
            )
            return

        self.log.debug(f"Post-processing result for {file_data.name}: {result}")
