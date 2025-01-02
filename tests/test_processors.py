"""Test processor implementations for pipeline testing."""

from pathlib import Path
from typing import Any

from download_pipeline_processor.processors.base_processor import BaseProcessor
from download_pipeline_processor.processors.base_post_processor import BasePostProcessor
from download_pipeline_processor.file_data import FileData

from .test_constants import PROCESSOR_OUTPUT_DIR, POST_PROCESSOR_OUTPUT_DIR


class FileWritingProcessor(BaseProcessor):
    """Test processor that writes files to track processing."""

    def __init__(self, debug: bool = False) -> None:
        super().__init__(debug=debug)
        self.output_dir = PROCESSOR_OUTPUT_DIR

    def process(self, file_data: FileData) -> str:
        """Write a file to mark processing and return the path."""
        output_path = self.output_dir / f"processed_{file_data.name}"
        output_path.write_text(f"Processed {file_data.name}")
        return str(output_path)


class FileWritingPostProcessor(BasePostProcessor):
    """Test post-processor that writes files to track post-processing."""

    def __init__(self, debug: bool = False) -> None:
        """Initialize with output directory from test constants."""
        super().__init__(debug=debug)
        self.output_dir = POST_PROCESSOR_OUTPUT_DIR

    def post_process(self, result: str, file_data: FileData) -> None:
        """Write a file to mark post-processing."""
        input_path = Path(result)
        output_path = self.output_dir / input_path.name
        output_path.write_text(
            f"Post-processed {file_data.name}: {input_path.read_text()}"
        )


class ErrorProcessor(BaseProcessor):
    """Test processor that raises an exception."""

    def process(self, file_data: FileData) -> None:
        """Always raise an exception."""
        raise ValueError(f"Simulated error processing {file_data.name}")


class ErrorPostProcessor(BasePostProcessor):
    """Test post-processor that raises an exception."""

    def post_process(self, result: Any) -> None:
        """Always raise an exception."""
        raise ValueError(f"Simulated error post-processing {result}")
