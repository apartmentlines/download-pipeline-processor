import logging
from typing import Any
from .base_post_processor import BasePostProcessor


class DummyPostProcessor(BasePostProcessor):
    """Dummy post-processor that simulates post-processing."""

    def post_process(self, result: Any) -> None:
        logging.debug(f"Skipping post-processing on result: {result}")
