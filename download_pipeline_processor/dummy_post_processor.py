from typing import Any
from .base_post_processor import BasePostProcessor


class DummyPostProcessor(BasePostProcessor):
    """Dummy post-processor that simulates post-processing."""

    def post_process(self, result: Any) -> None:
        self.log.debug(f"Post-processing result: {result}")
