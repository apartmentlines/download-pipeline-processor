import logging
from typing import Any


class DummyPostProcessor:
    """Dummy post-processor that simulates post-processing."""

    def post_process(self, result: Any) -> None:
        logging.debug(f"Skipping post-processing on result: {result}")
