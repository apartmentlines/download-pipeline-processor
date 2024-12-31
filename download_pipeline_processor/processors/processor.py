from abc import ABC
from download_pipeline_processor.logger import Logger


class Processor(ABC):
    """
    Abstract base class for all processors.
    """

    def __init__(self, debug: bool = False) -> None:
        """
        Initialize the post-processor with logging.

        :param debug: Enable debug logging
        """
        self.debug = debug
        self.log = Logger(self.__class__.__name__, debug=self.debug)
