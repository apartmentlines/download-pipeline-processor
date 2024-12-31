from abc import ABC, abstractmethod
from typing import Any
from .logger import Logger


class BasePostProcessor(ABC):
    """
    Abstract base class for post-processing results.
    Users must inherit from this class and implement the `post_process` method.
    """

    def __init__(self, debug: bool = False) -> None:
        """
        Initialize the post-processor with logging.

        :param debug: Enable debug logging
        """
        self.debug = debug
        self.log = Logger(self.__class__.__name__, debug=self.debug)

    @abstractmethod
    def post_process(self, result: Any) -> None:
        """
        Post-process the given result.

        :param result: Result returned from the processor
        """
        pass
