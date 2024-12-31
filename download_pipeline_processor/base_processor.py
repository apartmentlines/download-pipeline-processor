from abc import ABC, abstractmethod
from typing import Any
from .file_data import FileData
from .logger import Logger


class BaseProcessor(ABC):
    """
    Abstract base class for processing files.
    Users must inherit from this class and implement the `process` method.
    """

    def __init__(self, debug: bool = False) -> None:
        """
        Initialize the processor with logging.

        :param debug: Enable debug logging
        """
        self.debug = debug
        self.log = Logger(self.__class__.__name__, debug=self.debug)

    @abstractmethod
    def process(self, file_data: FileData) -> Any:
        """
        Process the given file_data.

        :param file_data: FileData object containing file information
        :return: Result of the processing
        """
        pass
