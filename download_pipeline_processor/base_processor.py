from abc import ABC, abstractmethod
from typing import Any
from .file_data import FileData


class BaseProcessor(ABC):
    """
    Abstract base class for processing files.
    Users must inherit from this class and implement the `process` method.
    """

    @abstractmethod
    def process(self, file_data: FileData) -> Any:
        """
        Process the given file_data.

        :param file_data: FileData object containing file information
        :return: Result of the processing
        """
        pass
