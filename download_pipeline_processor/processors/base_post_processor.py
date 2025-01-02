from abc import abstractmethod
from typing import Any

from .processor import Processor
from download_pipeline_processor.file_data import FileData


class BasePostProcessor(Processor):
    """
    Abstract base class for post-processing results.
    Users must inherit from this class and implement the `post_process` method.
    """

    @abstractmethod
    def post_process(self, result: Any, file_data: FileData) -> None:
        """
        Post-process the given result.

        :param result: Result returned from the processor
        """
        pass
