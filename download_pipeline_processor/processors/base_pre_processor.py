from abc import abstractmethod
from .processor import Processor
from download_pipeline_processor.file_data import FileData


class BasePreProcessor(Processor):
    @abstractmethod
    def pre_process(self, file_data: FileData) -> FileData:
        pass
