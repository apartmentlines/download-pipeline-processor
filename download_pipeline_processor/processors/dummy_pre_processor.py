from .base_pre_processor import BasePreProcessor
from download_pipeline_processor.file_data import FileData


class DummyPreProcessor(BasePreProcessor):
    def pre_process(self, file_data: FileData) -> FileData:
        self.log.debug(f"Pre-processing file: {file_data.name}")
        return file_data
