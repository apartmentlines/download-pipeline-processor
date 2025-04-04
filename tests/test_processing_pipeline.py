"""Tests for the ProcessingPipeline class."""

import logging
import os
import pytest
import requests
import subprocess
import threading
from typing import Any
from download_pipeline_processor.processors.base_processor import BaseProcessor
from download_pipeline_processor.processors.base_post_processor import BasePostProcessor
from download_pipeline_processor.processors.base_pre_processor import BasePreProcessor
from download_pipeline_processor.processors.dummy_pre_processor import DummyPreProcessor
from download_pipeline_processor.file_data import FileDataError
from .test_utils import MockRequestUtils
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
from download_pipeline_processor.file_data import FileData
from download_pipeline_processor.processing_pipeline import ProcessingPipeline
from download_pipeline_processor.constants import (
    DEFAULT_MAX_RETRIES,
    DEFAULT_PROCESSING_LIMIT,
    DEFAULT_DOWNLOAD_QUEUE_SIZE,
    NO_SLEEP_ENV_VAR,
)
from tests.test_constants import PROCESSOR_OUTPUT_DIR, POST_PROCESSOR_OUTPUT_DIR
from .test_processors import (
    FileWritingProcessor,
    FileWritingPostProcessor,
    ErrorProcessor,
    ErrorPostProcessor,
)


class TestDownloadRetries:
    """Test that the download function retries on failure."""

    @patch("time.sleep")
    def test_download_retries_on_failure(self, _, monkeypatch):
        """Test that download_file retries the correct number of times upon failure."""
        from download_pipeline_processor import constants

        max_retries = 3  # Set the desired number of retries
        monkeypatch.setattr(constants, "DEFAULT_MAX_RETRIES", max_retries)

        # Create a counter to track the number of attempts
        attempts = []

        def mock_requests_get(*args, **kwargs):
            del args, kwargs
            attempts.append(1)
            raise requests.exceptions.ConnectionError("Simulated connection error")

        # Mock requests.get to always raise a ConnectionError
        with patch("requests.get", side_effect=mock_requests_get):
            pipeline = ProcessingPipeline(simulate_downloads=False)
            file_data = FileData(
                id="1", name="test1.txt", url="https://example.com/test1.txt"
            )

            # Run the download_file method
            with pytest.raises(requests.exceptions.ConnectionError):
                pipeline.download_file(file_data)

            # Check that requests.get was called the correct number of times
            assert len(attempts) == max_retries

            # Verify that file_data.local_path is None (since download failed)
            assert file_data.local_path is None


class TestDownloadTimeoutHandling:
    """Test that download timeouts are handled correctly."""

    @patch("time.sleep")
    def test_download_timeout_handling(self, _):
        """Test that download_file retries upon timeouts and handles them correctly."""

        # Create a counter to track the number of attempts
        attempts = []

        def mock_requests_get(*args, **kwargs):
            del args, kwargs
            attempts.append(1)
            raise requests.exceptions.Timeout("Simulated timeout error")

        # Mock requests.get to always raise a Timeout error
        with patch("requests.get", side_effect=mock_requests_get):
            pipeline = ProcessingPipeline(simulate_downloads=False)
            file_data = FileData(
                id="1", name="test1.txt", url="https://example.com/test1.txt"
            )

            # Run the download_file method
            with pytest.raises(requests.exceptions.Timeout):
                pipeline.download_file(file_data)

            # Check that requests.get was called the correct number of times
            assert len(attempts) == DEFAULT_MAX_RETRIES

            # Verify that file_data.local_path is None (since download failed)
            assert file_data.local_path is None


class TestTemporaryFileCleanup:
    """Test that temporary files are cleaned up after download failures."""

    @patch("time.sleep")
    def test_temporary_file_cleanup_on_download_failure(self, _, tmp_path, monkeypatch):
        """Ensure that temporary files are deleted if download fails."""
        from download_pipeline_processor import constants

        max_retries = 1  # Set retries to 1 for faster test
        monkeypatch.setattr(constants, "DEFAULT_MAX_RETRIES", max_retries)

        temp_file_paths = []

        # We'll wrap create_cached_download_filepath to capture temp_path
        original_create_cached_download_filepath = (
            ProcessingPipeline.create_cached_download_filepath
        )

        def create_cached_download_filepath(self, file_data):
            temp_path = original_create_cached_download_filepath(self, file_data)
            temp_file_paths.append(temp_path)
            return temp_path

        def mock_requests_get(*args, **kwargs):
            del args, kwargs
            raise requests.exceptions.ConnectionError("Simulated connection error")

        # Mock requests.get to always raise a ConnectionError
        with (
            patch("requests.get", side_effect=mock_requests_get),
            patch.object(
                ProcessingPipeline,
                "create_cached_download_filepath",
                create_cached_download_filepath,
            ),
        ):

            pipeline = ProcessingPipeline(
                simulate_downloads=False, download_cache=tmp_path
            )
            file_data = FileData(
                id="1", name="test1.txt", url="https://example.com/test1.txt"
            )

            # Run the download_file method
            with pytest.raises(requests.exceptions.ConnectionError):
                pipeline.download_file(file_data)

            # Verify that the temporary file was cleaned up
            for temp_path in temp_file_paths:
                assert (
                    not temp_path.exists()
                ), f"Temporary file {temp_path} was not deleted."

    """Test the initialization of worker threads and processors."""


class TestWorkerInitialization:

    @patch("time.sleep")
    def test_initialize_worker(self, _):
        """Test that each worker thread initializes its own processor."""
        from threading import current_thread

        # Tracking variables
        initialization_count = 0
        thread_processor_ids = {}

        class TrackingProcessor(BaseProcessor):
            def __init__(self, debug: bool = False) -> None:
                nonlocal initialization_count
                super().__init__(debug)
                initialization_count += 1
                # Record the processor instance ID for the current thread
                thread_id = current_thread().ident
                thread_processor_ids[thread_id] = id(self)

            def process(self, file_data: FileData) -> Any:
                return f"Processed {file_data.name}"

        processing_limit = 3  # Use multiple worker threads

        pipeline = ProcessingPipeline(
            processor_class=TrackingProcessor,
            simulate_downloads=True,
            processing_limit=processing_limit,
        )

        # Create test data with enough files to require multiple threads
        test_files = [
            {
                "id": str(i),
                "name": f"test{i}.txt",
                "url": f"https://example.com/test{i}.txt",
            }
            for i in range(6)
        ]

        pipeline.run(test_files)

        # Assert that _initialize_worker was called once per worker thread
        assert initialization_count == processing_limit

        # Assert that each thread has its own processor instance
        assert len(thread_processor_ids) == processing_limit
        assert len(set(thread_processor_ids.values())) == processing_limit


class TestProcessorReuseWithinThread:
    """Test that the same processor instance is reused within a thread."""

    @patch("time.sleep")
    def test_processor_reuse_within_thread(self, _):
        from threading import current_thread

        # Tracking variables
        processor_instances = {}

        class TrackingProcessor(BaseProcessor):
            def __init__(self, debug: bool = False) -> None:
                super().__init__(debug)
                self.process_calls = 0
                # Record the processor instance for the thread
                thread_id = current_thread().ident
                processor_instances.setdefault(thread_id, self)

            def process(self, file_data: FileData) -> Any:
                self.process_calls += 1
                return f"Processed {file_data.name}"

        processing_limit = 2  # Use multiple worker threads

        pipeline = ProcessingPipeline(
            processor_class=TrackingProcessor,
            simulate_downloads=True,
            processing_limit=processing_limit,
        )

        # Create test data
        test_files = [
            {
                "id": str(i),
                "name": f"test{i}.txt",
                "url": f"https://example.com/test{i}.txt",
            }
            for i in range(4)
        ]

        pipeline.run(test_files)

        # Verify that each processor instance processed multiple files
        for processor in set(processor_instances.values()):
            assert processor.process_calls > 0

        # Verify total process call count matches number of files
        total_process_calls = sum(
            p.process_calls for p in set(processor_instances.values())
        )
        assert total_process_calls == len(test_files)


class TestProcessorDebugFlag:
    """Test that processors are initialized with the correct debug flag."""

    @patch("time.sleep")
    def test_processor_debug_flag(self, _):
        """Test that processors receive the correct debug flag upon initialization."""
        # Tracking variable
        debug_flags = []

        class DebugFlagProcessor(BaseProcessor):
            def __init__(self, debug: bool = False) -> None:
                super().__init__(debug)
                debug_flags.append(debug)

            def process(self, file_data: FileData) -> Any:
                return f"Processed {file_data.name}"

        pipeline = ProcessingPipeline(
            processor_class=DebugFlagProcessor,
            simulate_downloads=True,
            debug=True,
            processing_limit=2,
        )

        test_files = [
            {"id": "1", "name": "test1.txt", "url": "https://example.com/test1.txt"}
        ]

        pipeline.run(test_files)

        # Verify that all processor instances have debug=True
        assert all(flag is True for flag in debug_flags)


class TestThreadPoolExecutorInitialization:
    """Test ThreadPoolExecutor initialization in the pipeline."""

    @patch("time.sleep")
    def test_thread_pool_executor_initialization(self, _):
        """Test that ThreadPoolExecutor is initialized with the correct parameters."""

        # We'll create a subclass of ThreadPoolExecutor to capture initialization parameters
        class CustomThreadPoolExecutor(ThreadPoolExecutor):
            def __init__(self, *args, **kwargs):
                self.captured_args = args
                self.captured_kwargs = kwargs
                super().__init__(*args, **kwargs)

        # Patch the ProcessingPipeline to use CustomThreadPoolExecutor
        original_run_method = ProcessingPipeline.run

        def run_with_custom_executor(self, input_data):
            del input_data
            with CustomThreadPoolExecutor(
                max_workers=self.processing_limit,
                thread_name_prefix="ProcessingWorker",
                initializer=self._initialize_worker,
            ) as executor:
                self.executor = executor
                return 0

        try:
            ProcessingPipeline.run = run_with_custom_executor

            pipeline = ProcessingPipeline(
                simulate_downloads=True,
                processing_limit=2,
            )

            pipeline.run([])

            # Retrieve the captured executor parameters
            executor: CustomThreadPoolExecutor = (
                pipeline.executor
            )  # pyright: ignore[reportAssignmentType]
            assert (
                executor.captured_kwargs.get("initializer")
                == pipeline._initialize_worker
            )
            assert (
                executor.captured_kwargs.get("thread_name_prefix") == "ProcessingWorker"
            )
        finally:
            # Restore the original run method and ThreadPoolExecutor
            ProcessingPipeline.run = original_run_method

    """Test error handling in different pipeline stages."""


class TestPipelineErrorHandling:

    @patch("time.sleep")
    def test_download_error_handling(self, _, pipeline):
        """Test error handling during download stage."""
        test_files = [
            {"url": "https://invalid-url.com/nonexistent.txt", "name": "test.txt"}
        ]
        pipeline.simulate_downloads = False

        # Mock requests to simulate download error
        with patch("requests.get") as mock_get:
            mock_get.side_effect = requests.exceptions.ConnectionError(
                "Download failed"
            )
            result = pipeline.run(test_files)

        # Pipeline should complete despite error
        assert result == 0

        # Verify error was captured and passed to post-processing
        post_processed = list(POST_PROCESSOR_OUTPUT_DIR.iterdir())
        assert len(post_processed) == 0  # No successful processing output

    @patch("time.sleep")
    def test_pre_processor_error_handling(self, _, pipeline):
        """Test error handling during pre-processing stage."""

        class ErrorPreProcessor(BasePreProcessor):
            def pre_process(self, file_data: FileData) -> FileData:
                del file_data
                raise ValueError("Pre-processing error")

        test_files = [{"url": "https://example.com/test.txt", "name": "test.txt"}]
        pipeline.pre_processor_class = ErrorPreProcessor

        result = pipeline.run(test_files)
        assert result == 0

        # Verify error was captured
        assert len(list(PROCESSOR_OUTPUT_DIR.iterdir())) == 0

    @patch("time.sleep")
    def test_processor_error_handling(self, _, pipeline):
        """Test error handling during processing stage."""

        class ErrorProcessor(BaseProcessor):
            def process(self, file_data: FileData) -> None:
                del file_data
                raise ValueError("Processing error")

        test_files = [{"url": "https://example.com/test.txt", "name": "test.txt"}]
        pipeline.processor_class = ErrorProcessor

        result = pipeline.run(test_files)
        assert result == 0

        # Verify error was captured
        assert len(list(PROCESSOR_OUTPUT_DIR.iterdir())) == 0

    @patch("time.sleep")
    def test_error_logging(self, _, capsys, pipeline):
        """Test that errors are properly logged."""

        class ErrorProcessor(BaseProcessor):
            def process(self, file_data: FileData) -> None:
                del file_data
                raise ValueError("Test error")

        test_files = [{"url": "https://example.com/test.txt", "name": "test.txt"}]
        pipeline.processor_class = ErrorProcessor

        pipeline.run(test_files)

        # Verify error was logged to stderr
        captured = capsys.readouterr()
        assert "Test error" in captured.err
        assert "Error processing test.txt" in captured.err

    @patch("time.sleep")
    def test_continue_after_error(self, _, pipeline):
        """Test pipeline continues processing after errors."""

        class PartialErrorProcessor(FileWritingProcessor):
            def process(self, file_data: FileData) -> str:
                if file_data.name == "error.txt":
                    raise ValueError("Simulated error")
                return super().process(file_data)

        test_files = [
            {"url": "https://example.com/test1.txt", "name": "test1.txt"},
            {"url": "https://example.com/error.txt", "name": "error.txt"},
            {"url": "https://example.com/test2.txt", "name": "test2.txt"},
        ]
        pipeline.processor_class = PartialErrorProcessor

        result = pipeline.run(test_files)
        assert result == 0

        # Verify successful files were processed
        processed_files = [f.name for f in PROCESSOR_OUTPUT_DIR.iterdir()]
        assert len(processed_files) == 2
        assert "processed_test1.txt" in processed_files
        assert "processed_test2.txt" in processed_files

    @patch("time.sleep")
    def test_post_processor_receives_errors(self, _, pipeline):
        """Test that post-processor correctly receives error information."""
        errors_received = []

        class ErrorTrackingPostProcessor(BasePostProcessor):
            def post_process(self, result: Any, file_data: FileData) -> None:
                del result
                if file_data.has_error:
                    errors_received.append(
                        (
                            getattr(file_data.error, "stage", "unknown"),
                            str(getattr(file_data.error, "error", "unknown error")),
                        )
                    )

        class ErrorProcessor(BaseProcessor):
            def process(self, file_data: FileData) -> None:
                del file_data
                raise ValueError("Test error")

        test_files = [{"url": "https://example.com/test.txt", "name": "test.txt"}]
        pipeline.processor_class = ErrorProcessor
        pipeline.post_processor_class = ErrorTrackingPostProcessor

        result = pipeline.run(test_files)
        assert result == 0

        # Verify error was received by post-processor
        assert len(errors_received) == 1
        assert errors_received[0][0] == "process"
        assert "Test error" in errors_received[0][1]


class TestEdgeCasesAndErrorHandling:
    """Test edge cases and error handling in the pipeline."""

    @patch("time.sleep")
    def test_empty_file_list(self, _, pipeline, tmp_path):
        """Test pipeline handles empty file list correctly."""
        # Test with empty list
        result = pipeline.run([])
        assert result == 0

        # Test with empty JSON file
        empty_json_file = tmp_path / "empty.json"
        empty_json_file.write_text("[]")
        result = pipeline.run(empty_json_file)
        assert result == 0

    @patch("time.sleep")
    def test_invalid_urls(self, _, pipeline, monkeypatch):
        """Test pipeline handles invalid URLs appropriately."""
        test_file = FileData(
            id="1", name="test1.txt", url="https://invalid-url.com/test1.txt"
        )

        # Mock requests.get to simulate a connection error
        def mock_failed_request(*args, **kwargs):
            del args, kwargs
            raise requests.exceptions.ConnectionError("Simulated connection error")

        monkeypatch.setattr(requests, "get", mock_failed_request)

        # Test direct download handling
        pipeline.simulate_downloads = False

        # Attempt to download - should handle error gracefully
        with pytest.raises(requests.exceptions.ConnectionError):
            pipeline.download_file(test_file)

        # Verify no local file was created
        assert test_file.local_path is None

    @patch("time.sleep")
    def test_exception_handling(self, _, pipeline):
        """Test pipeline handles various exceptions without crashing."""
        test_files = [
            {"id": "1", "name": "test1.txt", "url": "https://example.com/test1.txt"}
        ]

        # Test download exception
        def mock_download():
            raise Exception("Simulated download error")

        # Test processing exception
        def mock_process():
            raise Exception("Simulated processing error")

        # Test post-processing exception
        def mock_post_process():
            raise Exception("Simulated post-processing error")

        # Run with each type of error
        for mock_func, target in [
            (mock_download, "download_file"),
            (mock_process, "process"),
            (mock_post_process, "post_process"),
        ]:
            with pytest.MonkeyPatch().context() as m:
                if target == "download_file":
                    m.setattr(pipeline, target, mock_func)
                elif target == "process":
                    m.setattr(pipeline.processor_class, target, mock_func)
                else:
                    m.setattr(pipeline.post_processor_class, target, mock_func)

                result = pipeline.run(test_files)
                assert result == 0  # Pipeline should complete despite errors


class TestPipelineExecutionFlow:
    """Test the complete execution flow of the pipeline."""

    @patch("time.sleep")
    def test_end_to_end_execution(self, _, pipeline):
        """Test complete pipeline execution with file artifacts."""
        # Track pre-processing calls
        pre_processed_files = set()

        class TrackingPreProcessor(DummyPreProcessor):
            def pre_process(self, file_data: FileData) -> FileData:
                pre_processed_files.add(file_data.name)
                return super().pre_process(file_data)

        # Create test data
        test_files = [
            {"id": "1", "name": "test1.txt", "url": "https://example.com/test1.txt"},
            {"id": "2", "name": "test2.txt", "url": "https://example.com/test2.txt"},
        ]

        # Create pipeline with tracking pre-processor
        pipeline = ProcessingPipeline(
            pre_processor_class=TrackingPreProcessor,
            processor_class=FileWritingProcessor,
            post_processor_class=FileWritingPostProcessor,
            simulate_downloads=True,
        )

        # Run pipeline
        result = pipeline.run(test_files)

        # Verify pipeline completed successfully
        assert result == 0

        # Verify all files went through pre-processing
        assert pre_processed_files == {"test1.txt", "test2.txt"}

        # Check for processing artifacts
        assert (PROCESSOR_OUTPUT_DIR / "processed_test1.txt").exists()
        assert (PROCESSOR_OUTPUT_DIR / "processed_test2.txt").exists()

        # Check for post-processing artifacts
        assert (POST_PROCESSOR_OUTPUT_DIR / "processed_test1.txt").exists()
        assert (POST_PROCESSOR_OUTPUT_DIR / "processed_test2.txt").exists()

        # Verify content of artifacts
        assert (
            "Processed test1.txt"
            in (PROCESSOR_OUTPUT_DIR / "processed_test1.txt").read_text()
        )
        assert (
            "Processed test2.txt"
            in (PROCESSOR_OUTPUT_DIR / "processed_test2.txt").read_text()
        )
        assert (
            "Processed test1.txt"
            in (POST_PROCESSOR_OUTPUT_DIR / "processed_test1.txt").read_text()
        )
        assert (
            "Processed test2.txt"
            in (POST_PROCESSOR_OUTPUT_DIR / "processed_test2.txt").read_text()
        )

    @patch("time.sleep")
    def test_graceful_shutdown(self, _, pipeline, monkeypatch):
        """Test that pipeline shuts down gracefully after completion."""
        test_files = [
            {"id": "1", "name": "test1.txt", "url": "https://example.com/test1.txt"}
        ]

        # Mock requests.get for successful downloads
        monkeypatch.setattr(requests, "get", MockRequestUtils.mock_successful_request)
        pipeline.simulate_downloads = False

        # Run pipeline
        result = pipeline.run(test_files)

        # Verify shutdown
        assert result == 0
        assert pipeline.shutdown_event.is_set()
        assert pipeline.download_queue.empty()
        assert pipeline.downloaded_queue.empty()
        assert pipeline.post_processing_queue.empty()


class TestPipelineInitialization:
    """Test ProcessingPipeline initialization."""

    def test_logging_configuration(self):
        # Test processor logging
        processor = FileWritingProcessor(debug=True)
        assert processor.log.level == logging.DEBUG

        # Test post-processor logging
        post_processor = FileWritingPostProcessor(debug=True)
        assert post_processor.log.level == logging.DEBUG

        # Test pipeline logging with file
        pipeline = ProcessingPipeline(
            processor_class=FileWritingProcessor,
            post_processor_class=FileWritingPostProcessor,
            debug=True,
        )
        assert pipeline.log.level == logging.DEBUG

    def test_default_initialization(self):
        """Test pipeline initializes with default parameters."""
        pipeline = ProcessingPipeline()

        assert pipeline.processing_limit == DEFAULT_PROCESSING_LIMIT
        assert pipeline.download_queue_size == DEFAULT_DOWNLOAD_QUEUE_SIZE
        assert pipeline.simulate_downloads is False
        assert pipeline.debug is False

    def test_custom_initialization(self):
        """Test pipeline initializes with custom parameters."""
        pipeline = ProcessingPipeline(
            processor_class=FileWritingProcessor,
            post_processor_class=FileWritingPostProcessor,
            processing_limit=5,
            download_queue_size=20,
            simulate_downloads=True,
            debug=True,
        )

        assert pipeline.processor_class == FileWritingProcessor
        assert pipeline.post_processor_class == FileWritingPostProcessor
        assert pipeline.processing_limit == 5
        assert pipeline.download_queue_size == 20
        assert pipeline.simulate_downloads is True
        assert pipeline.debug is True


class TestDownloadFunctionality:
    """Test download functionality of the pipeline."""

    @patch("time.sleep")
    def test_simulate_download(self, _, pipeline):
        """Test simulated download functionality."""
        file_data = FileData(
            id=123, url="https://example.com/test.txt", name="test.txt"
        )
        pipeline.simulate_download(file_data)
        assert file_data.local_path is not None
        assert file_data.local_path.name == "test.txt"

    def test_create_cached_download_filepath(self, pipeline):
        """Test creation of cached download filepath."""
        file_data = FileData(
            url="https://example.com/test.txt", id="123", name="test.txt"
        )
        path = pipeline.create_cached_download_filepath(file_data)
        assert path.parent == pipeline.download_cache
        assert path.name.startswith("123_")
        assert path.name.endswith(".tmp")

    def test_delete_cached_download_file(self, pipeline, tmp_path):
        """Test deletion of cached download file."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("test content")
        file_data = FileData(
            id=123,
            url="https://example.com/test.txt",
            name="test.txt",
            local_path=test_file,
        )

        # Test with simulate_downloads=True (should not delete)
        pipeline.simulate_downloads = True
        pipeline.delete_cached_download_file(file_data)
        assert test_file.exists()

        # Test with simulate_downloads=False (should delete)
        pipeline.simulate_downloads = False
        pipeline.delete_cached_download_file(file_data)
        assert not test_file.exists()


class TestProcessingFunctionality:
    """Test processing functionality of the pipeline."""

    def test_process_file(self, pipeline):
        """Test processing of a single file."""
        file_data = FileData(
            id=123, url="https://example.com/test.txt", name="test.txt"
        )
        # Initialize the thread-local processor
        pipeline.thread_local.processor = pipeline.processor_class(debug=pipeline.debug)

        # Now call process_file
        pipeline.process_file(file_data)

        # Retrieve the result from the post_processing_queue
        result, processed_file_data = pipeline.post_processing_queue.get_nowait()

        # Assertions to verify processing result
        assert result is not None
        assert processed_file_data == file_data
        assert not processed_file_data.has_error
        assert str(Path(result).name).startswith("processed_")
        assert str(Path(result).name).endswith(file_data.name)

    def test_process_file_with_error(self):
        """Test processing with error processor."""
        pipeline = ProcessingPipeline(
            processor_class=ErrorProcessor, simulate_downloads=True
        )
        file_data = FileData(
            id=123, url="https://example.com/test.txt", name="test.txt"
        )
        pipeline.process_file(file_data)

        # Initialize the thread-local processor with ErrorProcessor
        pipeline.thread_local.processor = pipeline.processor_class(debug=pipeline.debug)

        # Now call process_file
        pipeline.process_file(file_data)

        # Retrieve the result from the post_processing_queue
        result, error_file_data = pipeline.post_processing_queue.get_nowait()

        # Assertions to verify error handling
        assert result is None
        assert error_file_data == file_data
        assert error_file_data.has_error
        assert isinstance(error_file_data.error.error, ValueError)
        assert "Simulated error processing" in str(error_file_data.error.error)


class TestPreProcessingFunctionality:
    """Test pre-processing functionality of the pipeline."""

    def test_pre_processor_handles_file(self, pipeline):
        """Test pre-processor processes files correctly."""
        file_data = FileData(
            id=123, url="https://example.com/test.txt", name="test.txt"
        )
        pre_processor = pipeline.pre_processor_class(debug=True)
        processed_data = pre_processor.pre_process(file_data)
        assert processed_data == file_data  # For DummyPreProcessor

    @patch("time.sleep")
    def test_pre_processing_queue_behavior(self, _, pipeline):
        """Test pre-processing queue size limits are respected."""
        # Create test data with more files than queue size
        test_files = [
            {
                "id": str(i),
                "name": f"test{i}.txt",
                "url": f"https://example.com/test{i}.txt",
            }
            for i in range(pipeline.pre_processing_queue_size + 5)
        ]

        # Run pipeline
        result = pipeline.run(test_files)
        assert result == 0

        # Verify all files were processed despite queue constraints
        processed_files = list(PROCESSOR_OUTPUT_DIR.iterdir())
        assert len(processed_files) == len(test_files)

    @patch("time.sleep")
    def test_pre_processor_error_handling(self, _):
        """Test pre-processor handles errors gracefully."""

        class ErrorPreProcessor(BasePreProcessor):
            def pre_process(self, file_data: FileData) -> FileData:
                raise ValueError(f"Simulated error pre-processing {file_data.name}")

        pipeline = ProcessingPipeline(
            pre_processor_class=ErrorPreProcessor, simulate_downloads=True
        )

        test_files = [{"url": "https://example.com/test.txt", "name": "test.txt"}]
        result = pipeline.run(test_files)

        # Pipeline should complete despite pre-processing errors
        assert result == 0

    def test_pre_processing_queue_size_configuration(self):
        """Test that pre-processing queue size can be configured."""
        custom_size = 5
        pipeline = ProcessingPipeline(
            pre_processing_queue_size=custom_size, simulate_downloads=True
        )

        # Verify queue was created with correct size
        assert pipeline.pre_processed_queue.maxsize == custom_size


class TestPostProcessingFunctionality:
    """Test post-processing functionality of the pipeline."""

    def test_post_processor_handles_result(self, tmp_path):
        """Test post-processor handles results correctly."""
        # Create a test input file
        input_file = tmp_path / "processed_test.txt"
        input_file.write_text("Processed test content")

        # Run post-processor
        file_data = FileData(
            id=123, url="https://example.com/test.txt", name="test.txt"
        )
        post_processor = FileWritingPostProcessor()
        post_processor.post_process(str(input_file), file_data)

        # Verify output file exists with correct content
        output_file = POST_PROCESSOR_OUTPUT_DIR / input_file.name
        assert output_file.exists()
        assert (
            output_file.read_text() == "Post-processed test.txt: Processed test content"
        )


class TestConcurrencyAndThreading:
    """Test concurrent processing behavior of the pipeline."""

    @patch("time.sleep")
    def test_concurrent_downloads_and_processing(self, _):
        """Test that downloads and processing occur concurrently."""
        # Track pre-processing operations
        pre_processed_files = set()

        class TrackingPreProcessor(DummyPreProcessor):
            def pre_process(self, file_data: FileData) -> FileData:
                pre_processed_files.add(file_data.name)
                return super().pre_process(file_data)

        # Create test data with enough files to trigger concurrency
        test_files = [
            {
                "id": str(i),
                "name": f"test{i}.txt",
                "url": f"https://example.com/test{i}.txt",
            }
            for i in range(5)
        ]

        # Create pipeline with small delays to ensure overlap
        pipeline = ProcessingPipeline(
            pre_processor_class=TrackingPreProcessor,
            processor_class=FileWritingProcessor,
            post_processor_class=FileWritingPostProcessor,
            simulate_downloads=True,
            processing_limit=2,  # Limit concurrent processing
        )

        # Run pipeline
        result = pipeline.run(test_files)

        # Verify successful completion
        assert result == 0

        # Verify all files went through pre-processing
        assert len(pre_processed_files) == 5
        assert all(f"test{i}.txt" in pre_processed_files for i in range(5))

        # Verify all files were processed
        processed_files = list(PROCESSOR_OUTPUT_DIR.iterdir())
        assert len(processed_files) == 5

        # Verify post-processing completed
        post_processed_files = list(POST_PROCESSOR_OUTPUT_DIR.iterdir())
        assert len(post_processed_files) == 5

    @patch("time.sleep")
    def test_download_queue_full_behavior(self, _):
        """Test pipeline behavior when download queue is full."""
        # Create test data
        test_files = [
            {
                "id": str(i),
                "name": f"test{i}.txt",
                "url": f"https://example.com/test{i}.txt",
            }
            for i in range(4)
        ]

        # Create pipeline with minimal queue size to force queue full condition
        pipeline = ProcessingPipeline(
            processor_class=FileWritingProcessor,
            post_processor_class=FileWritingPostProcessor,
            simulate_downloads=True,
            download_queue_size=1,  # Minimal queue size
            processing_limit=1,  # Slow processing
        )

        # Run pipeline
        result = pipeline.run(test_files)

        # Verify successful completion despite queue constraints
        assert result == 0

        # Verify all files were processed
        processed_files = list(PROCESSOR_OUTPUT_DIR.iterdir())
        assert len(processed_files) == 4

    @patch("time.sleep")
    def test_shutdown_event_propagation(self, _):
        """Test that shutdown event stops all threads properly."""
        processing_started = threading.Event()
        processed_count = 0

        class SignalingProcessor(FileWritingProcessor):
            def process(self, file_data: FileData) -> str:
                nonlocal processed_count
                processed_count += 1
                processing_started.set()  # Signal that processing has started
                return super().process(file_data)

        # Create test data
        test_files = [
            {
                "id": str(i),
                "name": f"test{i}.txt",
                "url": f"https://example.com/test{i}.txt",
            }
            for i in range(10)
        ]

        # Create pipeline with signaling processor
        pipeline = ProcessingPipeline(
            processor_class=SignalingProcessor,
            post_processor_class=FileWritingPostProcessor,
            simulate_downloads=True,
        )

        # Start pipeline in a separate thread
        pipeline_thread = threading.Thread(target=pipeline.run, args=(test_files,))
        pipeline_thread.start()

        # Wait for processing to start
        assert processing_started.wait(timeout=1.0), "Processing never started"

        # Trigger shutdown
        pipeline.shutdown_event.set()

        # Wait for completion
        pipeline_thread.join(timeout=1.0)

        # Verify thread completed
        assert not pipeline_thread.is_alive()

        # Verify that not all files were processed (shutdown worked)
        assert processed_count < len(test_files)

    def test_post_processor_handles_error(self):
        """Test post-processor handles errors gracefully."""
        post_processor = ErrorPostProcessor()
        test_result = "Test Result"
        with pytest.raises(ValueError, match="Simulated error post-processing"):
            post_processor.post_process(
                test_result,
                FileData(id=123, name="test.txt", url="https://example.com/test.txt"),
            )


class TestCommandLineInterface:
    """Test command-line interface functionality."""

    @patch("time.sleep")
    def test_cli_default_arguments(self, _, temp_json_file):
        """Test pipeline execution via CLI with default arguments."""
        env = os.environ.copy()
        env[NO_SLEEP_ENV_VAR] = "1"
        result = subprocess.run(
            [
                "download-pipeline-processor",
                "--files",
                str(temp_json_file),
                "--simulate-downloads",
            ],
            capture_output=True,
            text=True,
            env=env,
        )
        assert result.returncode == 0
        assert "Pipeline completed successfully" in result.stderr

    def test_cli_argument_validation(self, temp_json_file):
        """Test CLI argument parsing and validation."""
        # Test missing required argument
        result = subprocess.run(
            ["python", "-m", "download_pipeline_processor.processing_pipeline"],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0
        assert "error: the following arguments are required: --files" in result.stderr

        # Test invalid processor class format
        result = subprocess.run(
            [
                "python",
                "-m",
                "download_pipeline_processor.processing_pipeline",
                "--files",
                str(temp_json_file),
                "--processor",
                "invalid_format",
                "--simulate-downloads",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0
        assert "Cannot load class 'invalid_format'" in result.stderr

        # Test invalid processing limit
        result = subprocess.run(
            [
                "python",
                "-m",
                "download_pipeline_processor.processing_pipeline",
                "--files",
                str(temp_json_file),
                "--processing-limit",
                "-1",
                "--simulate-downloads",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0
        assert (
            "argument --processing-limit: -1 is not a positive integer" in result.stderr
        )

        # Test invalid download queue size
        result = subprocess.run(
            [
                "python",
                "-m",
                "download_pipeline_processor.processing_pipeline",
                "--files",
                str(temp_json_file),
                "--download-queue-size",
                "0",
                "--simulate-downloads",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0
        assert (
            "argument --download-queue-size: 0 is not a positive integer"
            in result.stderr
        )


class TestFileDataError:
    """Test FileDataError class and error handling functionality."""

    def test_file_data_error_creation(self):
        """Test creating FileDataError objects."""
        error = ValueError("Test error")
        file_error = FileDataError(stage="test", error=error)
        assert file_error.stage == "test"
        assert file_error.error == error

    def test_file_data_error_handling(self):
        """Test error handling in FileData."""
        file_data = FileData(
            id=123, name="test.txt", url="https://example.com/test.txt"
        )
        assert not file_data.has_error
        assert file_data.error is None

        # Test adding error
        error = ValueError("Test error")
        file_data.add_error("download", error)
        assert file_data.has_error
        assert getattr(file_data.error, "stage", "unknown") == "download"
        assert getattr(file_data.error, "error", "unknown error") == error

        # Test updating error
        new_error = RuntimeError("New error")
        file_data.add_error("process", new_error)
        assert getattr(file_data.error, "stage", "unknown") == "process"
        assert getattr(file_data.error, "error", "unknown error") == new_error


class TestFileDataHandling:
    """Test FileData creation and handling in the pipeline."""

    def test_dynamic_attributes(self):
        """Test adding and accessing dynamic attributes."""
        file_data = FileData(
            id=123, name="file.txt", url="https://example.com/file.txt"
        )

        # Test adding and reading dynamic attributes
        file_data.custom_field = "value"
        assert file_data.custom_field == "value"

        # Test accessing non-existent attribute
        with pytest.raises(AttributeError):
            _ = file_data.non_existent_field

    def test_create_file_data_with_additional_fields(self, pipeline):
        """Test creation of FileData with additional fields."""
        test_dict = {
            "url": "https://example.com/test.txt",
            "id": "123",
            "name": "test.txt",
            "category": "document",
            "priority": 1,
            "tags": ["important", "urgent"],
        }

        file_data = pipeline.create_file_data(test_dict)

        # Test standard fields
        assert file_data.url == "https://example.com/test.txt"
        assert file_data.id == "123"
        assert file_data.name == "test.txt"

        # Test additional fields
        assert file_data.category == "document"
        assert file_data.priority == 1
        assert file_data.tags == ["important", "urgent"]
        assert "category" in file_data.additional_fields
        assert "priority" in file_data.additional_fields
        assert "tags" in file_data.additional_fields

    def test_create_file_data_minimal(self, pipeline):
        """Test creation of FileData with only required fields."""
        test_dict = {"url": "https://example.com/test.txt"}

        file_data = pipeline.create_file_data(test_dict)

        # Test default values
        assert file_data.url == "https://example.com/test.txt"
        assert file_data.id == "test"  # Default from URL basename
        assert file_data.name == "test"  # Default from URL basename
        assert file_data.additional_fields == {}

    def test_create_file_data_attribute_error(self, pipeline):
        """Test accessing non-existent additional field raises AttributeError."""
        test_dict = {"url": "https://example.com/test.txt", "category": "document"}

        file_data = pipeline.create_file_data(test_dict)

        # Test accessing existing additional field
        assert file_data.category == "document"

        # Test accessing non-existent field
        with pytest.raises(AttributeError) as exc_info:
            _ = file_data.nonexistent_field
        assert "'FileData' object has no attribute 'nonexistent_field'" in str(
            exc_info.value
        )

    def test_load_files_from_json_with_additional_fields(self, tmp_path):
        """Test loading files from JSON with additional fields."""
        # Create a test JSON file with additional fields
        json_content = """
        [
            {
                "url": "https://example.com/test1.txt",
                "id": "1",
                "name": "test1.txt",
                "category": "document",
                "priority": 1
            },
            {
                "url": "https://example.com/test2.txt",
                "tags": ["important"]
            }
        ]
        """
        json_file = tmp_path / "test_files.json"
        json_file.write_text(json_content)

        pipeline = ProcessingPipeline()
        file_list = pipeline._prepare_file_list(json_file)

        assert len(file_list) == 2
        assert file_list[0].category == "document"
        assert file_list[0].priority == 1
        assert file_list[1].tags == ["important"]


class TestFileLoading:
    """Test file loading functionality."""

    def test_load_files_from_json(self, temp_json_file):
        """Test loading files from a valid JSON file."""
        pipeline = ProcessingPipeline()
        file_list = pipeline._prepare_file_list(temp_json_file)

        assert len(file_list) == 2
        assert file_list[0].id == "1"
        assert file_list[0].name == "test1.txt"
        assert file_list[0].url == "https://example.com/test1.txt"

    def test_load_files_from_list(self, test_file_list):
        """Test loading files from a list of dictionaries."""
        pipeline = ProcessingPipeline()
        file_list = pipeline._prepare_file_list(test_file_list)

        assert len(file_list) == 2
        assert file_list[0].id == "1"
        assert file_list[0].name == "test1.txt"
        assert file_list[0].url == "https://example.com/test1.txt"

    def test_load_files_missing_url(self):
        """Test error handling when URL is missing."""
        pipeline = ProcessingPipeline()
        invalid_data = [{"id": "1", "name": "test.txt"}]

        with pytest.raises(KeyError, match="Required 'url' not provided"):
            pipeline._prepare_file_list(invalid_data)

    def test_load_files_invalid_json(self, tmp_path):
        """Test error handling with invalid JSON."""
        invalid_json = tmp_path / "invalid.json"
        invalid_json.write_text("{invalid json")

        pipeline = ProcessingPipeline()
        with pytest.raises(Exception):
            pipeline._prepare_file_list(invalid_json)

    def test_load_files_with_defaults(self):
        """Test that missing optional fields use defaults."""
        pipeline = ProcessingPipeline()
        data = [{"url": "https://example.com/test.txt"}]

        file_list = pipeline._prepare_file_list(data)
        assert len(file_list) == 1
        assert file_list[0].url == "https://example.com/test.txt"
        assert file_list[0].name == "test"  # Default from URL basename
        assert file_list[0].id == "test"  # Default from basename
