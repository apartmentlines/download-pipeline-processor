"""Tests for the ProcessingPipeline class."""

import logging
import os
import pytest
import requests
import subprocess
import threading
from .test_utils import MockRequestUtils
from pathlib import Path
from unittest.mock import patch
from download_pipeline_processor.file_data import FileData
from download_pipeline_processor.processing_pipeline import ProcessingPipeline
from download_pipeline_processor.constants import (
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


class TestEdgeCasesAndErrorHandling:
    """Test edge cases and error handling in the pipeline."""

    @patch("time.sleep")
    def test_empty_file_list(self, mock_sleep, pipeline, tmp_path):
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
    def test_invalid_urls(self, mock_sleep, pipeline, monkeypatch):
        """Test pipeline handles invalid URLs appropriately."""
        test_file = FileData(
            id="1", name="test1.txt", url="https://invalid-url.com/test1.txt"
        )

        # Mock requests.get to simulate a connection error
        def mock_failed_request(*args, **kwargs):
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
    def test_exception_handling(self, mock_sleep, pipeline):
        """Test pipeline handles various exceptions without crashing."""
        test_files = [
            {"id": "1", "name": "test1.txt", "url": "https://example.com/test1.txt"}
        ]

        # Test download exception
        def mock_download(*args, **kwargs):
            raise Exception("Simulated download error")

        # Test processing exception
        def mock_process(*args, **kwargs):
            raise Exception("Simulated processing error")

        # Test post-processing exception
        def mock_post_process(*args, **kwargs):
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
    def test_end_to_end_execution(self, mock_sleep, pipeline):
        """Test complete pipeline execution with file artifacts."""
        # Create test data
        test_files = [
            {"id": "1", "name": "test1.txt", "url": "https://example.com/test1.txt"},
            {"id": "2", "name": "test2.txt", "url": "https://example.com/test2.txt"},
        ]

        # Run pipeline
        result = pipeline.run(test_files)

        # Verify pipeline completed successfully
        assert result == 0

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
    def test_graceful_shutdown(self, mock_sleep, pipeline, tmp_path, monkeypatch):
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

    def test_logging_configuration(self):
        """Test logging configuration works correctly."""
        # Test debug mode
        pipeline = ProcessingPipeline(debug=True)
        assert pipeline.log.level == logging.DEBUG
        assert len(pipeline.log.handlers) == 1
        assert isinstance(pipeline.log.handlers[0], logging.StreamHandler)

        # Test normal mode
        pipeline = ProcessingPipeline(debug=False)
        assert pipeline.log.level == logging.INFO


class TestDownloadFunctionality:
    """Test download functionality of the pipeline."""

    @patch("time.sleep")
    def test_simulate_download(self, mock_sleep, pipeline):
        """Test simulated download functionality."""
        file_data = FileData(url="https://example.com/test.txt", name="test.txt")
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
            url="https://example.com/test.txt", name="test.txt", local_path=test_file
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
        file_data = FileData(url="https://example.com/test.txt", name="test.txt")
        # Create processor directly to test just the processing logic
        processor = pipeline.processor_class()
        result = processor.process(file_data)
        assert str(Path(result).name).startswith("processed_")
        assert str(Path(result).name).endswith(file_data.name)

    def test_process_file_with_error(self):
        """Test processing with error processor."""
        pipeline = ProcessingPipeline(
            processor_class=ErrorProcessor, simulate_downloads=True
        )
        file_data = FileData(url="https://example.com/test.txt", name="test.txt")
        pipeline.process_file(file_data)
        assert pipeline.post_processing_queue.empty()


class TestPostProcessingFunctionality:
    """Test post-processing functionality of the pipeline."""

    def test_post_processor_handles_result(self, tmp_path):
        """Test post-processor handles results correctly."""
        # Create a test input file
        input_file = tmp_path / "processed_test.txt"
        input_file.write_text("Processed test content")

        # Run post-processor
        post_processor = FileWritingPostProcessor()
        post_processor.post_process(str(input_file))

        # Verify output file exists with correct content
        output_file = POST_PROCESSOR_OUTPUT_DIR / input_file.name
        assert output_file.exists()
        assert output_file.read_text() == "Post-processed Processed test content"


class TestConcurrencyAndThreading:
    """Test concurrent processing behavior of the pipeline."""

    @patch("time.sleep")
    def test_concurrent_downloads_and_processing(self, mock_sleep):
        """Test that downloads and processing occur concurrently."""
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
            processor_class=FileWritingProcessor,
            post_processor_class=FileWritingPostProcessor,
            simulate_downloads=True,
            processing_limit=2,  # Limit concurrent processing
        )

        # Run pipeline
        result = pipeline.run(test_files)

        # Verify successful completion
        assert result == 0

        # Verify all files were processed
        processed_files = list(PROCESSOR_OUTPUT_DIR.iterdir())
        assert len(processed_files) == 5

        # Verify post-processing completed
        post_processed_files = list(POST_PROCESSOR_OUTPUT_DIR.iterdir())
        assert len(post_processed_files) == 5

    @patch("time.sleep")
    def test_download_queue_full_behavior(self, mock_sleep):
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
    def test_shutdown_event_propagation(self, mock_sleep):
        """Test that shutdown event stops all threads properly."""
        processing_started = threading.Event()

        class SignalingProcessor(FileWritingProcessor):
            def process(self, file_data: FileData) -> str:
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

        # Verify queues are empty or being drained
        assert pipeline.download_queue.empty()
        assert pipeline.post_processing_queue.empty()

    def test_post_processor_handles_error(self):
        """Test post-processor handles errors gracefully."""
        post_processor = ErrorPostProcessor()
        test_result = "Test Result"
        with pytest.raises(ValueError, match="Simulated error post-processing"):
            post_processor.post_process(test_result)


class TestCommandLineInterface:
    """Test command-line interface functionality."""

    @patch("time.sleep")
    def test_cli_default_arguments(self, mock_sleep, temp_json_file):
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
