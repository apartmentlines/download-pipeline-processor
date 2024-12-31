"""Test fixtures for pipeline testing."""

import json
import pytest
import shutil
from pathlib import Path
from typing import List, Dict
from download_pipeline_processor.processing_pipeline import ProcessingPipeline
from tests.test_processors import FileWritingProcessor, FileWritingPostProcessor
from .test_constants import PROCESSOR_OUTPUT_DIR, POST_PROCESSOR_OUTPUT_DIR


@pytest.fixture(autouse=True)
def setup_test_dirs():
    """Create test directories before each test and clean up after."""
    # Setup
    PROCESSOR_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    POST_PROCESSOR_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    yield  # This is where the test runs

    # Teardown
    shutil.rmtree(PROCESSOR_OUTPUT_DIR, ignore_errors=True)
    shutil.rmtree(POST_PROCESSOR_OUTPUT_DIR, ignore_errors=True)


@pytest.fixture
def temp_json_file(tmp_path) -> Path:
    """Create a temporary JSON file with test data."""
    test_data = [
        {"id": "1", "name": "test1.txt", "url": "https://example.com/test1.txt"},
        {"id": "2", "name": "test2.txt", "url": "https://example.com/test2.txt"},
    ]
    json_file = tmp_path / "test_files.json"
    json_file.write_text(json.dumps(test_data))
    return json_file


@pytest.fixture
def test_file_list() -> List[Dict]:
    """Return a list of test file dictionaries."""
    return [
        {"id": "1", "name": "test1.txt", "url": "https://example.com/test1.txt"},
        {"id": "2", "name": "test2.txt", "url": "https://example.com/test2.txt"},
    ]


@pytest.fixture
def pipeline() -> ProcessingPipeline:
    """Create a ProcessingPipeline instance with test processors."""
    return ProcessingPipeline(
        processor_class=FileWritingProcessor,
        post_processor_class=FileWritingPostProcessor,
        simulate_downloads=True,
    )


@pytest.fixture
def debug_pipeline() -> ProcessingPipeline:
    """Create a ProcessingPipeline instance with debug enabled."""
    return ProcessingPipeline(
        processor_class=FileWritingProcessor,
        post_processor_class=FileWritingPostProcessor,
        simulate_downloads=True,
        debug=True,
    )
