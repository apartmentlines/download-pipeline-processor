"""Constants used for testing the pipeline."""

import tempfile
from pathlib import Path

# Global test directories
TEST_ROOT_DIR = Path(tempfile.gettempdir()) / "download-pipeline-processor"
PROCESSOR_OUTPUT_DIR = TEST_ROOT_DIR / "processor-output"
POST_PROCESSOR_OUTPUT_DIR = TEST_ROOT_DIR / "post-processor-output"
