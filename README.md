# Download Pipeline Processor

## 1. Project Overview

The Download Pipeline Processor is a multi-threaded processing system designed to efficiently handle downloading a batch of files and running them through some kind of processing. It provides a flexible framework for:

- Managing concurrent downloads
- Processing downloaded files
- Post-processing results
- Simulating downloads for testing

Key Features:
- Customizable processors and post-processors
- Download queue with size limit
- Thread pool for parallel processing of files with size limit
- Automatic retry mechanism for failed downloads
- Temporary file caching system
- Simulation mode for testing

## 2. Installation

### Prerequisites
- Python 3.9 or higher
- Required packages: `requests`, `tenacity`

### Installation Steps
1. Clone the repository
2. Install the package in development mode:
   ```bash
   pip install -e .
   ```

## 3. Usage

### Input File Format

To process a list of files, provide a JSON file containing the metadata for all files in the following format:

```json
[
    {
        "id": "unique_id",          // Optional, string or int
        "name": "file_name",        // Optional, string
        "url": "https://example.com/file.txt"  // Required
    }
]
```

### Basic Usage

Run the pipeline with a JSON file containing files to process:

```bash
download-pipeline-processor --files path/to/files.json
```

To run a full simulation of the pipeline:

```bash
download-pipeline-processor --files path/to/files.json --simulate-downloads
```

### Configuration Options

Run `download-pipeline-processor` with the `--help` argument for a description of all arguments.

## 4. Extending the Pipeline

### Creating Custom Processors

To create a custom processor:

1. Inherit from `BaseProcessor`
2. Implement the `process` method

Example:

```python
from download_pipeline_processor.base_processor import BaseProcessor
from download_pipeline_processor.file_data import FileData

class CustomProcessor(BaseProcessor):
    def process(self, file_data: FileData) -> Any:
        # Custom processing logic
        return result
```

### Creating Custom Post-Processors

To create a custom post-processor:

1. Inherit from `BasePostProcessor`
2. Implement the `post_process` method

Example:

```python
from download_pipeline_processor.base_post_processor import BasePostProcessor

class CustomPostProcessor(BasePostProcessor):
    def post_process(self, result: Any) -> None:
        # Custom post-processing logic
```

## 5. Architecture Overview

The pipeline follows this workflow:

1. Files are loaded from the input JSON
2. Files are added to the download queue
3. Downloader thread processes the queue:
   - Either perform actual downloads or simulate them
   - Store files in temporary cache
4. Downloaded files are added to the processing queue
5. Processor threads:
   - Process files using the configured processor
   - Add results to the post-processing queue
6. Post-processor handles the final results

## 9. Troubleshooting

### Common Issues

1. **Missing URL in input file**
   - Ensure each file object has a "url" field

### Debugging Tips

- Use `--debug` flag for detailed logging
- Check log messages for specific error details
- Use simulation mode (`--simulate-downloads`) to test without actual downloads

### Error Handling

- Failed downloads are automatically retried (up to 3 times by default)
- Processing errors are logged but don't stop the pipeline
- KeyboardInterrupt triggers graceful shutdown
