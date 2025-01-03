# Download Pipeline Processor


## Project Overview

The Download Pipeline Processor is a multi-threaded processing system designed to efficiently handle downloading a batch of files and running them through some kind of processing. It provides a flexible framework for:

- Managing concurrent downloads
- Pre-processing/processing downloaded files
- Post-processing results
- Simulating downloads for testing

Key Features:
- Customizable pre-processors, processors and post-processors
- Download queue (configurable size limit)
- Pre-processing queue (configurable size limit)
- Thread pool for parallel processing of files (configurable size limit)
- Automatic retry mechanism for failed downloads
- Temporary file caching system
- Simulation mode for testing


## Installation

### Prerequisites

- Python 3.9 or higher
- Required packages: `requests`, `tenacity`

### Installation Steps

1. Clone the repository
2. Install the package in development mode:
   ```bash
   pip install -e .
   ```


## Usage

### Input Formats

The pipeline supports two input formats:

1. **JSON File** - Provide a path to a JSON file containing the metadata for all files:
   ```json
   [
       {
           "id": "unique_id",          // Optional, string or int
           "name": "file_name",        // Optional, string
           "url": "https://example.com/file.txt"  // Required
       }
   ]
   ```

2. **Python List of Dicts** - Provide a list of dictionaries containing the file metadata:
   ```python
   [
       {"id": "file1", "name": "example.txt", "url": "https://example.com/file1.txt"},
       {"url": "https://example.com/file2.txt"}
   ]
   ```

Both formats require at minimum a `url` field for each file. The `id` and `name` fields are optional.

### Executing

**Command Line Interface** (requires JSON file):

```bash
download-pipeline-processor --files path/to/files.json
```

**Programmatic Usage** (supports both formats):

```python
# Using a JSON file
pipeline = ProcessingPipeline()
pipeline.run(Path("path/to/files.json"))

# Using a list of dicts
file_list = [
    {"id": "file1", "name": "example.txt", "url": "https://example.com/file1.txt"},
    {"url": "https://example.com/file2.txt"}
]
pipeline = ProcessingPipeline()
pipeline.run(file_list)
```

To run a full simulation of the pipeline:

```bash
download-pipeline-processor --files path/to/files.json --simulate-downloads
```

### Configuration Options

Run `download-pipeline-processor` with the `--help` argument for a description of all arguments.


## Error Handling

The pipeline provides built-in error handling at each stage (download, pre-processing, processing, and post-processing). Failed files skip remaining stages and are sent directly to post-processing with error information attached.

#### How It Works

- Processing errors are captured and tracked in the `FileData` object
- Other files continue processing normally
- Post-processor receives both successful and failed files

#### Best Practices

1. **In Custom Processors**: Raise exceptions when errors occur - the pipeline will handle them:
   ```python
   class CustomProcessor(BaseProcessor):
       def process(self, file_data: FileData) -> Any:
           if invalid_condition:
               raise ValueError("Specific error message")
           return result
   ```

2. **In Post-Processors**: Always check for errors before processing:
   ```python
   class CustomPostProcessor(BasePostProcessor):
       def post_process(self, result: Any, file_data: FileData) -> None:
           if file_data.has_error:
               self.log.warning(f"Error in {file_data.error.stage}: {file_data.error.error}")
               return
           # Handle successful case
   ```


## Extending the Pipeline

### Creating Custom Pre-Processors

To create a custom pre-processor:

1. Inherit from `BasePreProcessor`
2. Implement the `pre_process` method

Example:

```python
from download_pipeline_processor.processors.base_pre_processor import BasePreProcessor
from download_pipeline_processor.file_data import FileData

class CustomPreProcessor(BasePreProcessor):
    def pre_process(self, file_data: FileData) -> FileData:
        # Custom pre-processing logic
        return file_data
```

### Creating Custom Processors

To create a custom processor:

1. Inherit from `BaseProcessor`
2. Implement the `process` method

Example:

```python
from download_pipeline_processor.processors.base_processor import BaseProcessor
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
from download_pipeline_processor.processors.base_post_processor import BasePostProcessor
from download_pipeline_processor.file_data import FileData

class CustomPostProcessor(BasePostProcessor):
    def post_process(self, result: Any, file_data: FileData) -> None:
        # Custom post-processing logic
```

### FileData Class

The `FileData` class is the core data structure used throughout the pipeline. It represents a file to be processed and supports both standard and dynamic attributes.

#### Reserved Attributes

##### Set from the passed list of file data
- `url`
- `id`
- `name`

##### Managed internally
- `local_path`
- `additional_fields`

#### Dynamic Attributes
You can add and access arbitrary attributes dynamically. These attributes are stored in the `additional_fields` dictionary but can be accessed as if they were direct attributes of the object.

Example:

```python
file_data = FileData(url="https://example.com/file.txt")
file_data.custom_field = "value"  # Dynamically add an attribute
print(file_data.custom_field)     # Access the attribute
```

Note:
- Accessing non-existent attributes raises an `AttributeError`


## Architecture Overview

The pipeline follows this workflow:

1. Files are loaded from the input JSON
2. Files are added to the download queue
3. Downloader thread processes the queue:
   - Either perform actual downloads or simulate them
   - Store files in temporary cache
4. Downloaded files are added to the pre-processing queue
5. Pre-processor thread:
   - Pre-processes files using the configured pre-processor
   - Add files to the processing queue
6. Processor threads:
   - Process files using the configured processor
   - Add results to the post-processing queue
7. Post-processor handles the final results


## Testing

### Running Tests

To run the test suite:

1. Install development dependencies:
   ```bash
   pip install -e ".[dev]"
   ```

2. Run pytest:
   ```bash
   pytest
   ```

### Test Environment

Tests use temporary directories for file operations and mock network calls by default. The environment variable `PIPELINE_NO_SLEEP` can be set to skip simulated processing delays during testing.


## Troubleshooting

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
