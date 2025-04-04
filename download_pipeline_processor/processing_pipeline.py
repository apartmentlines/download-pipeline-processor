#!/usr/bin/env python

"""
A multi-threaded processing pipeline that downloads and processes files.
"""

import argparse
import importlib
import json
from .logger import Logger
import os
import queue
import random
import tempfile
import threading
import time
import requests
import traceback
from urllib.parse import urlparse, urlunparse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, List, Optional, Type, Union
from tenacity import retry, stop_after_attempt, wait_exponential

from .file_data import FileData
from .processors.base_processor import BaseProcessor
from .processors.base_post_processor import BasePostProcessor
from .processors.base_pre_processor import BasePreProcessor
from .processors.dummy_processor import DummyProcessor
from .processors.dummy_post_processor import DummyPostProcessor
from .processors.dummy_pre_processor import DummyPreProcessor

from .constants import (
    DEFAULT_PROCESSING_LIMIT,
    DEFAULT_DOWNLOAD_QUEUE_SIZE,
    DEFAULT_DOWNLOAD_CACHE,
    DEFAULT_MAX_RETRIES,
    DOWNLOAD_TIMEOUT,
    DEFAULT_QUEUE_TIMEOUT,
    DEFAULT_PRE_PROCESSING_QUEUE_SIZE,
    NO_SLEEP_ENV_VAR,
)

# Module level logger for CLI usage
cli_logger = Logger("CLI")


class ProcessingPipeline:
    """Main class for managing the processing pipeline."""

    def __init__(
        self,
        pre_processor_class: Type[BasePreProcessor] = DummyPreProcessor,
        processor_class: Type[BaseProcessor] = DummyProcessor,
        post_processor_class: Type[BasePostProcessor] = DummyPostProcessor,
        processing_limit: int = DEFAULT_PROCESSING_LIMIT,
        download_queue_size: int = DEFAULT_DOWNLOAD_QUEUE_SIZE,
        pre_processing_queue_size: int = DEFAULT_PRE_PROCESSING_QUEUE_SIZE,
        download_cache: Path = DEFAULT_DOWNLOAD_CACHE,
        simulate_downloads: bool = False,
        debug: bool = False,
    ):
        """
        Initialize the processing pipeline.

        :param pre_processor_class: Class to use for pre-processing files
        :param processor_class: Class to use for processing files
        :param post_processor_class: Class to use for post-processing results
        :param processing_limit: Maximum concurrent processing threads
        :param download_queue_size: Maximum size of downloaded files queue
        :param pre_processing_queue_size: Maximum size of pre-processed files queue
        :param simulate_downloads: Whether to simulate downloads
        :param debug: Enable debug logging
        """
        self.pre_processor_class = pre_processor_class
        self.processor_class = processor_class
        self.post_processor_class = post_processor_class
        self.processing_limit = processing_limit
        self.download_queue_size = download_queue_size
        self.pre_processing_queue_size = pre_processing_queue_size
        self.simulate_downloads = simulate_downloads
        self.debug = debug

        self.download_cache = download_cache
        self.download_cache.mkdir(parents=True, exist_ok=True)

        self.thread_local = threading.local()
        self.executor: Optional[ThreadPoolExecutor] = None
        self.download_queue: queue.Queue[Optional[FileData]] = queue.Queue()
        self.downloaded_queue: queue.Queue[Optional[FileData]] = queue.Queue(
            maxsize=self.download_queue_size
        )
        self.pre_processed_queue: queue.Queue[Optional[FileData]] = queue.Queue(
            maxsize=self.pre_processing_queue_size
        )
        self.shutdown_event: threading.Event = threading.Event()

        self.post_processing_queue: queue.Queue[Any] = queue.Queue()

        self.log = Logger(self.__class__.__name__, debug=self.debug)

    def populate_download_queue(self, file_list: List[FileData]) -> None:
        """Producer that enqueues files for download.

        :param file_list: List of FileData objects to process
        """
        for file_data in file_list:
            self.log.debug(f"Adding {file_data.name} to download queue.")
            self.download_queue.put(file_data)
        self.log.info("Finished populating download queue.")

    def download_files(self) -> None:
        try:
            while not self.shutdown_event.is_set():
                try:
                    file_data = self.download_queue.get()
                    if file_data is None:
                        break
                    try:
                        self.handle_download(file_data)
                        self.downloaded_queue.put(file_data)
                        if self.downloaded_queue.full():
                            self.log.debug(
                                "Downloaded queue is full - downloader will block"
                            )
                    except Exception as e:
                        self.log.error(f"Failed to download {file_data.name}: {e}")
                        file_data.add_error("download", e)
                        self.post_processing_queue.put((None, file_data))
                except Exception as e:
                    self.log.error(f"Error in download thread: {e}")
                    traceback.print_exc()
        finally:
            self.downloaded_queue.put(None)
            self.log.info("Exiting download thread.")

    def handle_download(self, file_data: FileData) -> None:
        """Handle the download of a file, either simulated or actual.

        :param file_data: FileData object containing file information
        """
        if self.simulate_downloads:
            self.simulate_download(file_data)
        else:
            self.download_file(file_data)

    def simulate_download(self, file_data: FileData) -> None:
        """Simulate a download by sleeping and setting a local path.

        :param file_data: FileData object containing file information
        """
        self.log.debug(f"Simulating download of {file_data.name} from {file_data.url}")
        if not os.getenv(NO_SLEEP_ENV_VAR):
            time.sleep(random.uniform(1, 3))
        file_data.local_path = Path(file_data.name)
        self.log.info(f"Simulated download of {file_data.name}")

    @retry(
        stop=stop_after_attempt(DEFAULT_MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        reraise=True,
    )
    def download_file(self, file_data: FileData) -> None:
        temp_path = self.create_cached_download_filepath(file_data)
        try:
            response = requests.get(file_data.url, timeout=DOWNLOAD_TIMEOUT)
            response.raise_for_status()
            with open(temp_path, "wb") as f:
                f.write(response.content)
            file_data.local_path = temp_path
            stripped_url = urlunparse(urlparse(file_data.url)._replace(query=""))
            self.log.info(
                f"Downloaded {file_data.name} from {stripped_url} to {file_data.local_path}"
            )
        except Exception as e:
            if temp_path.exists():
                temp_path.unlink()
            self.log.warning(f"Error downloading {file_data.name}: {e}. Retrying...")
            raise

    def create_cached_download_filepath(self, file_data: FileData) -> Path:
        """Create a temporary file for the download and return its path.

        :param file_data: FileData object containing file information
        :return: Path to the temporary file
        """
        temp_file = tempfile.NamedTemporaryFile(
            dir=self.download_cache,
            delete=False,
            prefix=f"{file_data.id}_",
            suffix=".tmp",
        )
        temp_file.close()
        local_path = Path(temp_file.name)
        return local_path

    def delete_cached_download_file(self, file_data: FileData) -> None:
        """Delete the temporary file for the download.

        :param file_data: FileData object containing file information
        """
        if (
            not self.simulate_downloads
            and file_data.local_path
            and file_data.local_path.exists()
        ):
            try:
                file_data.local_path.unlink()
                self.log.debug(f"Deleted cached file: {file_data.local_path}")
            except Exception as e:
                self.log.warning(
                    f"Failed to delete cached file {file_data.local_path}: {e}"
                )

    def pre_processor(self) -> None:
        try:
            pre_processor = self.pre_processor_class(debug=self.debug)
            while not self.shutdown_event.is_set():
                file_data = self.downloaded_queue.get()
                if file_data is None:
                    break
                try:
                    self.log.debug(f"Pre-processing file: {file_data.name}")
                    processed_data = pre_processor.pre_process(file_data)
                    self.log.info(f"Finished pre-processing for: {file_data.name}")
                    self.pre_processed_queue.put(processed_data)
                    if self.pre_processed_queue.full():
                        self.log.debug("Pre-processed queue is full - will block")
                except Exception as e:
                    self.log.error(f"Error pre-processing {file_data.name}: {e}")
                    file_data.add_error("pre_process", e)
                    self.post_processing_queue.put((None, file_data))
        finally:
            self.pre_processed_queue.put(None)
            self.log.info("Exiting pre-processing thread.")

    def _initialize_worker(self) -> None:
        """Initialize worker thread with its own Processor backend."""
        self.thread_local.processor = self._initialize_processor()

    def _initialize_processor(self) -> BaseProcessor:
        """Initialize the processor backend for the worker thread.

        :return: Processor
        """
        return self.processor_class(debug=self.debug)

    def processing_consumer(self) -> None:
        """Consumer that pulls from downloaded_queue and submits processing tasks."""
        file_count = 0
        active_futures = []
        while not self.shutdown_event.is_set():
            active_futures = [f for f in active_futures if not f.done()]
            try:
                file_data = self.pre_processed_queue.get()
                if file_data is None:
                    self.log.info(f"All {file_count} files submitted for processing.")
                    break
                file_count += 1
                if self.executor:
                    future = self.executor.submit(self.process_file, file_data)
                    active_futures.append(future)
                    self.log.debug(
                        f"Submitted processing task for {file_data.name}. Active tasks: {len(active_futures)}"
                    )
            except Exception as e:
                self.log.error(f"Error processing downloaded file: {e}")
        for future in active_futures:
            try:
                future.result()
            except Exception as e:
                self.log.error(f"Error in processing task: {e}")
                traceback.print_exc()

        self.log.info("Exiting processing thread.")

    def process_file(self, file_data: FileData) -> None:
        """Processing function, executed by the ThreadPoolExecutor.

        :param file_data: FileData object containing file information
        """
        try:
            if self.shutdown_event.is_set():
                self.log.debug(
                    f"Shutdown event set. Skipping processing for {file_data.name}"
                )
                return
            self.log.debug(f"Starting processing for {file_data.name}")
            processing_result = self.thread_local.processor.process(file_data)
            self.log.info(f"Finished processing for {file_data.name}")
            self.post_processing_queue.put((processing_result, file_data))
            self.log.debug(
                f"Enqueued processing result for {file_data.name} to post-processing queue"
            )
            self.delete_cached_download_file(file_data)
        except Exception as e:
            self.log.error(f"Error processing {file_data.name}: {e}")
            file_data.add_error("process", e)
            self.post_processing_queue.put((None, file_data))

    def post_processor(self) -> None:
        """Process the processing results from the post-processing queue."""
        while (
            not self.shutdown_event.is_set() or not self.post_processing_queue.empty()
        ):
            try:
                result, file_data = self.post_processing_queue.get(
                    timeout=DEFAULT_QUEUE_TIMEOUT
                )
                post_processor = self.post_processor_class(debug=self.debug)
                self.log.debug(f"Starting post-processing for: {file_data.name}")
                post_processor.post_process(result, file_data)
                self.log.info(f"Finished post-processing for: {file_data.name}")
            except queue.Empty:
                continue
            except Exception as e:
                self.log.error(f"Error in post-processing: {e}")
        self.log.info("Exiting post-processing thread.")

    def create_file_data(self, file_dict: dict) -> FileData:
        """Create a FileData object from a dictionary, using defaults for optional missing fields.
        Any additional fields in the dictionary will be stored in additional_fields.

        :param file_dict: Dictionary containing file information
        :return: FileData object with all fields populated
        :raises KeyError: If URL is not provided in the input dictionary
        """
        try:
            url = file_dict["url"]
        except KeyError:
            raise KeyError(f"Required 'url' not provided for file: {file_dict}")

        basename = Path(url).stem
        id = file_dict.get("id", basename)
        name = file_dict.get("name", basename)

        # Create a dict of additional fields (excluding known fields)
        known_fields = {"url", "id", "name"}
        additional_fields = {
            k: v for k, v in file_dict.items() if k not in known_fields
        }

        return FileData(url=url, id=id, name=name, additional_fields=additional_fields)

    def _prepare_file_list(
        self, input_data: Union[Path, List[dict]] | None
    ) -> List[FileData]:
        """
        Prepare a list of FileData objects from either a Path to a JSON file or a list of dicts.

        :param input_data: Either a Path to a JSON file or a list of dicts
        :return: List of FileData objects
        :raises TypeError: If input_data is neither a Path nor a list of dicts
        """
        if isinstance(input_data, Path):
            return self._load_files_from_path(input_data)
        elif isinstance(input_data, list):
            return self._load_files_from_list(input_data)
        else:
            raise TypeError("input_data must be either a Path or a list of dicts")

    def _load_files_from_path(self, file_path: Path) -> List[FileData]:
        """
        Load and parse the JSON file containing files to process.

        :param file_path: Path to JSON file
        :return: List of FileData objects
        :raises Exception: If file cannot be loaded or parsed
        """
        self.log.info(f"Loading files from {file_path}")
        try:
            with open(file_path) as f:
                file_data = json.load(f)
                return self._load_files_from_list(file_data)
        except KeyError as e:
            self.log.error(f"Invalid file entry: {str(e)}")
            exit(1)
        except Exception as e:
            self.log.error(f"Error loading JSON file: {e}")
            raise

    def _load_files_from_list(self, file_list: List[dict]) -> List[FileData]:
        """
        Convert a list of dicts into a list of FileData objects.

        :param file_list: List of dicts containing file information
        :return: List of FileData objects
        :raises KeyError: If URL is not provided in any of the input dicts
        """
        return [self.create_file_data(item) for item in file_list]

    def run(self, input_data: Union[Path, List[dict]]) -> int:
        """
        Run the processing pipeline with the given input data.

        :param input_data: Either a Path to a JSON file or a list of dicts
        :return: Exit code (0 for success, non-zero for failure)
        """
        self.log.info("Starting processing pipeline...")

        try:
            file_list = self._prepare_file_list(input_data)
        except TypeError as e:
            cli_logger.error(e)
            return 1

        pre_processor_thread = threading.Thread(
            target=self.pre_processor,
            name="PreProcessor",
            daemon=True,
        )
        post_processor_thread = threading.Thread(
            target=self.post_processor,
            name="PostProcessor",
            daemon=True,
        )
        download_thread = threading.Thread(
            target=self.download_files,
            daemon=True,
            name="Downloader",
        )
        processing_thread = threading.Thread(
            target=self.processing_consumer,
            daemon=True,
            name="Processor",
        )

        with ThreadPoolExecutor(
            max_workers=self.processing_limit,
            thread_name_prefix="ProcessingWorker",
            initializer=self._initialize_worker,
        ) as executor:
            self.executor = executor

            download_thread.start()
            pre_processor_thread.start()
            processing_thread.start()
            post_processor_thread.start()

            try:
                self.populate_download_queue(file_list)
                self.download_queue.put(None)  # Signal that no more files will be added

                download_thread.join()
                pre_processor_thread.join()
                processing_thread.join()

                # Signal the post_processor to shutdown
                self.shutdown_event.set()
                self.log.debug("Signaled shutdown event.")

                post_processor_thread.join()

                self.log.info("Pipeline completed successfully.")
                return 0

            except KeyboardInterrupt:
                self.log.info("Received interrupt signal. Shutting down gracefully...")
                self.shutdown_event.set()

                download_thread.join()
                pre_processor_thread.join()
                processing_thread.join()
                post_processor_thread.join()

                return 130
            except Exception as e:
                self.log.error(f"Pipeline failed: {e}")
                return 1


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments.

    :return: Parsed arguments
    """
    parser = argparse.ArgumentParser(description="Run the processing pipeline.")
    parser.add_argument(
        "--files",
        type=Path,
        required=True,
        help="Path to JSON file containing list of files to process",
    )
    parser.add_argument(
        "--pre-processor",
        type=str,
        help="Custom pre-processing class in 'package:ClassName' format",
    )
    parser.add_argument(
        "--processor",
        type=str,
        help="Custom processing class in 'package:ClassName' format",
    )
    parser.add_argument(
        "--post-processor",
        type=str,
        help="Custom post-processing class in 'package:ClassName' format",
    )

    def positive_int(value):
        ivalue = int(value)
        if ivalue <= 0:
            raise argparse.ArgumentTypeError(f"{value} is not a positive integer")
        return ivalue

    parser.add_argument(
        "--processing-limit",
        type=positive_int,
        default=DEFAULT_PROCESSING_LIMIT,
        help="Maximum concurrent processing threads, default %(default)s",
    )
    parser.add_argument(
        "--download-queue-size",
        type=positive_int,
        default=DEFAULT_DOWNLOAD_QUEUE_SIZE,
        help="Maximum size of downloaded files queue, default %(default)s",
    )
    parser.add_argument(
        "--pre-processing-queue-size",
        type=positive_int,
        default=DEFAULT_PRE_PROCESSING_QUEUE_SIZE,
        help="Maximum size of pre-processed files queue, default %(default)s",
    )
    parser.add_argument(
        "--download-cache",
        type=Path,
        default=DEFAULT_DOWNLOAD_CACHE,
        help="Directory to cache downloaded files, default %(default)s",
    )
    parser.add_argument(
        "--simulate-downloads",
        action="store_true",
        help="Simulate downloads instead of performing actual downloads",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )
    return parser.parse_args()


def validate_processor_class(class_path: str, base_class: Type) -> Type:
    """
    Validate and load a processor class from a module path.

    :param class_path: String in format 'module:ClassName'
    :param base_class: Base class that the loaded class must inherit from
    :return: The validated class
    :raises RuntimeError: If the class cannot be imported or doesn't inherit from base_class
    """
    try:
        module_name, class_name = class_path.split(":")
        module = importlib.import_module(module_name)
        cls = getattr(module, class_name)
        if not issubclass(cls, base_class):
            raise RuntimeError(
                f"The class '{class_path}' must inherit from {base_class.__name__}."
            )
        return cls
    except (ValueError, ModuleNotFoundError, AttributeError) as e:
        raise RuntimeError(f"Cannot load class '{class_path}': {e}") from e


def main() -> int:
    """Main function to run the processing pipeline.

    :return: Exit code (0 for success, non-zero for failure)
    """
    args = parse_arguments()

    pre_processor_class = DummyPreProcessor
    processor_class = DummyProcessor
    post_processor_class = DummyPostProcessor

    if args.pre_processor:
        try:
            pre_processor_class = validate_processor_class(
                args.pre_processor, BasePreProcessor
            )
        except RuntimeError as e:
            cli_logger.error(e)
            return 1

    if args.processor:
        try:
            processor_class = validate_processor_class(args.processor, BaseProcessor)
        except RuntimeError as e:
            cli_logger.error(e)
            return 1

    if args.post_processor:
        try:
            post_processor_class = validate_processor_class(
                args.post_processor, BasePostProcessor
            )
        except RuntimeError as e:
            cli_logger.error(e)
            return 1

    # Create pipeline with validated classes
    pipeline = ProcessingPipeline(
        pre_processor_class=pre_processor_class,
        processor_class=processor_class,
        post_processor_class=post_processor_class,
        processing_limit=args.processing_limit,
        download_queue_size=args.download_queue_size,
        pre_processing_queue_size=args.pre_processing_queue_size,
        download_cache=args.download_cache,
        simulate_downloads=args.simulate_downloads,
        debug=args.debug,
    )

    return pipeline.run(Path(args.files))


if __name__ == "__main__":
    exit(main())
