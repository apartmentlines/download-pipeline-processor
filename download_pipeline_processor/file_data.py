from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Union, Dict, Any


@dataclass
class FileDataError:
    """Error information for a file being processed.

    Attributes:
        stage (str): The pipeline stage where the error occurred
        error (Exception): The actual error that occurred
    """

    stage: str
    error: Exception


@dataclass
class FileData:
    """Dataclass representing a file to be processed.

    Attributes:
        url (str): The URL of the file.
        id (Union[str, int]): An optional identifier for the file.
        name (str): An optional name for the file.
        local_path (Optional[Path]): The local path where the file is stored.
        additional_fields (Dict[str, Any]): A dictionary for storing arbitrary metadata.
        error (Optional[FileDataError]): Error information if processing failed.

    Dynamic Attributes:
        You can add and access arbitrary attributes dynamically. These attributes
        are stored in the `additional_fields` dictionary but can be accessed as
        if they were direct attributes of the object.

        Example:
            file_data = FileData(url="https://example.com/file.txt")
            file_data.custom_field = "value"  # Dynamically add an attribute
            print(file_data.custom_field)     # Access the attribute
    """

    url: str
    id: Union[str, int]
    name: str
    local_path: Optional[Path] = None
    additional_fields: Dict[str, Any] = field(default_factory=dict)
    error: Optional[FileDataError] = None

    def add_error(self, stage: str, error: Exception) -> None:
        """Record an error that occurred during processing."""
        self.error = FileDataError(stage=stage, error=error)

    @property
    def has_error(self) -> bool:
        """Check if any error occurred during processing."""
        return self.error is not None

    def __setattr__(self, name: str, value: Any) -> None:
        """Allow setting arbitrary attributes dynamically."""
        if name in self.__dataclass_fields__:
            # Use object.__setattr__ to avoid recursion
            object.__setattr__(self, name, value)
        else:
            self.additional_fields[name] = value

    def __getattr__(self, name: str) -> Any:
        """Allow accessing additional_fields values as attributes."""
        if name in self.additional_fields:
            return self.additional_fields[name]
        raise AttributeError(f"'FileData' object has no attribute '{name}'")
