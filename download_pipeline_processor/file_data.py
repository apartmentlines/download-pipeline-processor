from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Union, Dict, Any


@dataclass
class FileData:
    """Dataclass representing a file to be processed.

    Attributes:
        url (str): The URL of the file.
        id (Optional[Union[str, int]]): An optional identifier for the file.
        name (Optional[str]): An optional name for the file.
        local_path (Optional[Path]): The local path where the file is stored.
        additional_fields (Dict[str, Any]): A dictionary for storing arbitrary metadata.

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
    id: Optional[Union[str, int]] = None
    name: Optional[str] = None
    local_path: Optional[Path] = None
    additional_fields: Dict[str, Any] = field(default_factory=dict)

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
