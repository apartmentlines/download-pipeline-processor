from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Union, Dict, Any


@dataclass
class FileData:
    """Dataclass representing a file to be processed."""

    url: str
    id: Optional[Union[str, int]] = None
    name: Optional[str] = None
    local_path: Optional[Path] = None
    additional_fields: Dict[str, Any] = field(default_factory=dict)

    def __getattr__(self, name: str) -> Any:
        """Allow accessing additional_fields values as attributes."""
        if name in self.additional_fields:
            return self.additional_fields[name]
        raise AttributeError(f"'FileData' object has no attribute '{name}'")
