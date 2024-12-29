from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union


@dataclass
class FileData:
    """Dataclass representing a file to be processed."""
    url: str
    id: Optional[Union[str, int]] = None
    name: Optional[str] = None
    local_path: Optional[Path] = None
