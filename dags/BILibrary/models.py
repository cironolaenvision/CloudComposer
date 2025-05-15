"""
Data models for BI Library.
"""

import dataclasses
from dataclasses import dataclass
from typing import ClassVar, Dict

class ElementType:
    """Enum-like class for element types."""
    View = 1,
    UDF = 2,
    Procedure = 3,
    MaterializedView = 4

@dataclass
class Account:
    """Data class representing an account."""
    AccountName: str
    CollectionName: int
    Id: str
    IdName: str

@dataclass
class Element:
    """Data class representing a database element (view, UDF, or procedure)."""
    __version__: ClassVar[int] = 1
    Name: str
    Type: ElementType
    Definition: str
    DDL: str
    
    def serialize(self) -> dict:
        """Convert the element to a dictionary."""
        return dataclasses.asdict(self)
    
    @staticmethod
    def deserialize(data: dict, version: int):
        """Create an Element instance from a dictionary."""
        return Element(**data) 