from abc import ABC, abstractmethod
from typing import Type, TypeVar

T = TypeVar("T", bound="QueryResult")

class QueryResult(ABC):
    @classmethod
    @abstractmethod
    def from_bytes(cls: Type[T], data: bytes) -> T:
        """
        Receives a CSV line as bytes, return an instance of the model
        Must be implemented by subclasses
        """
        pass

    @abstractmethod
    def __str__(self) -> str:
        """
        Returns a string representation of the model
        Must be implemented by subclasses
        """
        pass

    def to_bytes(self) -> bytes:
        """
        Returns a bytes representation of the model
        """
        return self.__str__().encode("utf-8")