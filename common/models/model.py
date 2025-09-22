from abc import ABC, abstractmethod
from typing import Type, ClassVar, TypeVar

T = TypeVar("T", bound="Model")

class Model(ABC):
    _ORIGINAL_HEADER: ClassVar[str]

    @classmethod
    def model_for(cls: Type[T], header: bytes) -> Type["Model"]:
        """
        Receives a CSV header as bytes, returns the class associated with the CSV
        Raise ValueError if no class associated with the header
        """
        for subclass in cls.__subclasses__():
            if subclass.is_header_of(header):
                return subclass
        raise ValueError(f"Unknown CSV Header: {header.decode('utf-8').strip()}")

    @classmethod
    def is_header_of(cls: Type[T], header: bytes) -> bool:
        """
        Receives a header, return true if it is from a user CSV, false otherwise
        """
        return header.decode("utf-8").strip() == cls._ORIGINAL_HEADER

    @classmethod
    @abstractmethod
    def from_bytes_and_project(cls: Type[T], data: bytes) -> T:
        """
        Receives a CSV line as bytes, return an instance of the model without the unnecessary columns
        To be used to sanitize the original data uploaded by the user
        Should be implemented by subclasses
        """
        pass

    @classmethod
    @abstractmethod
    def from_bytes(cls: Type[T], data: bytes) -> T:
        """
        Receives a CSV line as bytes, return an instance of the model
        Should be implemented by subclasses
        """
        pass


    @abstractmethod
    def to_bytes(self) -> bytes:
        """
        Returns a bytes representation of the model
        Should be implemented by subclasses
        """
        pass
