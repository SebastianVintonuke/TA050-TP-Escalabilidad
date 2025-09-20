from abc import ABC, abstractmethod
from typing import Type, ClassVar, TypeVar

T = TypeVar("T", bound="Model")

class Model(ABC):
    _HEADER: ClassVar[str]

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
        return header.decode("utf-8").strip() == cls._HEADER

    @classmethod
    @abstractmethod
    def from_bytes(cls: Type[T], data: bytes) -> T:
        """
        Convierte una línea CSV en bytes en una instancia del modelo.
        Debe ser implementado por cada subclase.
        """
        pass
