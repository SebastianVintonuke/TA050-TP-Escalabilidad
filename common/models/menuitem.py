from dataclasses import dataclass
from .model import Model
from typing import ClassVar, Optional

@dataclass
class MenuItem(Model):
    _ORIGINAL_HEADER: ClassVar[str] = "item_id,item_name,category,price,is_seasonal,available_from,available_to"

    item_id: int
    item_name: str
    category: str
    price: float

    @classmethod
    def from_bytes_and_project(cls, data: bytes) -> "MenuItem":
        """
        Check superclass documentation
        Expected format:
        item_id,item_name,category,price,is_seasonal,available_from,available_to
        Example:
        b"1,Espresso,coffee,6.0,False,,"
        """
        line = data.decode("utf-8").strip()
        fields = line.split(",")

        item_id = int(fields[0])
        item_name = fields[1].strip()
        category = fields[2].strip()
        price = float(fields[3])

        return cls(
            item_id=item_id,
            item_name=item_name,
            category=category,
            price=price,
        )

    @classmethod
    def from_bytes(cls, data: bytes) -> "MenuItem":
        """
        Check superclass documentation
        Expected format:
        item_id,item_name,category,price
        Example:
        b"1,Espresso,coffee,6.0"
        """
        line = data.decode("utf-8").strip()
        fields = line.split(",")

        item_id = int(fields[0])
        item_name = fields[1].strip()
        category = fields[2].strip()
        price = float(fields[3])

        return cls(
            item_id=item_id,
            item_name=item_name,
            category=category,
            price=price,
        )

    def __str__(self) -> str:
        return f"{self.item_id},{self.item_name},{self.category},{self.price}"
