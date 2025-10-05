from dataclasses import dataclass
from datetime import datetime
from .model import Model
from typing import ClassVar

@dataclass
class TransactionItem(Model):
    _ORIGINAL_HEADER: ClassVar[str] = "transaction_id,item_id,quantity,unit_price,subtotal,created_at"

    transaction_id: str
    item_id: int
    quantity: int
    unit_price: float
    subtotal: float
    created_at: datetime

    @classmethod
    def from_bytes_and_project(cls, data: bytes) -> "TransactionItem":
        """
        Check superclass documentation
        Expected format:
        transaction_id,item_id,quantity,unit_price,subtotal,created_at
        Example:
        b"79e08e8a-488b-4b27-bf33-7095ddd52b29,6,3,9.5,28.5,2025-01-01 12:20:53"
        """
        line = data.decode("utf-8").strip()
        fields = line.split(",")

        transaction_id = fields[0].strip()
        item_id = int(fields[1])
        quantity = int(fields[2])
        unit_price = float(fields[3])
        subtotal = float(fields[4])
        created_at = datetime.strptime(fields[5], "%Y-%m-%d %H:%M:%S")

        return cls(
            transaction_id=transaction_id,
            item_id=item_id,
            quantity=quantity,
            unit_price=unit_price,
            subtotal=subtotal,
            created_at=created_at,
        )

    @classmethod
    def from_bytes(cls, data: bytes) -> "TransactionItem":
        """
        Check superclass documentation
        Expected format:
        transaction_id,item_id,quantity,unit_price,subtotal,created_at
        Example:
        b"79e08e8a-488b-4b27-bf33-7095ddd52b29,6,3,9.5,28.5,2025-01-01 12:20:53"
        """
        line = data.decode("utf-8").strip()
        fields = line.split(",")

        transaction_id = fields[0].strip()
        item_id = int(fields[1])
        quantity = int(fields[2])
        unit_price = float(fields[3])
        subtotal = float(fields[4])
        created_at = datetime.strptime(fields[5], "%Y-%m-%d %H:%M:%S")

        return cls(
            transaction_id=transaction_id,
            item_id=item_id,
            quantity=quantity,
            unit_price=unit_price,
            subtotal=subtotal,
            created_at=created_at,
        )

    def __str__(self) -> str:
        return f"{self.transaction_id},{self.item_id},{self.quantity},{self.unit_price},{self.subtotal},{self.created_at}"
