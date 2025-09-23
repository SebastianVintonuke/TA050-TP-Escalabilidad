from dataclasses import dataclass
from datetime import datetime
from .model import Model
from typing import ClassVar, Optional

@dataclass
class Transaction(Model):
    _ORIGINAL_HEADER: ClassVar[str] = "transaction_id,store_id,payment_method_id,voucher_id,user_id,original_amount,discount_applied,final_amount,created_at"

    transaction_id: str
    store_id: int
    user_id: Optional[int]
    original_amount: float
    final_amount: float
    created_at: datetime

    @classmethod
    def from_bytes_and_project(cls, data: bytes) -> "Transaction":
        """
        Check superclass documentation
        Expected format:
        transaction_id,store_id,payment_method_id,voucher_id,user_id,original_amount,discount_applied,final_amount,created_at
        Example:
        b"86652dc6-f350-4aeb-8e41-ce3a94d27825,10,3,,13060.0,9.5,0.0,9.5,2025-01-01 12:52:56"
        """
        line = data.decode("utf-8").strip()
        fields = line.split(",")

        transaction_id = fields[0].strip()
        store_id = int(fields[1])
        user_id = int(fields[4]) if fields[4] else None
        original_amount = float(fields[5])
        final_amount = float(fields[7])
        created_at = datetime.strptime(fields[8], "%Y-%m-%d %H:%M:%S")

        return cls(
            transaction_id=transaction_id,
            store_id=store_id,
            user_id=user_id,
            original_amount=original_amount,
            final_amount=final_amount,
            created_at=created_at,
        )

    @classmethod
    def from_bytes(cls, data: bytes) -> "Transaction":
        """
        Check superclass documentation
        Expected format:
        transaction_id,store_id,user_id,original_amount,final_amount,created_at
        Example:
        b"86652dc6-f350-4aeb-8e41-ce3a94d27825,10,13060.0,9.5,9.5,2025-01-01 12:52:56"
        """
        line = data.decode("utf-8").strip()
        fields = line.split(",")

        transaction_id = fields[0].strip()
        store_id = int(fields[1])
        user_id = int(fields[2]) if fields[2] else None
        original_amount = float(fields[3])
        final_amount = float(fields[4])
        created_at = datetime.strptime(fields[5], "%Y-%m-%d %H:%M:%S")

        return cls(
            transaction_id=transaction_id,
            store_id=store_id,
            user_id=user_id,
            original_amount=original_amount,
            final_amount=final_amount,
            created_at=created_at,
        )

    def __str__(self) -> str:
        return f"{self.transaction_id},{self.store_id},{self.user_id},{self.original_amount},{self.final_amount},{self.created_at}"
