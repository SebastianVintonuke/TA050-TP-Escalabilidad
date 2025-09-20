from dataclasses import dataclass
from datetime import datetime, date
from typing import Optional
from .model import Model
from typing import ClassVar

@dataclass
class User(Model):
    _HEADER: ClassVar[str] = "user_id,gender,birthdate,registered_at"

    user_id: Optional[int]
    birthdate: Optional[date]
    registered_at: Optional[datetime]

    @classmethod
    def from_bytes(cls, data: bytes) -> "User":
        """
        Receive a CSV line as bytes and returns a User
        Expected format:
        user_id,gender,birthdate,registered_at
        ej: b"1127889,male,1971-03-18,2025-01-01 07:31:48"
        """
        line = data.decode("utf-8").strip()
        fields = line.split(",")

        user_id = int(fields[0]) if fields[0] else None
        birthdate = datetime.strptime(fields[2], "%Y-%m-%d").date() if fields[2] else None
        registered_at = datetime.strptime(fields[3], "%Y-%m-%d %H:%M:%S") if fields[3] else None

        return cls(user_id=user_id, birthdate=birthdate, registered_at=registered_at)