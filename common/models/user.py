from dataclasses import dataclass
from datetime import datetime, date
from .model import Model
from typing import ClassVar

@dataclass
class User(Model):
    _ORIGINAL_HEADER: ClassVar[str] = "user_id,gender,birthdate,registered_at"

    user_id: int
    birthdate: date
    registered_at: datetime

    @classmethod
    def from_bytes_and_project(cls, data: bytes) -> "User":
        """
        Check superclass documentation
        Expected format:
        user_id,gender,birthdate,registered_at
        ej: b"1127889,male,1971-03-18,2025-01-01 07:31:48"
        """
        line = data.decode("utf-8").strip()
        fields = line.split(",")

        user_id = int(fields[0])
        birthdate = datetime.strptime(fields[2], "%Y-%m-%d").date()
        registered_at = datetime.strptime(fields[3], "%Y-%m-%d %H:%M:%S")

        return cls(user_id=user_id, birthdate=birthdate, registered_at=registered_at)

    @classmethod
    def from_bytes(cls, data: bytes) -> "User":
        """
        Check superclass documentation
        Expected format:
        user_id,birthdate,registered_at
        ej: b"1127889,1971-03-18,2025-01-01 07:31:48"
        """
        line = data.decode("utf-8").strip()
        fields = line.split(",")

        user_id = int(fields[0])
        birthdate = datetime.strptime(fields[1], "%Y-%m-%d").date()
        registered_at = datetime.strptime(fields[2], "%Y-%m-%d %H:%M:%S")

        return cls(user_id=user_id, birthdate=birthdate, registered_at=registered_at)

    def __str__(self) -> str:
        return f"{self.user_id},{self.birthdate},{self.registered_at}"