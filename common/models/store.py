from dataclasses import dataclass
from .model import Model
from typing import ClassVar

@dataclass
class Store(Model):
    _ORIGINAL_HEADER: ClassVar[str] = "store_id,store_name,street,postal_code,city,state,latitude,longitude"

    store_id: int
    store_name: str
    state: str
    city: str


    def parse_row(data: bytes):
        fields = data.strip().split(b",")
        return [
            fields[0], #Store id
            fields[1], #Store name
        ]


    @classmethod
    def from_bytes_and_project(cls, data: bytes) -> "Store":
        """
        Check superclass documentation
        Expected format:
        store_id,store_name,address,postal_code,state,city,latitude,longitude
        Example:
        b"1,G Coffee @ USJ 89q,Jalan Dewan Bahasa 5/9,50998,USJ 89q,Kuala Lumpur,3.117134,101.615027"
        """
        line = data.decode("utf-8").strip()
        fields = line.split(",")

        store_id = int(fields[0])
        store_name = fields[1].strip()
        state = fields[4].strip()
        city = fields[5].strip()

        return cls(
            store_id=store_id,
            store_name=store_name,
            state=state,
            city=city,
        )

    @classmethod
    def from_bytes(cls, data: bytes) -> "Store":
        """
        Check superclass documentation
        Expected format:
        store_id,store_name,state,city
        Example:
        b"1,G Coffee @ USJ 89q,USJ 89q,Kuala Lumpur"
        """
        line = data.decode("utf-8").strip()
        fields = line.split(",")

        store_id = int(fields[0])
        store_name = fields[1].strip()
        state = fields[2].strip()
        city = fields[3].strip()

        return cls(
            store_id=store_id,
            store_name=store_name,
            state=state,
            city=city,
        )

    def __str__(self) -> str:
        return f"{self.store_id},{self.store_name},{self.state},{self.city}"