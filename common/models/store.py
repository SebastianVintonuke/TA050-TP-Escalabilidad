from dataclasses import dataclass
from typing import Optional
from .model import Model
from typing import ClassVar

@dataclass
class Store(Model):
    _HEADER: ClassVar[str] = "store_id,store_name,street,postal_code,city,state,latitude,longitude"

    store_id: Optional[int]
    store_name: Optional[str]
    state: Optional[str]
    city: Optional[str]

    @classmethod
    def from_bytes(cls, data: bytes) -> "Store":
        """
        Receive a CSV line as bytes and returns a Store
        Expected format:
        store_id,store_name,address,postal_code,state,city,latitude,longitude
        Example:
        b"1,G Coffee @ USJ 89q,Jalan Dewan Bahasa 5/9,50998,USJ 89q,Kuala Lumpur,3.117134,101.615027"
        """
        line = data.decode("utf-8").strip()
        fields = line.split(",")

        store_id = int(fields[0]) if fields[0] else None
        store_name = fields[1].strip() if len(fields) > 1 and fields[1] else None
        state = fields[4].strip() if len(fields) > 4 and fields[4] else None
        city = fields[5].strip() if len(fields) > 5 and fields[5] else None

        return cls(
            store_id=store_id,
            store_name=store_name,
            state=state,
            city=city,
        )
