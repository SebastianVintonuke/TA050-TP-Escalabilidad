from datetime import datetime, date
from dataclasses import dataclass
from enum import Enum

from common.results.query import QueryResult

class HalfCreatedAt(str, Enum):
    H1 = "H1"
    H2 = "H2"

@dataclass
class QueryResult3(QueryResult):
    year_created_at: date
    half_created_at: HalfCreatedAt
    store_name: str
    tpv: float

    @classmethod
    def from_bytes(cls, data: bytes) -> "QueryResult3":
        line = data.decode("utf-8").strip()
        fields = line.split(",")

        year_str, half_created_at_str = fields[0].split("-")
        half_created_at = HalfCreatedAt(half_created_at_str)
        year_created_at = datetime.strptime(year_str, "%Y").date()
        store_name = fields[1].strip()
        tpv = float(fields[2])

        return cls(year_created_at=year_created_at, half_created_at=half_created_at, tpv=tpv, store_name=store_name)

    def __str__(self) -> str:
        return f"{self.year_created_at.strftime('%Y')}-{self.half_created_at},{self.tpv},{self.store_name}"