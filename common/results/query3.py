from datetime import datetime, date
from dataclasses import dataclass

from common.results.query import QueryResult


@dataclass
class QueryResult3(QueryResult):
    year_created_at: date
    half_created_at: str
    store_id: int
    tpv: float
    store_name: str

    @classmethod
    def from_bytes(cls, data: bytes) -> "QueryResult3":
        line = data.decode("utf-8").strip()
        fields = line.split(",")

        year_str, half_created_at = fields[0].split("-")
        year_created_at = datetime.strptime(year_str, "%Y").date()
        store_id = int(fields[1])
        tpv = float(fields[2])
        store_name = fields[3].strip()

        return cls(year_created_at=year_created_at, half_created_at=half_created_at, store_id=store_id, tpv=tpv, store_name=store_name)

    def __str__(self) -> str:
        return f"{self.year_created_at.strftime('%Y')}-{self.half_created_at},{self.store_id},{self.tpv},{self.store_name}"