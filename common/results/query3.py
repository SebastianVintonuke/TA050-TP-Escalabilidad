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

        year_created_at = datetime.strptime(fields[0], "%Y").date()
        half_created_at = fields[1].strip()
        store_id = int(fields[2])
        tpv = float(fields[3])
        store_name = fields[4].strip()

        return cls(year_created_at=year_created_at, half_created_at=half_created_at, store_id=store_id, tpv=tpv, store_name=store_name)

    def __str__(self) -> str:
        return f"{self.year_created_at.strftime('%Y')}-{self.half_created_at},{self.store_id},{self.tpv},{self.store_name}"