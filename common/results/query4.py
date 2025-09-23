from datetime import datetime, date
from dataclasses import dataclass

from common.results.query import QueryResult


@dataclass
class QueryResult4(QueryResult):
    store_id: int
    user_id: int
    purchases_qty: int
    store_name: str
    birthdate: date

    @classmethod
    def from_bytes(cls, data: bytes) -> "QueryResult4":
        line = data.decode("utf-8").strip()
        fields = line.split(",")

        store_id = int(fields[0])
        user_id = int(fields[1])
        purchases_qty = int(fields[2])
        store_name = fields[3].strip()
        birthdate = datetime.strptime(fields[4], "%Y-%m-%d").date()

        return cls(store_id=store_id, user_id=user_id, purchases_qty=purchases_qty, store_name=store_name, birthdate=birthdate)

    def __str__(self) -> str:
        return f"{self.store_id},{self.user_id},{self.purchases_qty},{self.store_name},{self.birthdate.strftime('%Y-%m-%d')}"