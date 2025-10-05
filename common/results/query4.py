from datetime import datetime, date
from dataclasses import dataclass

from common.results.query import QueryResult


@dataclass
class QueryResult4(QueryResult):
    store_name: str
    birthdate: date

    @classmethod
    def from_bytes(cls, data: bytes) -> "QueryResult4":
        line = data.decode("utf-8").strip()
        fields = line.split(",")

        store_name = fields[0].strip()
        birthdate = datetime.strptime(fields[1], "%Y-%m-%d").date()

        return cls(store_name=store_name, birthdate=birthdate)

    def __str__(self) -> str:
        return f"{self.store_name},{self.birthdate.strftime('%Y-%m-%d')}"