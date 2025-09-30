from datetime import datetime, date
from dataclasses import dataclass

from common.results.query import QueryResult


@dataclass
class QueryResult2BestSelling(QueryResult):
    year_month_created_at: date
    item_name: str
    sellings_qty: int

    @classmethod
    def from_bytes(cls, data: bytes) -> "QueryResult2BestSelling":
        line = data.decode("utf-8").strip()
        fields = line.split(",")

        year_month_created_at = datetime.strptime(fields[0], "%Y-%m").date()
        item_name = fields[1].strip()
        sellings_qty = int(fields[2])

        return cls(year_month_created_at=year_month_created_at, item_name=item_name, sellings_qty=sellings_qty)

    def __str__(self) -> str:
        return f"{self.year_month_created_at.strftime('%Y-%m')},{self.item_name},{self.sellings_qty}"