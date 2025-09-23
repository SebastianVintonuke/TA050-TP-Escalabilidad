from datetime import datetime, date
from dataclasses import dataclass

from common.results.query import QueryResult


@dataclass
class QueryResult2BestSelling(QueryResult):
    year_month_created_at: date
    item_id: int
    sellings_qty: int
    item_name: str

    @classmethod
    def from_bytes(cls, data: bytes) -> "QueryResult2BestSelling":
        line = data.decode("utf-8").strip()
        fields = line.split(",")

        year_month_created_at = datetime.strptime(fields[0], "%Y-%m").date()
        item_id = int(fields[1])
        sellings_qty = int(fields[2])
        item_name = fields[3].strip()

        return cls(year_month_created_at=year_month_created_at, item_id=item_id, sellings_qty=sellings_qty, item_name=item_name)

    def __str__(self) -> str:
        return f"{self.year_month_created_at.strftime('%Y-%m')},{self.item_id},{self.sellings_qty},{self.item_name}"