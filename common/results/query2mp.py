from datetime import datetime, date
from dataclasses import dataclass

from common.results.query import QueryResult


@dataclass
class QueryResult2MostProfit(QueryResult):
    year_month_created_at: date
    item_id: int
    profit_sum: float
    item_name: str

    @classmethod
    def from_bytes(cls, data: bytes) -> "QueryResult2MostProfit":
        line = data.decode("utf-8").strip()
        fields = line.split(",")

        year_month_created_at = datetime.strptime(fields[0], "%Y-%m").date()
        item_id = int(fields[1])
        profit_sum = float(fields[2])
        item_name = fields[3].strip()

        return cls(year_month_created_at=year_month_created_at, item_id=item_id, profit_sum=profit_sum, item_name=item_name)

    def __str__(self) -> str:
        return f"{self.year_month_created_at.strftime('%Y-%m')},{self.item_id},{self.profit_sum},{self.item_name}"