from datetime import datetime, date
from dataclasses import dataclass

from common.results.query import QueryResult


@dataclass
class QueryResult2MostProfit(QueryResult):
    year_month_created_at: date
    item_name: str
    profit_sum: float

    @classmethod
    def from_bytes(cls, data: bytes) -> "QueryResult2MostProfit":
        line = data.decode("utf-8").strip()
        fields = line.split(",")

        year_month_created_at = datetime.strptime(fields[0], "%Y-%m").date()
        profit_sum = float(fields[1])
        item_name = fields[2].strip()

        return cls(year_month_created_at=year_month_created_at, profit_sum=profit_sum, item_name=item_name)

    def __str__(self) -> str:
        return f"{self.year_month_created_at.strftime('%Y-%m')},{self.profit_sum},{self.item_name}"