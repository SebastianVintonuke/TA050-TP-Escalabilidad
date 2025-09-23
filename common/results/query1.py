from dataclasses import dataclass

from common.results.query import QueryResult


@dataclass
class QueryResult1(QueryResult):
    transaction_id: str
    final_amount: float

    @classmethod
    def from_bytes(cls, data: bytes) -> "QueryResult1":
        line = data.decode("utf-8").strip()
        fields = line.split(",")

        transaction_id = fields[0].strip()
        final_amount = float(fields[1])

        return cls(transaction_id=transaction_id, final_amount=final_amount)

    def __str__(self) -> str:
        return f"{self.transaction_id},{self.final_amount}"