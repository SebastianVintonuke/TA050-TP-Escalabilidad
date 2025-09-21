from datetime import datetime, date
from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import Type, ClassVar, TypeVar

T = TypeVar("T", bound="QueryResult")

class QueryResult(ABC):
    @classmethod
    @abstractmethod
    def from_bytes(cls: Type[T], data: bytes) -> T:
        """
        Convierte una línea CSV en bytes en una instancia del modelo
        Debe ser implementado por cada subclase
        """
        pass

    @abstractmethod
    def to_bytes(self) -> bytes:
        """
        Convierte una instancia del modelo en una línea de CSV en bytes
        Debe ser implementado por cada subclase
        """
        pass

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

    def to_bytes(self) -> bytes:
        return f"{self.transaction_id},{self.final_amount}".encode("utf-8")

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

    def to_bytes(self) -> bytes:
        return f"{self.year_month_created_at.strftime('%Y-%m')},{self.item_id},{self.sellings_qty},{self.item_name}".encode("utf-8")

@dataclass
class QueryResult2MostProfit(QueryResult):
    year_month_created_at: date
    item_id: int
    profit_sum: int
    item_name: str

    @classmethod
    def from_bytes(cls, data: bytes) -> "QueryResult2MostProfit":
        line = data.decode("utf-8").strip()
        fields = line.split(",")

        year_month_created_at = datetime.strptime(fields[0], "%Y-%m").date()
        item_id = int(fields[1])
        profit_sum = int(fields[2])
        item_name = fields[3].strip()

        return cls(year_month_created_at=year_month_created_at, item_id=item_id, profit_sum=profit_sum, item_name=item_name)

    def to_bytes(self) -> bytes:
        return f"{self.year_month_created_at.strftime('%Y-%m')},{self.item_id},{self.profit_sum},{self.item_name}".encode("utf-8")

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

    def to_bytes(self) -> bytes:
        return f"{self.year_created_at.strftime('%Y')}-{self.half_created_at},{self.store_id},{self.tpv},{self.store_name}".encode("utf-8")

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

    def to_bytes(self) -> bytes:
        return f"{self.store_id},{self.user_id},{self.purchases_qty},{self.store_name},{self.birthdate.strftime('%Y-%m-%d')}".encode("utf-8")