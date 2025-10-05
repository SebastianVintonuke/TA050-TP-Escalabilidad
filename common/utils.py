import hashlib
import uuid
from enum import Enum
from typing import Type

from common.results.query import QueryResult
from common.results.query1 import QueryResult1
from common.results.query2bs import QueryResult2BestSelling
from common.results.query2mp import QueryResult2MostProfit
from common.results.query3 import QueryResult3
from common.results.query4 import QueryResult4


def new_uuid() -> str:
    return str(uuid.uuid4())

class QueryId(str, Enum):
    Query1 = "1"
    Query2BestSelling = "2BS"
    Query2MostProfit = "2MP"
    Query3 = "3"
    Query4 = "4"

def query_id_from(string: str) -> QueryId:
    try:
        return QueryId(string)
    except ValueError:
        raise ValueError(f"Invalid query id {string}")

def query_result_for(query_id: QueryId) -> Type[QueryResult]:
    if query_id == QueryId.Query1:
        return QueryResult1
    elif query_id == QueryId.Query2BestSelling:
        return QueryResult2BestSelling
    elif query_id == QueryId.Query2MostProfit:
        return QueryResult2MostProfit
    elif query_id == QueryId.Query3:
        return QueryResult3
    elif query_id == QueryId.Query4:
        return QueryResult4
    else:
        raise ValueError(f"No QueryResult found for query_id: {query_id}")

def stable_hash(value: str) -> int:
    return int(hashlib.md5(value.encode("utf-8")).hexdigest(), 16)
