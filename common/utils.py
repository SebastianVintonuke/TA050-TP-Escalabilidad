import uuid
from enum import Enum

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