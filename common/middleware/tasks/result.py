from typing import ClassVar, Sequence

from common import QueryId
from common.results.query import QueryResult
from common.utils import query_result_for


class ResultTask:
    TASK_ID: ClassVar[str] = "RESULT"

    def __init__(self, user_id: str, query_id: QueryId, eof: bool, abort: bool, data: Sequence[QueryResult]):
        self.user_id = user_id
        self.query_id = query_id
        self.eof = eof
        self.abort = abort
        self.data = data

    @classmethod
    def from_bytes(cls, data: bytes) -> "ResultTask":
        decoded = data.decode("utf-8").strip()
        lines = decoded.split("\n")

        header = lines[0]
        user_id, query_id_str, task_id, eof_str, abort_str = header.split("|")

        if task_id != cls.TASK_ID:
            raise ValueError(f"Invalid task_id: {task_id}, expected {cls.TASK_ID}")

        query_id = QueryId(query_id_str)
        eof = eof_str.lower() == "true"
        abort = abort_str.lower() == "true"

        query_result = query_result_for(query_id)

        body = lines[1:]
        query_results = []
        for line in body:
            if line.strip():
                query_results.append(query_result.from_bytes(line.encode("utf-8")))

        return cls(user_id, query_id, eof, abort, query_results)

    def to_bytes(self) -> bytes:
        return self.__str__().encode("utf-8")

    def __str__(self) -> str:
        header = f"{self.user_id}|{self.query_id}|{self.TASK_ID}|{self.eof}|{self.abort}"
        body = "\n".join(str(query_result) for query_result in self.data)
        return f"{header}\n{body}"