from typing import List, ClassVar, Type, Union

from common import QueryId

ValidModelForSelection = Type[Union["Transaction", "TransactionItem"]]

class SelectTask:
    TASK_ID: ClassVar[str] = "SELECT"

    def __init__(self, user_id: str, query_id: QueryId, eof: bool, abort: bool, output: str, data: List[ValidModelForSelection]):
        self.user_id = user_id
        self.query_id = query_id
        self.eof = eof
        self.abort = abort
        self.output = output
        self.data = data

    @classmethod
    def from_bytes(cls, model: ValidModelForSelection, data: bytes) -> "SelectTask":
        decoded = data.decode("utf-8").strip()
        lines = decoded.split("\n")

        header = lines[0]
        user_id, query_id_str, task_id, eof_str, abort_str, output = header.split("|")

        if task_id != cls.TASK_ID:
            raise ValueError(f"Invalid task_id: {task_id}, expected {cls.TASK_ID}")

        query_id = QueryId(query_id_str)
        eof = eof_str.lower() == "true"
        abort = abort_str.lower() == "true"

        body = lines[1:]
        models = []
        for line in body:
            if line.strip():
                models.append(model.from_bytes(line.encode("utf-8")))

        return cls(user_id, query_id, eof, abort, output, models)

    def to_bytes(self) -> bytes:
        return self.__str__().encode("utf-8")

    def __str__(self) -> str:
        header = f"{self.user_id}|{self.query_id}|{self.TASK_ID}|{self.eof}|{self.abort}|{self.output}"
        body = "\n".join(str(model) for model in self.data)
        return f"{header}\n{body}"