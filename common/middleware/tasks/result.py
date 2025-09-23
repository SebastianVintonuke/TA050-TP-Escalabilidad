from typing import List, ClassVar, Type, Union

from common import QueryId

ValidModelForSelection = Type[Union["Transaction", "TransactionItem"]]

class ResultTask:
    TASK_ID: ClassVar[str] = "SELECT"