import shutil
import threading
from io import BufferedReader
from pathlib import Path
from typing import Callable, Dict, List, Sequence, Tuple, TypedDict

from common import QueryId
from common.results.query import QueryResult


def file_name_for(query_id: QueryId) -> str:
    for q, name in FILE_NAMES:
        if q == query_id:
            return name
    raise Exception(f"Unknown query id: {query_id}")


FILE_NAMES: List[Tuple[QueryId, str]] = [
    (QueryId.Query1, "query_1.csv"),
    (QueryId.Query2BestSelling, "query_2_best_selling.csv"),
    (QueryId.Query2MostProfit, "query_2_most_profits.csv"),
    (QueryId.Query3, "query_3.csv"),
    (QueryId.Query4, "query_4.csv"),
]


class QueryState(TypedDict):
    lock: threading.Lock
    ready: bool


class UserResult:
    def __init__(self, base_dir: Path):
        self._base_dir = base_dir
        self._query_states: Dict[QueryId, QueryState] = {
            query_id: {
                "lock": threading.Lock(),
                "ready": False,
            }
            for query_id, _ in FILE_NAMES
        }
        self._all_queries_are_ready = threading.Condition()
        self._base_dir.mkdir(exist_ok=True)
        for query_id, file_name in FILE_NAMES:
            path = self._base_dir / file_name
            ready_path = self._base_dir / f"ready_{file_name}"
            if not path.exists() and not ready_path.exists():
                path.touch()
            if ready_path.exists():
                self._query_states[query_id]["ready"] = True

    def append(self, query_id: QueryId, results: Sequence[QueryResult]) -> None:
        if not results:
            return
        file_path = self._base_dir / file_name_for(query_id)
        with self._query_states[query_id]["lock"]:
            with file_path.open("ab") as f:
                for r in results:
                    line = r.to_bytes() + b"\n"
                    f.write(line)

    def mark_ready(self, query_id: QueryId) -> None:
        file_name = file_name_for(query_id)
        src = self._base_dir / file_name
        dst = self._base_dir / f"ready_{file_name}"
        with self._query_states[query_id]["lock"]:
            if dst.exists():
                return # 2 EOF
            src.rename(dst)
        with self._all_queries_are_ready:
            self._query_states[query_id]["ready"] = True
            self._all_queries_are_ready.notify_all()

    def do_with_each_result_file_when_ready(
        self, a_closure: Callable[[BufferedReader], None]
    ) -> None:
        with self._all_queries_are_ready:
            while not all(
                query_state["ready"] for query_state in self._query_states.values()
            ):
                self._all_queries_are_ready.wait()

        for query_id, file_name in FILE_NAMES:
            ready_path = self._base_dir / f"ready_{file_name}"
            state = self._query_states[query_id]
            with state["lock"]:
                with ready_path.open("rb") as reader:
                    a_closure(reader)

    def delete(self) -> None:
        shutil.rmtree(self._base_dir)
