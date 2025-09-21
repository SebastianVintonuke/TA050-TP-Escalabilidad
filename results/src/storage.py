import threading
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

from common.models.queryresult import QueryResult
from common.protocol.results import QueryId


class ResultStorage:
    _FILE_NAMES: List[Tuple[QueryId, str]] = [
        (QueryId.Query1, "query_1.csv"),
        (QueryId.Query2BestSelling, "query_2_best_selling.csv"),
        (QueryId.Query2MostProfit, "query_2_most_profits.csv"),
        (QueryId.Query3, "query_3.csv"),
        (QueryId.Query4, "query_4.csv"),
    ]

    def __init__(self, dir_path: str):
        self._dir_path = Path(f"results/{dir_path}")
        self._dir_path.mkdir(parents=True, exist_ok=True)
        self._results: Dict[str, Dict[QueryId, threading.Lock]] = (
            self.__load_results_from(self._dir_path)
        )
        self._users_lock = threading.Lock()

    def append_results(
        self, user_id: str, query_id: QueryId, partial_results: Sequence[QueryResult]
    ) -> None:
        self.__if_absent_create_user_for(user_id)

        file_path = self._dir_path / user_id / self.__file_name_for(query_id)
        with self._results[user_id][QueryId.Query4]:
            with file_path.open("a", encoding="utf-8") as file_descriptor:
                for partial_result in partial_results:
                    line = partial_result.to_bytes().decode("utf-8")
                    if not line.endswith("\n"):
                        line += "\n"
                    file_descriptor.write(line)

    def notify_eof_results(self, user_id: str, query_id: QueryId) -> None:
        file_path = self._dir_path / user_id / self.__file_name_for(query_id)
        ready_file_path = (
            self._dir_path / user_id / f"ready_{self.__file_name_for(query_id)}"
        )
        with self._results[user_id][QueryId.Query4]:
            file_path.rename(ready_file_path)

    def results_are_ready(self, user_id: str) -> bool:
        with self._users_lock:
            if user_id not in self._results:
                return False
        user_dir = self._dir_path / user_id
        for file_name in self._FILE_NAMES:
            ready_file = user_dir / f"ready_{file_name}"
            if not ready_file.exists():
                return False
        return True

    def __if_absent_create_user_for(self, user_id: str) -> None:
        with self._users_lock:
            if user_id in self._results:
                return
            user_dir = self._dir_path / user_id
            user_dir.mkdir()
            self._results[user_id] = {}
            for query_id, file_name in self._FILE_NAMES:
                (user_dir / file_name).touch()
                self._results[user_id][query_id] = threading.Lock()

    def __file_name_for(self, query_id: QueryId) -> str:
        for query_id, file_name in self._FILE_NAMES:
            if query_id == query_id:
                return file_name
        raise Exception(f"Unknown query id: {query_id}")

    def __load_results_from(
        self, dir_path: Path
    ) -> Dict[str, Dict[QueryId, threading.Lock]]:
        results = {}
        for f in dir_path.iterdir():
            print(f.name)
        for user_dir in dir_path.iterdir():
            if user_dir.is_dir():
                results[user_dir.name] = self.__load_user_from(user_dir)
        return results

    def __load_user_from(self, user_dir_path: Path) -> Dict[QueryId, threading.Lock]:
        result: Dict[QueryId, threading.Lock] = {}
        for query_id, file_name in self._FILE_NAMES:
            not_ready = user_dir_path / file_name
            ready = user_dir_path / f"ready_{file_name}"
            if not_ready.exists() and not_ready.is_file():
                result[query_id] = threading.Lock()
            elif ready.exists() and ready.is_file():
                result[query_id] = threading.Lock()
            else:
                raise FileNotFoundError(
                    f"Not founded {file_name} or ready_{file_name} in {user_dir_path}"
                )
        return result
