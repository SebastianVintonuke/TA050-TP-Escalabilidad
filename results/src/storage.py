import logging
import threading
from io import BufferedReader
from pathlib import Path
from typing import Callable, Dict

from common.middleware.tasks.result import ResultTask
from results.src.UserResults import UserResult


class ResultStorage:
    def __init__(self, dir_path: str):
        self._dir_path = Path(f"results/{dir_path}")
        self._dir_path.mkdir(parents=True, exist_ok=True)
        self._users: Dict[str, UserResult] = self.__load_users()
        self._users_lock = threading.Lock()

    def handle(self, result_task: ResultTask) -> None:
        logging.info(
            f"action: handle_task | result: in-progress | {result_task.user_id}:{result_task.query_id}"
        )

        if result_task.abort:
            self.__delete_results(result_task.user_id)
            return

        self.__get_or_create_user(result_task.user_id).append(
            result_task.query_id, result_task.data
        )
        if result_task.eof:
            self.__get_or_create_user(result_task.user_id).mark_ready(
                result_task.query_id
            )

    def do_with_results_when_ready(
        self, user_id: str, a_closure: Callable[[BufferedReader], None]
    ) -> None:
        self._users[user_id].do_with_each_result_file_when_ready(a_closure)

    def assert_exists(self, user_id: str) -> None:
        with self._users_lock:
            if user_id not in self._users:
                raise Exception(
                    f"Does not exist user_id: '{user_id}' in the result storage"
                )

    def __get_or_create_user(self, user_id: str) -> UserResult:
        with self._users_lock:
            if user_id not in self._users:
                user_dir = self._dir_path / user_id
                self._users[user_id] = UserResult(user_dir)
            return self._users[user_id]

    def __delete_results(self, user_id: str) -> None:
        with self._users_lock:
            user = self._users.pop(user_id, None)
            if user:
                user.delete()

    def __load_users(self) -> Dict[str, UserResult]:
        users = {}
        for user_dir in self._dir_path.iterdir():
            if user_dir.is_dir():
                users[user_dir.name] = UserResult(user_dir)
        return users
