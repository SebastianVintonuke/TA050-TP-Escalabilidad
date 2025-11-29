from common.middleware.tasks.result import ResultTask
from common import QueryId

from datetime import datetime, date
from common.results.query4 import QueryResult4
import logging


class HandlerQuery4:
    def handle_new_results(msg, user_id: str) -> bytes:


        data: List[QueryResult4] = []
        for line in msg.stream_rows():
            #logging.info(f"Q_4 {line}")
            store_name: str = line[0]
            birthdate: date = datetime.strptime(line[1], "%Y-%m-%d").date()
            data.append(QueryResult4(store_name=store_name, birthdate=birthdate))
        return ResultTask(user_id, QueryId.Query4, False, False, data).to_bytes()

    def get_eof_msg(user_id):
        return ResultTask(user_id, QueryId.Query4, True, False, []).to_bytes()

    def get_abort_msg(user_id):
        return ResultTask(user_id, QueryId.Query4, True, True, []).to_bytes()
