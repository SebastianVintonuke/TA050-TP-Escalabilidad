from common.middleware.tasks.result import ResultTask
from common import QueryId

from datetime import datetime, date
from common.results.query4 import QueryResult4
import logging


class HandlerQuery4:
    def handle_new_results(headers, msg,counter, user_id: str) -> bytes:
        if headers.is_eof():
            counter.expected_count_query_4 = headers.msg_count
            logging.info(f"Received expected message count for query 4, expect {counter.expected_count_query_4} got: {counter.count_query_4}")            
            return
        counter.count_query_4 += 1

        data: List[QueryResult4] = []
        for line in msg.stream_rows():
            #logging.info(f"Q_4 {line}")
            store_name: str = line[0]
            birthdate: date = datetime.strptime(line[1], "%Y-%m-%d").date()
            data.append(QueryResult4(store_name=store_name, birthdate=birthdate))
        return ResultTask(user_id, QueryId.Query4, False, False, data).to_bytes()

    def check_send_eof(counter, user_id, middleware):
        if counter.is_eof_q4():
            logging.info(f"Received last message for query 4 count: {counter.count_query_4} expected_count: {counter.expected_count_query_4}")
            result_task = ResultTask(user_id, QueryId.Query4, True, False, []).to_bytes()
            middleware.send(result_task)

    def send_abort(user_id, middleware):
        middleware.send(ResultTask(user_id, QueryId.Query4, True, True, []).to_bytes())
