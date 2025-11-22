from common.middleware.tasks.result import ResultTask
from common import QueryId

from datetime import datetime, date
from common.results.query3 import QueryResult3, HalfCreatedAt
from typing import List, Tuple
import logging




def year_semester_decode(year_semester_str: str) -> Tuple[date, HalfCreatedAt]:
    year_semester = int(year_semester_str)
    year_str = str((year_semester * 6 // 12) + 2024)
    year = datetime.strptime(str(year_str), "%Y").date()
    if year_semester % 2 == 0:
        semester = HalfCreatedAt.H1
    else:
        semester = HalfCreatedAt.H2
    return year, semester

class HandlerQuery3:
    def handle_new_results(headers, msg,counter, user_id: str) -> bytes:
        if headers.is_eof():
            counter.expected_count_query_3 = headers.msg_count
            logging.info(f"Received expected message count for query 3, expect {counter.expected_count_query_3} got: {counter.count_query_3}")
            return
        counter.count_query_3 += 1

        data: List[QueryResult3] = []
        for line in msg.stream_rows():
            #logging.info(f"Q_3 {line}")
            store_name = line[0]
            year_created_at, half_created_at = year_semester_decode(line[1])
            tpv = float(line[2])
            data.append(QueryResult3(year_created_at=year_created_at, half_created_at=half_created_at, store_name=store_name, tpv=tpv))
        return ResultTask(user_id, QueryId.Query3, False, False, data).to_bytes()

    def check_send_eof(counter, user_id, middleware):
        if counter.is_eof_q3():
            logging.info(f"Received last message for query 3 count: {counter.count_query_3} expected_count: {counter.expected_count_query_3}")
            result_task = ResultTask(user_id, QueryId.Query3, True, False, []).to_bytes()
            middleware.send(result_task)

    def send_abort(user_id, middleware):
        middleware.send(ResultTask(user_id, QueryId.Query3, True, True, []).to_bytes())
