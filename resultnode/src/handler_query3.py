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
    def handle_new_results(msg, user_id: str, packet_id) -> bytes:

        data: List[QueryResult3] = []
        for line in msg.stream_rows():
            #logging.info(f"Q_3 {line}")
            store_name = line[0]
            year_created_at, half_created_at = year_semester_decode(line[1])
            tpv = float(line[2])
            data.append(QueryResult3(year_created_at=year_created_at, half_created_at=half_created_at, store_name=store_name, tpv=tpv))
        return ResultTask(user_id, QueryId.Query3, False, False, data, packet_id).to_bytes()

    def get_eof_msg(user_id, packet_id):
        return ResultTask(user_id, QueryId.Query3, True, False, [], packet_id).to_bytes()

    def get_abort_msg(user_id):
        return ResultTask(user_id, QueryId.Query3, True, True, []).to_bytes()
