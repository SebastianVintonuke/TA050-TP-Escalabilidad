from common.middleware.tasks.result import ResultTask
from common.results.query1 import QueryResult1
from common import QueryId
import logging


class HandlerQuery1:
    def handle_new_results(msg, user_id: str, packet_id) -> bytes:
        data: List[QueryResult1] = []
        for line in msg.stream_rows():
            transaction_id = line[0]
            final_amount = float(line[1])
            data.append(QueryResult1(transaction_id=transaction_id, final_amount=final_amount))
        return ResultTask(user_id, QueryId.Query1, False, False, data, packet_id).to_bytes()

    def get_eof_msg(user_id, packet_id):
        return ResultTask(user_id, QueryId.Query1, True, False, [], packet_id).to_bytes()

    def get_abort_msg(user_id):
        return ResultTask(user_id, QueryId.Query1, True, True, []).to_bytes()