from common.middleware.tasks.result import ResultTask
from common.results.query1 import QueryResult1
from common import QueryId
import logging


class HandlerQuery1:
    def handle_new_results(headers, msg,counter, user_id: str) -> bytes:
        if headers.is_eof():
            counter.expected_count_query_1 = headers.msg_count
            logging.info(f"Received expected message count for query 1, expect {counter.expected_count_query_1} got: {counter.count_query_1}")
            return None
        counter.count_query_1 += 1

        data: List[QueryResult1] = []
        for line in msg.stream_rows():
            transaction_id = line[0]
            final_amount = float(line[1])
            data.append(QueryResult1(transaction_id=transaction_id, final_amount=final_amount))
        return ResultTask(user_id, QueryId.Query1, False, False, data).to_bytes()


    def check_send_eof(counter, user_id, middleware):
        if counter.is_eof_q1():
            logging.info(f"Received last message for query 1 count: {counter.count_query_1} expected_count: {counter.expected_count_query_1}")
            middleware.send(ResultTask(user_id, QueryId.Query1, True, False, []).to_bytes())
