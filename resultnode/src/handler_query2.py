from common.middleware.tasks.result import ResultTask
from common import QueryId
from common.results.query2mp import QueryResult2MostProfit
from common.results.query2bs import QueryResult2BestSelling

from datetime import datetime, date


class HandlerQuery2BestSelling:
    def handle_new_results(headers, msg, counter, user_id: str) -> ResultTask:
        if headers.is_eof():

            counter.expected_count_query_2_quantity = headers.msg_count
            logging.info(f"Received expected message count for query 2 quantity, expect {counter.expected_count_query_2_quantity} got: {counter.count_query_2_quantity}")
            return None
        counter.count_query_2_quantity += 1

        data: List[QueryResult2BestSelling] = []
        for line in msg.stream_rows():
            #logging.info(f"Q_2_QUAN {line}")
            item_name: str = line[0]
            month_encoded = int(line[1])
            year = (month_encoded - 1) // 12 + 2024
            month = (month_encoded-1) % 12 + 1
            year_month_created_at: date = datetime.strptime(f"{year}-{month}", "%Y-%m").date()
            sellings_qty: int = int(float(line[2]))
            #logging.info(f"type: {msg.types[0]}: {year_month_created_at}, {item_name}, {sellings_qty}")
            data.append(QueryResult2BestSelling(year_month_created_at=year_month_created_at, item_name=item_name, sellings_qty=sellings_qty))
        return ResultTask(user_id, QueryId.Query2BestSelling, False, False, data).to_bytes()

    def check_send_eof(counter, user_id, middleware):
        if counter.is_eof_q2_quantity():
            logging.info(f"Received last message for query 2 best selling count: {counter.count_query_2_quantity} expected_count: {counter.expected_count_query_2_quantity}")
            result_task = ResultTask(user_id, QueryId.Query2BestSelling, True, False, []).to_bytes()
            middleware.send(result_task)






class HandlerQuery2MostProfit:
    def handle_new_results(headers, msg, counter, user_id: str) -> ResultTask:
        if headers.is_eof():
            counter.expected_count_query_2_profit = headers.msg_count
            logging.info(f"Received expected message count for query 2 profit, expect {counter.expected_count_query_2_profit} got: {counter.count_query_2_profit}")            
            return None
        counter.count_query_2_profit += 1

        data: List[QueryResult2MostProfit] = []
        for line in msg.stream_rows():
            #logging.info(f"Q_2_prof {line}")
            item_name: str = line[0]
            month_encoded = int(line[1])
            year = (month_encoded - 1) // 12 + 2024
            month = (month_encoded-1) % 12 + 1
            year_month_created_at: date = datetime.strptime(f"{year}-{month}", "%Y-%m").date()
            profit_sum: float = line[2]
            #logging.info(f"type: {msg.types[0]}: {year_month_created_at}, {item_name}, {profit_sum}")
            data.append(QueryResult2MostProfit(year_month_created_at=year_month_created_at, item_name=item_name, profit_sum=profit_sum))
        return ResultTask(user_id, QueryId.Query2MostProfit, False, False, data).to_bytes()

    def check_send_eof(counter, user_id, middleware):
        if counter.is_eof_q2_profit():
            logging.info(f"Received last message for query 2 profit count: {counter.count_query_2_profit} expected_count: {counter.expected_count_query_2_profit}")
            result_task = ResultTask(user_id, QueryId.Query2MostProfit, True, False, []).to_bytes()
            middleware.send(result_task)

