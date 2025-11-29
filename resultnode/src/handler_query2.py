from common.middleware.tasks.result import ResultTask
from common import QueryId
from common.results.query2mp import QueryResult2MostProfit
from common.results.query2bs import QueryResult2BestSelling

from datetime import datetime, date

import logging



class HandlerQuery2BestSelling:
    def handle_new_results(msg, user_id: str) -> bytes:

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

    def get_eof_msg(user_id):
        return ResultTask(user_id, QueryId.Query2BestSelling, True, False, []).to_bytes()

    def get_abort_msg(user_id):
        return ResultTask(user_id, QueryId.Query2BestSelling, True, True, []).to_bytes()


class HandlerQuery2MostProfit:
    def handle_new_results(msg, user_id: str) -> bytes:
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

    def get_eof_msg(user_id):
        return ResultTask(user_id, QueryId.Query2MostProfit, True, False, []).to_bytes()

    def get_abort_msg(user_id):
        return ResultTask(user_id, QueryId.Query2MostProfit, True, True, []).to_bytes()
