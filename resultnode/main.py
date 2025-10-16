#!/usr/bin/env python3

import logging
import os
from configparser import ConfigParser
from datetime import datetime, date
from typing import List, Tuple
import signal

from common import QueryId
from common.middleware.middleware import MessageMiddlewareQueue
from common.middleware.tasks.result import ResultTask
from common.results.query1 import QueryResult1
from common.results.query2bs import QueryResult2BestSelling
from common.results.query2mp import QueryResult2MostProfit
from common.results.query3 import QueryResult3, HalfCreatedAt
from common.results.query4 import QueryResult4
from middleware.src.result_node_middleware import ResultNodeMiddleware
from middleware.src.routing.query_types import QUERY_1, QUERY_3, QUERY_2, QUERY_4, QUERY_2_QUANTITY, QUERY_2_REVENUE
from middleware.src.routing.csv_message import CSVMessage



def initialize_config():  # type: ignore[no-untyped-def]
    """Parse env variables or config file to find program config params

    Function that search and parse program configuration parameters in the
    program environment variables first and the in a config file.
    If at least one of the config parameters is not found a KeyError exception
    is thrown. If a parameter could not be parsed, a ValueError is thrown.
    If parsing succeeded, the function returns a ConfigParser object
    with config parameters
    """

    config = ConfigParser(os.environ)
    # If config.ini does not exists original config object is not modified
    config.read("config.ini")

    config_params = {}
    try:
        config_params["port"] = int(os.getenv("PORT", config["DEFAULT"]["PORT"]))
        config_params["node_id"] = os.getenv(
            "RESULT_NODE_ID", config["DEFAULT"]["RESULT_NODE_ID"]
        )
        config_params["logging_level"] = os.getenv(
            "LOGGING_LEVEL", config["DEFAULT"]["LOGGING_LEVEL"]
        )
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting resultnode".format(e))
    except ValueError as e:
        raise ValueError(
            "Key could not be parsed. Error: {}. Aborting resultnode".format(e)
        )

    return config_params


def initialize_log(logging_level: int) -> None:
    """
    Python custom logging initialization

    Current timestamp is added to be able to identify in docker
    compose logs the date when the log has arrived
    """
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging_level,
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main() -> None:
    config_params = initialize_config()
    port = config_params["port"]
    node_id = config_params["node_id"]
    logging_level = config_params["logging_level"]

    initialize_log(logging_level)

    # Log config parameters at the beginning of the program to verify the configuration of the component
    logging.debug(
        f"action: config | result: success | port: {port} | node_id: {node_id} | logging_level: {logging_level}"
    )

    result_middleware = ResultNodeMiddleware()

    results_storage_middleware = MessageMiddlewareQueue("middleware", "results")
    class Counter:
        def __init__(self):
            self.count_query_1 = 0
            self.count_query_2_profit = 0
            self.count_query_2_quantity = 0
            self.count_query_3 = 0
            self.count_query_4 = 0
            self.expected_count_query_1 = -1
            self.expected_count_query_2_profit = -1
            self.expected_count_query_2_quantity = -1
            self.expected_count_query_3 = -1
            self.expected_count_query_4 = -1

        def is_eof_q1(self):
            return self.expected_count_query_1 >=0 and self.count_query_1 >= self.expected_count_query_1
        def is_eof_q2_profit(self):
            return self.expected_count_query_2_profit >=0 and self.count_query_2_profit >= self.expected_count_query_2_profit
        def is_eof_q2_quantity(self):
            return self.expected_count_query_2_quantity >=0 and self.count_query_2_quantity >= self.expected_count_query_2_quantity
        
        def is_eof_q3(self):
            return self.expected_count_query_3 >=0 and self.count_query_3 >= self.expected_count_query_3
        def is_eof_q4(self):
            return self.expected_count_query_4 >=0 and self.count_query_4 >= self.expected_count_query_4

    results_message_counter= {}
    def get_counter(user_id: str):
        counter = results_message_counter.get(user_id, None)
        if counter == None:
            counter = Counter()
            results_message_counter[user_id] = counter

        return counter            

    def handle_query_1_result(headers, msg,counter, user_id: str) -> None:
        if headers.is_eof():
            counter.expected_count_query_1 = headers.msg_count
            logging.info(f"Received expected message count for query 1, expect {counter.expected_count_query_1} got: {counter.count_query_1}")
            return
        counter.count_query_1 += 1

        data: List[QueryResult1] = []
        for line in msg.stream_rows():
            transaction_id = line[0]
            final_amount = float(line[1])
            data.append(QueryResult1(transaction_id=transaction_id, final_amount=final_amount))
        result_task = ResultTask(user_id, QueryId.Query1, False, False, data).to_bytes()
        results_storage_middleware.send(result_task)

    def handle_query_2_best_selling_result(headers, msg, counter, user_id: str) -> None:
        if headers.is_eof():

            counter.expected_count_query_2_quantity = headers.msg_count
            logging.info(f"Received expected message count for query 2 quantity, expect {counter.expected_count_query_2_quantity} got: {counter.count_query_2_quantity}")
            return
        counter.count_query_2_quantity += 1

        data: List[QueryResult2BestSelling] = []
        for line in msg.stream_rows():
            #logging.info(f"Q_2_QUAN {line}")
            item_name: str = line[0]
            month_encoded = int(line[1])
            year = month_encoded // 12 + 2024
            month = (month_encoded-1) % 12 + 1
            year_month_created_at: date = datetime.strptime(f"{year}-{month}", "%Y-%m").date()
            sellings_qty: int = int(float(line[2]))
            #logging.info(f"type: {msg.types[0]}: {year_month_created_at}, {item_name}, {sellings_qty}")
            data.append(QueryResult2BestSelling(year_month_created_at=year_month_created_at, item_name=item_name, sellings_qty=sellings_qty))
        result_task = ResultTask(user_id, QueryId.Query2BestSelling, False, False, data).to_bytes()
        results_storage_middleware.send(result_task)

    def handle_query_2_most_profit_result(headers, msg, counter, user_id: str) -> None:
        if headers.is_eof():
            counter.expected_count_query_2_profit = headers.msg_count
            logging.info(f"Received expected message count for query 2 profit, expect {counter.expected_count_query_2_profit} got: {counter.count_query_2_profit}")            
            return
        counter.count_query_2_profit += 1

        data: List[QueryResult2MostProfit] = []
        for line in msg.stream_rows():
            #logging.info(f"Q_2_prof {line}")
            item_name: str = line[0]
            month_encoded = int(line[1])
            year = month_encoded // 12 + 2024
            month = (month_encoded-1) % 12 + 1
            year_month_created_at: date = datetime.strptime(f"{year}-{month}", "%Y-%m").date()
            profit_sum: float = line[2]
            #logging.info(f"type: {msg.types[0]}: {year_month_created_at}, {item_name}, {profit_sum}")
            data.append(QueryResult2MostProfit(year_month_created_at=year_month_created_at, item_name=item_name, profit_sum=profit_sum))
        result_task = ResultTask(user_id, QueryId.Query2MostProfit, False, False, data).to_bytes()
        results_storage_middleware.send(result_task)

    def handle_query_3_result(headers, msg, counter, user_id: str) -> None:
        if headers.is_eof():
            counter.expected_count_query_3 = headers.msg_count
            logging.info(f"Received expected message count for query 3, expect {counter.expected_count_query_3} got: {counter.count_query_3}")

            return
        counter.count_query_3 += 1

        data: List[QueryResult3] = []
        for line in msg.stream_rows():
            #logging.info(f"Q_3 {line}")
            store_name = line[0]
            year_created_at, half_created_at = __year_semester_decode(line[1])
            tpv = float(line[2])
            data.append(QueryResult3(year_created_at=year_created_at, half_created_at=half_created_at, store_name=store_name, tpv=tpv))
        result_task = ResultTask(user_id, QueryId.Query3, False, False, data).to_bytes()
        results_storage_middleware.send(result_task)

    def handle_query_4_result(headers, msg,counter,  user_id: str) -> None:
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
        result_task = ResultTask(user_id, QueryId.Query4, False, False, data).to_bytes()
        results_storage_middleware.send(result_task)

    def __year_semester_decode(year_semester_str: str) -> Tuple[date, HalfCreatedAt]:
        year_semester = int(year_semester_str)
        year_str = str((year_semester * 6 // 12) + 2024)
        year = datetime.strptime(str(year_str), "%Y").date()
        if year_semester % 2 == 0:
            semester = HalfCreatedAt.H1
        else:
            semester = HalfCreatedAt.H2
        return year, semester

    def handle_result(headers, msg):
        user_id = headers.ids[0]
        query_type = headers.types[0]
        msg = CSVMessage(msg)
        counter = get_counter(user_id)

        if query_type == QUERY_1:
            handle_query_1_result(headers, msg, counter,  user_id)
            if counter.is_eof_q1():
                logging.info(f"Received last message for query 1 count: {counter.count_query_1} expected_count: {counter.expected_count_query_1}")
                result_task = ResultTask(user_id, QueryId.Query1, True, False, []).to_bytes()
                results_storage_middleware.send(result_task)


        elif query_type == QUERY_2_QUANTITY: # TODO QUANTITY TRAE DATOS DE REVENUE
            handle_query_2_best_selling_result(headers, msg,counter,  user_id)
            if counter.is_eof_q2_quantity():
                logging.info(f"Received last message for query 2 best selling count: {counter.count_query_2_quantity} expected_count: {counter.expected_count_query_2_quantity}")
                result_task = ResultTask(user_id, QueryId.Query2BestSelling, True, False, []).to_bytes()
                results_storage_middleware.send(result_task)


        elif query_type == QUERY_2_REVENUE:
            handle_query_2_most_profit_result(headers, msg,counter,  user_id)
            
            if counter.is_eof_q2_profit():
                logging.info(f"Received last message for query 2 profit count: {counter.count_query_2_profit} expected_count: {counter.expected_count_query_2_profit}")
                result_task = ResultTask(user_id, QueryId.Query2MostProfit, True, False, []).to_bytes()
                results_storage_middleware.send(result_task)

        elif query_type == QUERY_3:
            handle_query_3_result(headers, msg,counter,  user_id)

            if counter.is_eof_q3():
                logging.info(f"Received last message for query 3 count: {counter.count_query_3} expected_count: {counter.expected_count_query_3}")
                result_task = ResultTask(user_id, QueryId.Query3, True, False, []).to_bytes()
                results_storage_middleware.send(result_task)

        elif query_type == QUERY_4: # TODO DATOS DE QUERY 4
            handle_query_4_result(headers, msg,counter,  user_id)

            if counter.is_eof_q4():
                logging.info(f"Received last message for query 4 count: {counter.count_query_4} expected_count: {counter.expected_count_query_4}")
                result_task = ResultTask(user_id, QueryId.Query4, True, False, []).to_bytes()
                results_storage_middleware.send(result_task)

        else:
            logging.info(f"NO EXISTE {headers.types}")
            #raise ValueError(f"Unknown query type: {query_type}")

    def close_handler(sig, frame):
        logging.info("Received close signal... gracefully finishing")
        result_middleware.close()
        results_storage_middleware.close()
        
    signal.signal(signal.SIGINT, close_handler)
    signal.signal(signal.SIGTERM, close_handler)

    result_middleware.start_consuming(handle_result)


if __name__ == "__main__":
    main()
