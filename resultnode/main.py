#!/usr/bin/env python3

import logging
import os
from configparser import ConfigParser
from datetime import datetime
from typing import List

from common import QueryId
from common.middleware.middleware import MessageMiddlewareQueue
from common.middleware.tasks.result import ResultTask
from common.results.query1 import QueryResult1
from common.results.query3 import QueryResult3, HalfCreatedAt
from middleware.src.result_node_middleware import ResultNodeMiddleware
from middleware.src.routing.query_types import QUERY_3, QUERY_1


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


    def handle_result(msg):
        query_type = msg.types[0]
        if msg.is_partition_eof():
            logging.critical("es EOF")
        else:
            if query_type == QUERY_1:
                data: List[QueryResult1] = []
                for line in msg.stream_rows():
                    data.append(QueryResult1(transaction_id=line[0], final_amount=float(line[1])))
                result_task = ResultTask(msg.ids[0], QueryId.Query1, msg.is_eof(), False, data).to_bytes()
                results_storage_middleware.send(result_task)
                msg.ack_self()

            if query_type == QUERY_3:
                data: List[QueryResult3] = []
                for line in msg.stream_rows():
                    year_semester = int(line[1])
                    year = (year_semester * 6 // 12) + 2024
                    if year_semester % 2 == 0:
                        semester = "H1"
                    else:
                        semester = "H2"
                    data.append(QueryResult3(year_created_at=datetime.strptime(str(year), "%Y").date(),
                                             half_created_at=HalfCreatedAt(semester), tpv=float(line[2]),
                                             store_name=line[0]))

                result_task = ResultTask(msg.ids[0], QueryId.Query3, msg.is_eof(), False, data).to_bytes()
                results_storage_middleware.send(result_task)
                msg.ack_self()

    result_middleware.start_consuming(handle_result)


if __name__ == "__main__":
    main()
