#!/usr/bin/env python3

import logging
import os
from configparser import ConfigParser
from typing import List

from common import QueryId
from common.middleware.middleware import MessageMiddlewareQueue
from common.middleware.tasks.result import ResultTask
from common.results.query1 import QueryResult1
from middleware.src.result_node_middleware import ResultNodeMiddleware


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
        data: List[QueryResult1] = []
        for line in msg.stream_rows():
            data.append(QueryResult1(transaction_id=line[0], final_amount=line[1]))

        result_task = ResultTask(msg.ids[0], QueryId.Query1, msg.is_partition_eof(), False, data).to_bytes()
        results_storage_middleware.send(result_task)
        msg.ack_self()


        #if msg.is_partition_eof(): # Partition EOF is sent when no more data on partition, or when real EOF or error happened as signal.
        #    if msg.is_last_message():
        #        if msg.is_eof():
        #            logging.info(f"Received final eof OF {msg.ids}")
        #        else:
        #            logging.info(f"Received ERROR code: {msg.partition} IN {msg.ids}")
        #    else:
        #        logging.info(f"RESULT STORAGE RECEIVED PARITION EOF? IGNORED, partition {msg.partition}, ids:{msg.ids}")

        #    msg.ack_self()
        #    return
        #logging.info(f"GOT RESULT MSG? FROM QUERIES {msg.ids}")
        #for itm in msg.stream_rows():
        #    logging.info(f"ROW {itm}")
        
        #msg.ack_self()

    result_middleware.start_consuming(handle_result)


if __name__ == "__main__":
    main()
