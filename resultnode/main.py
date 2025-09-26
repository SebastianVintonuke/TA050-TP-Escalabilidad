#!/usr/bin/env python3

import logging
import os
import time
from configparser import ConfigParser

from common import QueryId
from common.middleware.middleware import MessageMiddlewareQueue
from common.middleware.tasks.result import ResultTask


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

    time.sleep(20)
    logging.getLogger("pika").setLevel(logging.WARNING)
    logging.getLogger("pika.adapters").setLevel(logging.WARNING)
    message_middleware_queue = MessageMiddlewareQueue("middleware", "results")
    # fake_user_id = new_uuid()

    eof = ResultTask("test_user_id", QueryId.Query1, True, False, []).to_bytes()
    message_middleware_queue.send(eof)
    """
    # CONSULTA 1
    partial_results_1 = [
        QueryResult1.from_bytes(
            "9af9901b-60a8-4f95-a586-980a725d1049,87.0".encode("utf-8")
        ),
        QueryResult1.from_bytes(
            "6a58d026-823f-4bda-a15c-2c3f090b7a27,78.0".encode("utf-8")
        ),
    ]
    partial_results_2 = [
        QueryResult1.from_bytes(
            "8845cdaa-d230-4453-bbdf-0e4f783045bf,76.5".encode("utf-8")
        ),
        QueryResult1.from_bytes(
            "1b42e037-691b-40e1-9082-04743ef92f7b,75.0".encode("utf-8")
        ),
    ]
    message_1 = ResultTask(
        fake_user_id, QueryId.Query1, False, False, partial_results_1
    ).to_bytes()
    message_2 = ResultTask(
        fake_user_id, QueryId.Query1, True, False, partial_results_2
    ).to_bytes()
    message_middleware_queue.send(message_1)
    message_middleware_queue.send(message_2)
    # CONSULTA 2MP
    partial_results_3 = [
        QueryResult2MostProfit.from_bytes(
            "2025-01,8,3104830.0,Matcha Latte".encode("utf-8")
        ),
        QueryResult2MostProfit.from_bytes(
            "2025-02,8,2804010.0,Matcha Latte".encode("utf-8")
        ),
    ]
    partial_results_4 = [
        QueryResult2MostProfit.from_bytes(
            "2025-03,8,3103800.0,Matcha Latte".encode("utf-8")
        ),
        QueryResult2MostProfit.from_bytes(
            "2025-04,8,2995990.0,Matcha Latte".encode("utf-8")
        ),
    ]
    message_3 = ResultTask(
        fake_user_id, QueryId.Query2MostProfit, False, False, partial_results_3
    ).to_bytes()
    message_4 = ResultTask(
        fake_user_id, QueryId.Query2MostProfit, True, False, partial_results_4
    ).to_bytes()
    message_middleware_queue.send(message_3)
    message_middleware_queue.send(message_4)
    # CONSULTA 2BS
    partial_results_5 = [
        QueryResult2BestSelling.from_bytes(
            "2025-01,5,156120,Flat White".encode("utf-8")
        ),
        QueryResult2BestSelling.from_bytes(
            "2025-02,7,140290,Hot Chocolate".encode("utf-8")
        ),
        QueryResult2BestSelling.from_bytes(
            "2025-03,2,155389,Americano".encode("utf-8")
        ),
        QueryResult2BestSelling.from_bytes("2025-04,6,150773,Mocha".encode("utf-8")),
    ]
    message_5 = ResultTask(
        fake_user_id, QueryId.Query2BestSelling, True, False, partial_results_5
    ).to_bytes()
    message_middleware_queue.send(message_5)
    # CONSULTA 3
    partial_results_6 = [
        QueryResult3.from_bytes(
            "2025-H1,1,7967688.5,G Coffee @ USJ 89q".encode("utf-8")
        ),
        QueryResult3.from_bytes(
            "2025-H1,2,8007511.0,G Coffee @ Kondominium Putra".encode("utf-8")
        ),
    ]
    partial_results_7 = [
        QueryResult3.from_bytes("2025-H1,9,7989533.5,G Coffee @ PJS8".encode("utf-8")),
        QueryResult3.from_bytes(
            "2025-H1,10,7968619.0,G Coffee @ Taman Damansara".encode("utf-8")
        ),
    ]
    message_6 = ResultTask(
        fake_user_id, QueryId.Query3, False, False, partial_results_6
    ).to_bytes()
    message_7 = ResultTask(
        fake_user_id, QueryId.Query3, True, False, partial_results_7
    ).to_bytes()
    message_middleware_queue.send(message_6)
    message_middleware_queue.send(message_7)
    # CONSULTA 4
    partial_results_8 = [
        QueryResult4.from_bytes(
            "5,1238919,5,G Coffee @ Seksyen 21,2001-04-10".encode("utf-8")
        )
    ]
    partial_results_9 = [
        QueryResult4.from_bytes(
            "10,1196101,5,G Coffee @ Taman Damansara,1976-11-18".encode("utf-8")
        )
    ]
    message_8 = ResultTask(
        fake_user_id, QueryId.Query4, False, False, partial_results_8
    ).to_bytes()
    message_9 = ResultTask(
        fake_user_id, QueryId.Query4, True, False, partial_results_9
    ).to_bytes()
    message_middleware_queue.send(message_8)
    message_middleware_queue.send(message_9)
    """


if __name__ == "__main__":
    main()
