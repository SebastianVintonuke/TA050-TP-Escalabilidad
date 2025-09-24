#!/usr/bin/env python3

import logging
import os
import socket
from configparser import ConfigParser

from common import QueryId, QueryResult1, ResultsProtocol, new_uuid

from middleware import routing 
from middleware.result_node_middleware import * 
from middleware.select_tasks_middleware import * 
from middleware.errors import * 
from middleware.routing.select_task_message import * 


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
        config_params["results_ip"] = os.getenv(
            "RESULTS_IP", config["DEFAULT"]["RESULTS_IP"]
        )
        config_params["results_port"] = int(
            os.getenv("RESULTS_PORT", config["DEFAULT"]["RESULTS_PORT"])
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

def start_notifying(addr):
    a_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    a_socket.connect(addr)

    results_protocol = ResultsProtocol(a_socket)
    fake_user_id = new_uuid()
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

    results_protocol.notify_results_for(fake_user_id, QueryId.Query1)
    for result in partial_results_1:
        print(f"send {result}")
    results_protocol.append_results(partial_results_1)
    for result in partial_results_2:
        print(f"send {result}")
    results_protocol.append_results(partial_results_2)
    print("notify results are ready")
    results_protocol.notify_eof_results()  # Indica que se guardó el último resultado, el server cierra su socket
    results_protocol.close_with(lambda socket_to_close: socket_to_close.close())
    print("end")

def main() -> None:
    config_params = initialize_config()
    port = config_params["port"]
    results_ip = config_params["results_ip"]
    results_port = config_params["results_port"]
    node_id = config_params["node_id"]
    logging_level = config_params["logging_level"]

    initialize_log(logging_level)

    # Log config parameters at the beginning of the program to verify the configuration of the component
    logging.debug(
        f"action: config | result: success | port: {port} | node_id: {node_id} | logging_level: {logging_level}"
    )

    result_middleware = ResultNodeMiddleware()
    tmp_send_middle = SelectTasksMiddleware()

    msg_build = SelectTaskMessageBuilder(["8845cdaa-d230-4453-bbdf-0e4f783045bf,76.5"], ["query_1"])

    ## From test
    rows_pass = [
        {'year': 2024, 'hour': 7, 'sum': 88},
        {'year': 2025, 'hour': 23, 'sum': 942},
        {'year': 2024, 'hour': 6, 'sum': 942},
    ]
    rows_fail = [
        {'year': 2027, 'hour': 7, 'sum': 88},
        {'year': 2025, 'hour': 24, 'sum': 942},
        {'year': 2024, 'hour': 6, 'sum': 55},
    ]

    for itm in rows_pass:
        msg_build.add_row(itm)

    for itm in rows_fail:
        msg_build.add_row(itm)

    logging.info(
        f"action: select_msg_build | result: success | {msg_build.get_headers()} | {msg_build.serialize_payload()}"
    )

    tmp_send_middle.send(msg_build)

    logging.info(
        f"action: start_consuming | result: success"
    )

    def handle_result(result_msg):
        print("GOT RESULT MSG? ", result_msg)
        for itm in result_msg.stream_rows():
            print("ROW", itm)

    result_middleware.start_consuming(handle_result)
    #start_notifying((results_ip, results_port))


if __name__ == "__main__":
    main()
