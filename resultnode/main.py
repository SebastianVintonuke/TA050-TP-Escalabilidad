#!/usr/bin/env python3

import logging
import os
from configparser import ConfigParser
from typing import List, Tuple
import signal

from common.middleware.middleware import MessageMiddlewareQueue

from middleware.src.result_node_middleware import ResultNodeMiddleware
from middleware.src.routing.csv_message import CSVMessage


from middleware.src.routing.query_types import QUERY_1, QUERY_3, QUERY_2, QUERY_4, QUERY_2_QUANTITY, QUERY_2_REVENUE
from resultnode.src.resultnode import ResultNode
from resultnode.src.handler_query1 import HandlerQuery1
from resultnode.src.handler_query2 import HandlerQuery2BestSelling, HandlerQuery2MostProfit
from resultnode.src.handler_query3 import HandlerQuery3
from resultnode.src.handler_query4 import HandlerQuery4


from common.state_storage.nothing_state_storage import  NothingQueryStateStorage
from common.state_storage.query_state_storage import  QueryStateStorage
def creator_query_storage_in(folder):
    return lambda manager: QueryStateStorage(folder, manager)    

    
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

        config_params["profile_node"] = os.getenv(
            "PROFILE_NODE", 0)        
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


def main(config_params) -> None:
    port = config_params["port"]
    node_id = config_params["node_id"]
    logging_level = config_params["logging_level"]

    node_folder= "/etc/node_state/resultnode"

    initialize_log(logging_level)

    # Log config parameters at the beginning of the program to verify the configuration of the component
    logging.debug(
        f"action: config | result: success | port: {port} | node_id: {node_id} | logging_level: {logging_level}"
    )

    logging.debug(f"Using for resultnode state: {node_folder}")

    in_middle = ResultNodeMiddleware()
    out_middle = MessageMiddlewareQueue("middleware", "results")

    handlers = {
        QUERY_1: HandlerQuery1,
        QUERY_2_QUANTITY: HandlerQuery2BestSelling,
        QUERY_2_REVENUE: HandlerQuery2MostProfit,
        QUERY_3: HandlerQuery3,
        QUERY_4: HandlerQuery4
    }

    result_node = ResultNode(in_middle, out_middle, payload_deserializer= CSVMessage, handlers = handlers, store_creator = creator_query_storage_in(node_folder))

    def close_handler(sig, frame):
        logging.info("Received close signal... gracefully finishing")
        result_node.close()
        
    signal.signal(signal.SIGINT, close_handler)
    signal.signal(signal.SIGTERM, close_handler)


    result_node.start()



if __name__ == "__main__":
    config_params = initialize_config()

    if config_params["profile_node"] == 0:
        config_params["profile_node"] = False
        main(config_params)
    else:
        from common.profiling import profile
        config_params["profile_node"] = True
        logging.info("Profiling...")
        profile()(main)(config_params)