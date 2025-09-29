#!/usr/bin/env python3

import logging
import traceback
import os
from configparser import ConfigParser

from middleware.result_node_middleware import * 
from middleware.groupby_middleware import * 
from middleware.memory_middleware import * 


from src.groupbynode import GroupbyNode 
from src.groupby_initialize import * 
from src.topk_initialize import * 

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
        config_params["node_ind"] = os.getenv(
            "NODE_IND", config["DEFAULT"]["NODE_IND"]
        )
        config_params["node_count"] = os.getenv(
            "NODE_COUNT", config["DEFAULT"]["NODE_COUNT"]
        )
        config_params["load_topk"] = os.getenv(
            "LOAD_TOPK", 0
        )
        
        config_params["logging_level"] = os.getenv(
            "LOGGING_LEVEL", config["DEFAULT"]["LOGGING_LEVEL"]
        )
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting groupbynode".format(e))
    except ValueError as e:
        raise ValueError(
            "Key could not be parsed. Error: {}. Aborting groupbynode".format(e)
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
    logging_level = config_params["logging_level"]
    node_ind = config_params["node_ind"]
    node_count = config_params["node_count"]
    loadtopk = config_params["load_topk"] != 0

    initialize_log(logging_level)

    # Log config parameters at the beginning of the program to verify the configuration of the component
    logging.debug(
        f"action: config | result: success | port: {port} | logging_level: {logging_level} | node_ind: {node_ind} | node_count:{node_count}" #| topk {loadtopk}
    )

    try:
        result_middleware = ResultNodeMiddleware()
        topk_middleware = MemoryMiddleware()
        middleware_group = GroupbyTasksMiddleware(node_count, ind = node_ind)

        types_config_groupby = configure_types_groupby(result_middleware, topk_middleware)

        # In memory it doesnt actually connect to network nor block for messeging
        types_config_topk = configure_types_groupby(result_middleware, result_middleware)
        node_topk = GroupbyNode(middleware_group, types_config_topk)
        node_topk.start()

        node = GroupbyNode(middleware_group, types_config_groupby)

        restart = True
        while restart:
            try:
                node.start()
                restart = False
            except MessageMiddlewareMessageError as e:
                traceback.print_exc()
                logging.error(f"Non fatal fail {e}")

        
        node.close()
    except Exception as e:
        logging.error(
            f"action: groupby_node_main | result: error | err:{e}"
            )

if __name__ == "__main__":
    main()
