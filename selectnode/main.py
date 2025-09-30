#!/usr/bin/env python3

import logging
import os
import traceback
from configparser import ConfigParser

from middleware.errors import *
from middleware.groupby_middleware import *
from middleware.result_node_middleware import *
from middleware.select_tasks_middleware import *
from src.selectnode import SelectNode

from common.config.type_expander import *
from src.config_init import *



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
            "SELECT_NODE_ID", config["DEFAULT"]["SELECT_NODE_ID"]
        )
        config_params["groupby_node_count"] = os.getenv(
            "GROUPBY_NODE_COUNT", config["DEFAULT"]["GROUPBY_NODE_COUNT"]
        )

        config_params["logging_level"] = os.getenv(
            "LOGGING_LEVEL", config["DEFAULT"]["LOGGING_LEVEL"]
        )
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting selectnode".format(e))
    except ValueError as e:
        raise ValueError(
            "Key could not be parsed. Error: {}. Aborting selectnode".format(e)
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
    groupby_node_count = config_params["groupby_node_count"]
    initialize_log(logging_level)

    # Log config parameters at the beginning of the program to verify the configuration of the component
    logging.debug(
        f"action: config | result: success | port: {port} | node_id: {node_id} | logging_level: {logging_level} | groupby_node_count: {groupby_node_count}"
    )

    try:
        types_expander = TypeExpander()
        result_middleware = ResultNodeMiddleware()
        groupby_middleware = GroupbyTasksMiddleware(groupby_node_count)
        add_selectnode_config(types_expander, result_middleware, groupby_middleware)

        node = SelectNode(SelectTasksMiddleware(), types_expander)

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
        logging.error(f"action: select_node_main | result: error | err:{e}")


if __name__ == "__main__":
    main()
