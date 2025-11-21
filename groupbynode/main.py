#!/usr/bin/env python3

import logging
import traceback
import os
from configparser import ConfigParser
import signal

from middleware.result_node_middleware import * 
from middleware.memory_middleware import MemoryMiddleware, HashedMemoryMessageBuilder, MemoryMessage
from middleware.routing.csv_message import CSVMessage

from middleware.groupby_middleware import * 
from middleware.join_tasks_middleware import * 


from src.groupbynode import GroupbyNode 
from src.groupby_initialize import * 
from src.topk_initialize import * 
from common.node_utils import RestartLogic

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
        config_params["node_ind"] = os.getenv(
            "NODE_IND", config["DEFAULT"]["NODE_IND"]
        )
        config_params["node_count"] = os.getenv(
            "NODE_COUNT", config["DEFAULT"]["NODE_COUNT"]
        )

        config_params["join_node_count"] = os.getenv(
            "JOIN_NODE_COUNT", config["DEFAULT"]["JOIN_NODE_COUNT"]
        )

        

        config_params["load_topk"] = os.getenv(
            "LOAD_TOPK", 0
        )
        
        config_params["logging_level"] = os.getenv(
            "LOGGING_LEVEL", config["DEFAULT"]["LOGGING_LEVEL"]
        )
        config_params["profile_node"] = os.getenv(
            "PROFILE_NODE", 0)

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

def main(config_params) -> None:
    port = config_params["port"]
    logging_level = config_params["logging_level"]
    node_ind = config_params["node_ind"]
    node_count = config_params["node_count"]
    join_node_count = config_params["join_node_count"]
    
    loadtopk = config_params["load_topk"] != 0
    is_profiling = config_params["profile_node"]
    init_folder="/etc/node_state/"
    grp_folder = init_folder+"groupby"
    topk_folder = init_folder+"topk"

    initialize_log(logging_level)

    # Log config parameters at the beginning of the program to verify the configuration of the component
    logging.debug(
        f"action: config | result: success | profiling: {is_profiling} | port: {port} | logging_level: {logging_level} | node_ind: {node_ind} | node_count:{node_count}" #| topk {loadtopk}
    )

    logging.debug(f"Using for groupbynode state: {grp_folder} topk: {topk_folder}")


    try:
        #result_middleware = ResultNodeMiddleware()
        topk_middleware = MemoryMiddleware()
        join_middleware = JoinTasksMiddleware(join_node_count)
        middleware_group = GroupbyTasksMiddleware(node_count, ind = node_ind)

        #types_config_groupby = configure_types_groupby(join_middleware, topk_middleware)
        types_config_groupby = configure_types_groupby(
                join_middleware, topk_middleware, topk_middleware_type = HashedMemoryMessageBuilder)

        # In memory it doesnt actually connect to network nor block for messeging
        types_config_topk = configure_types_topk(join_middleware)
        node_topk = GroupbyNode(topk_middleware, MemoryMessage, types_config_topk, store_creator = creator_query_storage_in(topk_folder))
        node_topk.start()

        node = GroupbyNode(middleware_group, CSVMessage, types_config_groupby, store_creator = creator_query_storage_in(grp_folder))
        restarter = RestartLogic(MessageMiddlewareMessageError)

        def close_handler(sig, frame):
            logging.info("Received close signal... gracefully finishing")
            restarter.stop_restart()
            node.close()
        signal.signal(signal.SIGINT, close_handler)
        signal.signal(signal.SIGTERM, close_handler)

        restarter.start_node_loop(node)
        
        node.close()
    except Exception as e:
        logging.error(
            f"action: groupby_node_main | result: error | err:{e}"
            )

if __name__ == "__main__":
    config_params = initialize_config()

    if config_params["profile_node"] == 0:
        config_params["profile_node"] = False
        main(config_params)
    else:
        from common.profiling import profile
        config_params["profile_node"] = True
        profile()(main)(config_params)