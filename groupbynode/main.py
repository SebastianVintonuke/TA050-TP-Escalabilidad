#!/usr/bin/env python3

import logging
import traceback
import os
from configparser import ConfigParser

from middleware import routing 
from middleware.result_node_middleware import * 
from middleware.groupby_middleware import * 

from middleware.routing import csv_message
from middleware.errors import * 
from middleware.routing.query_types import *

from src.groupbynode import GroupbyNode 
from src.row_grouping import * 
from src.groupby_type_config import * 

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

    initialize_log(logging_level)

    # Log config parameters at the beginning of the program to verify the configuration of the component
    logging.debug(
        f"action: config | result: success | port: {port} | logging_level: {logging_level} | node_ind: {node_ind} | node_count:{node_count}"
    )

    try:

        """
       Transacciones (Id y monto) realizadas durante 2024 y 2025 entre las 06:00 AM y las
        11:00 PM con monto total mayor o igual a 75.
        2. Productos más vendidos (nombre y cant) y productos que más ganancias han generado
        (nombre y monto), para cada mes en 2024 y 2025.
        3. TPV (Total Payment Value) por cada semestre en 2024 y 2025, para cada sucursal, para
        transacciones realizadas entre las 06:00 AM y las 11:00 PM.
        4. Fecha de cumpleaños de los 3 clientes que han hecho más compras durante 2024 y
        2025, para cada sucursal.           
        """

        result_middleware = ResultNodeMiddleware()
        types_config = {}

        types_config[QUERY_2] = GroupbyTypeConfiguration(result_middleware, csv_message.csv_msg_from_msg, 
                in_fields = ["product_id", "month", "revenue"], #EQUALS to out cols from select node main 
                grouping_conf = [["product_id", "month"], {
                    "revenue": SUM_ACTION,
                    "quantity_sold": COUNT_ACTION
                }],
                out_conf={ROW_CONFIG_OUT_COLS: ["product_id", "month", "revenue", "quantity_sold"]},
        )

        types_config[QUERY_3] = GroupbyTypeConfiguration(result_middleware, csv_message.csv_msg_from_msg, 
                in_fields = ["store_id","mapped_semester","revenue"],  
                grouping_conf= [["store_id", "mapped_semester"], {
                    "revenue": SUM_ACTION,
                }],
                out_conf={ROW_CONFIG_OUT_COLS: ["store_id","mapped_semester", "revenue"]},
        )

        types_config[QUERY_4] = GroupbyTypeConfiguration(result_middleware, csv_message.csv_msg_from_msg,
                in_fields= ["transaction_id", "store_id", "user_id"],
                grouping_conf= [["store_id", "user_id"], {
                    "purchase_count": COUNT_ACTION
                }],
                out_conf={ROW_CONFIG_OUT_COLS: ["store_id","user_id", "purchase_count"]},                
        )


        middleware_group = GroupbyTasksMiddleware(node_count, ind = node_ind)
        node = GroupbyNode(middleware_group, types_config)

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
