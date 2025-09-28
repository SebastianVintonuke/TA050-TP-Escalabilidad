#!/usr/bin/env python3

import logging
import os
from configparser import ConfigParser

from middleware.errors import *
from middleware.result_node_middleware import *
from middleware.routing import csv_message
from middleware.routing.query_types import *
from middleware.select_tasks_middleware import *
from src.row_filtering import *
from src.row_mapping import *
from src.select_type_config import *
from src.type_expander import *
from src.selectnode import SelectNode

from middleware import routing


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

    initialize_log(logging_level)

    # Log config parameters at the beginning of the program to verify the configuration of the component
    logging.debug(
        f"action: config | result: success | port: {port} | node_id: {node_id} | logging_level: {logging_level}"
    )

    try:
        types_expander = TypeExpander()
        result_middleware = ResultNodeMiddleware()

        # Basic filter description
        """
            QUERY 1
            1. Transacciones (Id y monto) realizadas durante 2024 y 2025 entre las 06:00 AM y las
            11:00 PM con monto total mayor o igual a 75.        
        """
        types_expander.add_configuration_to_many( 
            SelectTypeConfiguration(
                result_middleware,
                csv_message.csv_msg_from_msg, # Message builder creator
                in_fields=["transaction_id", "year", "hour", "sum"],  # In order
                filters_conf=[
                    ["year", EQUALS_ANY, ["2024", "2025"]],
                    ["hour", BETWEEN_THAN_OP, [6, 23]],
                    ["sum", GREATER_THAN_OP, [75]],
                ],
                out_conf={ROW_CONFIG_OUT_COLS: ["transaction_id", "sum"]},
            )
            , ALL_FOR_TRANSACTIONS, QUERY_1 # add to query 1 independently for testing and so on.
        )

        """
            QUERY 2
                2. Productos más vendidos (nombre y cant) y productos que más ganancias han generado
                (nombre y monto), para cada mes en 2024 y 2025.     
        """        
        types_expander.add_configurations(ALL_FOR_TRANSACTIONS_ITEMS, 
            SelectTypeConfiguration(
                result_middleware,
                csv_message.csv_msg_from_msg,
                in_fields=["product_id", "year", "month", "revenue"],  # In order
                filters_conf=[["year", EQUALS_ANY, ["2024", "2025"]]],
                out_conf={
                    ROW_CONFIG_ACTIONS: [
                        [
                            MAP_MONTH,
                            {
                                "init_year": 2024,
                                "col_year": "year",
                                "col_month": "month",
                                "col_out": "month",
                            },
                        ]
                    ],
                    ROW_CONFIG_OUT_COLS: ["product_id", "month", "revenue"],
                },
            )
        )

        """
            QUERY 3
                3. TPV (Total Payment Value) por cada semestre en 2024 y 2025, para cada sucursal, para
                transacciones realizadas entre las 06:00 AM y las 11:00 PM.  
        """    
        types_expander.add_configurations(ALL_FOR_TRANSACTIONS, 
            SelectTypeConfiguration(
                result_middleware,
                csv_message.csv_msg_from_msg,
                in_fields=[
                    "transaction_id",
                    "store_id",
                    "year",
                    "month",
                    "hour",
                    "revenue",
                ],  # In order
                filters_conf=[
                    ["year", EQUALS_ANY, ["2024", "2025"]],
                    ["hour", BETWEEN_THAN_OP, [6, 23]],
                ],
                out_conf={
                    ROW_CONFIG_ACTIONS: [
                        [
                            MAP_SEMESTER,
                            {
                                "init_year": 2024,
                                "col_year": "year",
                                "col_month": "month",
                                "col_out": "mapped_semester",
                            },
                        ]
                    ],
                    ROW_CONFIG_OUT_COLS: [
                        "transaction_id",
                        "store_id",
                        "mapped_semester",
                        "revenue",
                    ],
                },
            )
        )
        """
            QUERY 4
                4. Fecha de cumpleaños de los 3 clientes que han hecho más compras durante 2024 y
                2025, para cada sucursal.
        """
        types_expander.add_configurations(ALL_FOR_TRANSACTIONS, 
            SelectTypeConfiguration(
                result_middleware,
                csv_message.csv_msg_from_msg,
                in_fields=["transaction_id", "store_id", "user_id", "year"],  # In order
                filters_conf=[
                    ["year", EQUALS_ANY, ["2024", "2025"]],
                ],
                out_conf={ROW_CONFIG_OUT_COLS: ["transaction_id", "store_id", "user_id"]},
            )
        )

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
