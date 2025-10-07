#!/usr/bin/env python3

import logging
import os
import signal
from configparser import ConfigParser

from client.src.client import Client


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
        config_params["server_address"] = os.getenv(
            "SERVER_ADDRESS", config["DEFAULT"]["SERVER_ADDRESS"]
        )
        config_params["input_dir"] = os.getenv(
            "INPUT_DIR", config["DEFAULT"]["INPUT_DIR"]
        )
        config_params["output_dir"] = os.getenv(
            "OUTPUT_DIR", config["DEFAULT"]["OUTPUT_DIR"]
        )
        config_params["logging_level"] = os.getenv(
            "LOGGING_LEVEL", config["DEFAULT"]["LOGGING_LEVEL"]
        )
        config_params["executions"] = int(os.getenv(
            "EXECUTIONS", config["DEFAULT"]["EXECUTIONS"])
        )
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting client".format(e))
    except ValueError as e:
        raise ValueError(
            "Key could not be parsed. Error: {}. Aborting client".format(e)
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
    server_address = config_params["server_address"]
    input_dir = config_params["input_dir"]
    output_dir = config_params["output_dir"]
    logging_level = config_params["logging_level"]
    number_of_executions = config_params["executions"]

    initialize_log(logging_level)

    # Log config parameters at the beginning of the program to verify the configuration of the component
    logging.debug(
        f"action: config | result: success | server_address: {server_address} | input_dir: {input_dir} | output_dir: {output_dir} | logging_level: {logging_level} | client_executions: {number_of_executions}"
    )

    client = Client(server_address, input_dir, output_dir)
    signal.signal(signal.SIGTERM, client.graceful_shutdown)

    client.start(number_of_executions)

    logging.shutdown()


if __name__ == "__main__":
    main()
