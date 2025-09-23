#!/usr/bin/env python3

import logging
import os
import socket
from configparser import ConfigParser
from pathlib import Path

from common import BatchProtocol, SignalProtocol


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
        config_params["server_ip"] = os.getenv(
            "SERVER_IP", config["DEFAULT"]["SERVER_IP"]
        )
        config_params["server_port"] = int(
            os.getenv("SERVER_PORT", config["DEFAULT"]["SERVER_PORT"])
        )
        config_params["data_dir"] = os.getenv("DATA_DIR", config["DEFAULT"]["DATA_DIR"])
        config_params["logging_level"] = os.getenv(
            "LOGGING_LEVEL", config["DEFAULT"]["LOGGING_LEVEL"]
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
    port = config_params["port"]
    server_ip = config_params["server_ip"]
    server_port = config_params["server_port"]
    data_dir = config_params["data_dir"]
    logging_level = config_params["logging_level"]

    initialize_log(logging_level)

    # Log config parameters at the beginning of the program to verify the configuration of the component
    logging.debug(
        f"action: config | result: success | port: {port} | server_ip: {server_ip} | server_port: {server_port}, data_dir: {data_dir}| logging_level: {logging_level}"
    )

    a_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    a_socket.connect((server_ip, server_port))

    signal_protocol = SignalProtocol(a_socket)
    batch_protocol = BatchProtocol(a_socket)

    stores_path = Path(data_dir) / "stores" / "stores.csv"

    with open(stores_path, "rb") as reader:
        for index, line in enumerate(reader):
            if index == 0:
                batch_protocol.send_batch([line])
            else:
                batch_protocol.send_batch([line])  # batch trivial de tamaño 1
                signal_protocol.wait_signal()

    batch_protocol.send_batch([])  # mando uno vacio para indicar el fin


if __name__ == "__main__":
    main()
