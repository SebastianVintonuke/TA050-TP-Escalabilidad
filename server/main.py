#!/usr/bin/env python3

import logging
import os
import socket
from configparser import ConfigParser

from common import BatchProtocol, Model, SignalProtocol


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
        config_params["listen_backlog"] = int(
            os.getenv("LISTEN_BACKLOG", config["DEFAULT"]["LISTEN_BACKLOG"])
        )
        config_params["logging_level"] = os.getenv(
            "LOGGING_LEVEL", config["DEFAULT"]["LOGGING_LEVEL"]
        )
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError(
            "Key could not be parsed. Error: {}. Aborting server".format(e)
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
    listen_backlog = config_params["listen_backlog"]
    logging_level = config_params["logging_level"]

    initialize_log(logging_level)

    # Log config parameters at the beginning of the program to verify the configuration of the component
    logging.debug(
        f"action: config | result: success | port: {port} | listen_backlog: {listen_backlog} | logging_level: {logging_level}"
    )

    _socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    _socket.bind(("", port))
    _socket.listen(listen_backlog)

    client_socket, addr = _socket.accept()
    logging.info(f"action: accept_connections | result: success | ip: {addr[0]}")

    signal_protocol = SignalProtocol(client_socket)
    batch_protocol = BatchProtocol(client_socket)

    # count = 0
    try:
        batch = batch_protocol.wait_batch()
        header = batch[0]
        model = Model.model_for(header)
        while len(batch) > 0:  # el fin se indica con un batch vacio
            # count += 1
            # if count == 3:
            #    raise Exception("prueba del protocolo de error")  # Descomentar para ver propagacion de errores
            batch = batch_protocol.wait_batch()
            signal_protocol.send_ack()
            for item in batch:
                print(model.from_bytes_and_project(item))
    except Exception as e:
        print(e)
        signal_protocol.send_error(str(e))

    print("end")


if __name__ == "__main__":
    main()
