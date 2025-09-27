import logging
import os
import socket
import sys
from configparser import ConfigParser
from pathlib import Path

from common.protocol.clientserver import ClientProtocol

MAX_BATCH_SIZE = 8 * 1024  # 8KB

CONFIG_PATH = "config.ini"

EXIT_ERROR = 1


class Client:

    def __init__(self, config_path: str = CONFIG_PATH):
        self.config = self.initialize_config(config_path)
        self.initialize_log()

    def initialize_config(self, config_path: str) -> dict:  # type: ignore[no-untyped-def]
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
        config.read(config_path)

        config_params = {}
        try:
            config_params["port"] = int(os.getenv("PORT", config["DEFAULT"]["PORT"]))
            config_params["server_ip"] = os.getenv(
                "SERVER_IP", config["DEFAULT"]["SERVER_IP"]
            )
            config_params["server_port"] = int(
                os.getenv("SERVER_PORT", config["DEFAULT"]["SERVER_PORT"])
            )
            config_params["data_dir"] = os.getenv(
                "DATA_DIR", config["DEFAULT"]["DATA_DIR"]
            )
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

    def initialize_log(self) -> None:
        """
        Python custom logging initialization

        Current timestamp is added to be able to identify in docker
        compose logs the date when the log has arrived
        """
        logging_level = self.config["logging_level"]
        logging.basicConfig(
            format="%(asctime)s %(levelname)-8s %(message)s",
            level=logging_level,
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    def handshake_with_server(self) -> tuple[str, int, str]:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            client_socket.connect(
                (self.config["server_ip"], self.config["server_port"])
            )

            client_protocol = ClientProtocol(client_socket)

            dispatcher_ip, dispatcher_port, request_id = (
                client_protocol.request_upload()
            )

            logging.info(
                f"Dispatcher: {dispatcher_ip}:{dispatcher_port}, request_id: {request_id}"
            )

            return dispatcher_ip, dispatcher_port, request_id

        finally:
            try:
                client_socket.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            client_socket.close()

    def upload(self) -> str:
        logging.info("uploading files...")

        data_dir = Path(self.config["data_dir"])
        csv_paths = [
            data_dir / "stores" / "stores.csv",
            data_dir / "transactions" / "transactions.csv",
            data_dir / "users" / "users.csv",
            # TODO: Agregar más archivos según sea necesario
        ]

        dispatcher_ip, dispatcher_port, request_id = self.handshake_with_server()

        dispatcher_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            dispatcher_socket.connect((dispatcher_ip, dispatcher_port))

            client_protocol = ClientProtocol(dispatcher_socket)

            client_protocol.define_queries(["1", "2", "3", "4"])

            for path in csv_paths:
                client_protocol.process_and_send_file(path)

            client_protocol.end_transmission()
            logging.info("transmission completed")

            return request_id

        finally:
            try:
                dispatcher_socket.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            dispatcher_socket.close()

    def get_results(self, request_id: str) -> None:
        logging.info(f"requesting results with id: {request_id}")

        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            client_socket.connect(
                (self.config["server_ip"], self.config["server_port"])
            )

            client_protocol = ClientProtocol(client_socket)

            results = client_protocol.request_results(request_id)

            for item in results:
                print(item.decode("utf-8"))

        except Exception as e:
            logging.error(f"Error: {e}")
            raise
        finally:
            try:
                client_socket.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            client_socket.close()

    def run(self) -> None:
        try:
            request_id = self.upload()

            self.get_results(request_id)

        except Exception as e:
            logging.exception(f"Error: {e}")
            sys.exit(EXIT_ERROR)
