import errno
import logging
import os
import socket
import threading
from configparser import ConfigParser
from types import FrameType
from typing import Dict, List, Optional

from common.models.model import Model
from common.protocol.clientserver import ClientDispatcherOperation, DispatcherProtocol


class Dispatcher:
    def __init__(self, config_path: str = "config.ini"):
        self.config = self.initialize_config(config_path)
        self.initialize_logging()

        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind(("", self.config["port"]))
        self._server_socket.listen(self.config["listen_backlog"])

        self._was_stopped = False

    def initialize_config(self, config_path: str) -> dict:
        config = ConfigParser(os.environ)
        config.read(config_path)

        config_params = {}
        try:
            config_params["server_host"] = os.getenv(
                "SERVER_HOST", config["DEFAULT"].get("SERVER_HOST", "server")
            )
            config_params["server_port"] = int(
                os.getenv("SERVER_PORT", config["DEFAULT"].get("SERVER_PORT", "8080"))
            )

            config_params["port"] = int(
                os.getenv("PORT", config["DEFAULT"].get("PORT", "12347"))
            )
            config_params["listen_backlog"] = int(
                os.getenv(
                    "LISTEN_BACKLOG", config["DEFAULT"].get("LISTEN_BACKLOG", "1")
                )
            )

            config_params["logging_level"] = os.getenv(
                "LOGGING_LEVEL", config["DEFAULT"].get("LOGGING_LEVEL", "INFO")
            )
        except KeyError as e:
            raise KeyError(
                "Key was not found. Error: {} .Aborting dispatcher".format(e)
            )
        except ValueError as e:
            raise ValueError(
                "Key could not be parsed. Error: {}. Aborting dispatcher".format(e)
            )

        return config_params

    def initialize_logging(self) -> None:
        logging_level = self.config["logging_level"]
        logging.basicConfig(
            format="%(asctime)s %(levelname)-8s %(message)s",
            level=logging_level,
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    def handle_client_connection(self, client_socket: socket.socket) -> None:
        dispatcher_protocol = DispatcherProtocol(client_socket)

        queries = []
        current_file = None
        file_models = {}

        try:
            while True:
                op_code, data = dispatcher_protocol.wait_command()

                if op_code == ClientDispatcherOperation.EndTransmission.value:
                    break

                elif op_code == ClientDispatcherOperation.DefineQueries.value:
                    if data:
                        queries = data[0].decode().split(",")

                elif op_code == ClientDispatcherOperation.FileHeader.value:
                    if len(data) >= 2:
                        filename = data[0].decode()
                        header = data[1]

                        model_cls = Model.model_for(header)

                        current_file = filename
                        file_models[filename] = {
                            "header": header,
                            "model": model_cls,
                            "data": [],
                        }

                elif op_code == ClientDispatcherOperation.FileBatch.value:
                    if current_file is None:
                        continue

                    for item in data:
                        try:
                            model = file_models[current_file]["model"].from_bytes(item)
                            file_models[current_file]["data"].append(model)
                        except Exception as e:
                            logging.error(f"Error processing register: {e}")

                elif op_code == ClientDispatcherOperation.Error.value:
                    logging.error("Error in client command")

            # process data
            if queries and file_models:
                processing_thread = threading.Thread(
                    target=self.process_data, args=(queries, file_models)
                )
                processing_thread.daemon = True
                processing_thread.start()

        except Exception as e:
            logging.exception(f"Error processing in dispatcher: {e}")
        finally:
            try:
                client_socket.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            client_socket.close()

    def process_data(self, queries: List[str], file_models: Dict) -> None:
        # TODO: Processing logic with nodes
        pass

    def graceful_shutdown(
        self, _signal_number: int, _current_stack_frame: Optional[FrameType]
    ) -> None:
        self._was_stopped = True

        try:
            self._server_socket.close()
        except Exception as e:
            logging.error(f"Error: {e}")

    def run(self) -> None:
        while not self._was_stopped:
            try:
                client_socket, addr = self._server_socket.accept()

                self.handle_client_connection(client_socket)

            except OSError as e:
                if e.errno == errno.EBADF and self._was_stopped:
                    break
                else:
                    logging.error(f"Error accepting connection in dispatcher: {e}")
