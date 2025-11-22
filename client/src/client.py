import errno
import logging
import socket
import time
from io import BufferedReader, BufferedWriter
from pathlib import Path
from types import FrameType
from typing import List, Optional, Tuple, Union

from common.protocol.client import ClientProtocol

MAX_ATTEMPTS = 1
MAX_ATTEMPTS_DOWNLOAD_RESULTS = 1
DEFAULT_EXECUTIONS = 1


class Client:
    @staticmethod
    def decode(address: str) -> Tuple[str, int]:
        host, port_str = address.rsplit(":", 1)
        port = int(port_str)
        return host, port

    @staticmethod
    def __try_close(a_socket: Optional[socket.socket], socket_name_to_log: str) -> None:
        """
        Attempt to gracefully close a given socket and log the result

        If the socket was already closed, for example by a signal, it will be silently ignored
        Any other errors will be logged
        """
        if a_socket is None:
            return

        try:
            a_socket.close()
            logging.info(f"action: close_{socket_name_to_log} | result: success")
        except OSError as e:
            if e.errno == errno.EBADF:
                pass  # The socket was already closed by the graceful shutdown
            else:
                logging.error(
                    f"action: close_{socket_name_to_log} | result: fail | error: {e}"
                )

    def __init__(self, server_address: str, input_dir: str, output_dir: str) -> None:
        self._server_address = server_address
        self._client_socket_to_server: Optional[socket.socket] = None
        self._client_socket_to_dispatcher: Optional[socket.socket] = None
        self._client_socket_to_results_storage: Optional[socket.socket] = None
        self._input_dir = Path(input_dir)
        self._output_dir = Path(output_dir)
        self._file_descriptors: List[Union[BufferedWriter, BufferedReader]] = []

    def create_client_socket(self, address: str) -> socket.socket:
        try:
            server_host, server_port = self.decode(address)
            return socket.create_connection((server_host, server_port))
        except Exception as e:
            logging.critical(
                f"action: connect | result: fail | address: {address} | error: {e}"
            )
            raise

    def start(self, executions: int = DEFAULT_EXECUTIONS) -> None:
        for current_execution in range(executions):
            logging.info(
                f"action: execution | result: in-progress | n°: {current_execution + 1}/{executions}"
            )
            self.exec()
        logging.info(
            f"action: all_executions_completed | total_executions: {executions}"
        )

    def exec(self) -> None:
        attempt = 0
        while attempt < MAX_ATTEMPTS:
            try:
                dispatcher_address = self._request_dispatcher_address()
                client_id = self._upload_data_to_dispatcher(dispatcher_address)
                results_storage_address = self._request_results_storage_address(client_id)
                self._download_results_with_retries(results_storage_address, client_id)
                logging.info("action: client_execution | result: success")
                return
            except Exception as e:
                logging.info(f"action: client_execution | result: fail | attempt n° {attempt + 1} | error: {e}")
                attempt += 1
                time.sleep(attempt * 1)
        logging.error("action: client_execution | result: fail | error: failed all attempts")

    def _request_dispatcher_address(self) -> str:
        """
            Se obtiene del servidor la dirección de un dispatcher
        """
        logging.info(f"action: request_dispatcher | result: in-progress")
        self._client_socket_to_server = self.create_client_socket(self._server_address)

        try:
            client_protocol_to_server = ClientProtocol(self._client_socket_to_server)
            dispatcher_address = client_protocol_to_server.request_dispatcher_address()
            self.__try_close(self._client_socket_to_server, 'socket_to_server')
            self._client_socket_to_server = None
            logging.info(
                f"action: request_dispatcher | result: success | dispatcher: {dispatcher_address}"
            )
            return dispatcher_address
        except Exception as e:
            logging.error(
                f"action: request_dispatcher | result: fail | error: {e}"
            )
            self.__try_close(self._client_socket_to_server, 'socket_to_server')
            self._client_socket_to_server = None
            raise

    def _upload_data_to_dispatcher(self, dispatcher_address: str) -> str:
        """
            Se suben los datos locales al dispatcher y este devuelve el ID asociado a la consulta
        """
        logging.info(f"action: upload_data | result: in-progress")
        self._client_socket_to_dispatcher = self.create_client_socket(dispatcher_address)

        try:
            client_protocol_to_dispatcher = ClientProtocol(self._client_socket_to_dispatcher)
            client_id = client_protocol_to_dispatcher.upload_files(
                self._input_dir, self.__open_input_file, self.__close_file
            )
            self.__try_close(self._client_socket_to_dispatcher, 'socket_to_dispatcher')
            self._client_socket_to_dispatcher = None
            logging.info(f"action: upload_data | result: success | client_id: {client_id}")
            return client_id
        except Exception as e:
            logging.error(
                f"action: upload_data | result: fail | error: {e}"
            )
            self.__try_close(self._client_socket_to_dispatcher, 'socket_to_dispatcher')
            self._client_socket_to_dispatcher = None
            raise

    def _request_results_storage_address(self, client_id: str) -> str:
        """
            Se obtiene del servidor la dirección de un results storage
        """
        logging.info(f"action: request_results_storage | result: in-progress")
        self._client_socket_to_server = self.create_client_socket(self._server_address)

        try:
            client_protocol_to_server = ClientProtocol(self._client_socket_to_server)
            results_storage_address = client_protocol_to_server.request_results_storage_address(client_id)
            self.__try_close(self._client_socket_to_server, 'socket_to_server')
            self._client_socket_to_server = None
            logging.info(
                f"action: request_results_storage | result: success | results_storage: {results_storage_address}"
            )
            return results_storage_address
        except Exception as e:
            logging.error(
                f"action: request_results_storage | result: fail | error: {e}"
            )
            self.__try_close(self._client_socket_to_server, 'socket_to_server')
            self._client_socket_to_server = None
            raise

    def _download_results_with_retries(self, results_storage_address: str, client_id: str) -> None:
        """
            Descargo los resultados del results storage

            Como los datos fueron subidos al servidor sin errores y este garantiza que estarán disponibles,
            si ocurre un error de red en la transferencia se reintenta hasta MAX_ATTEMPTS
            para evitar que el error se propague y el usuario tenga que comenzar la operacion desde el comienzo
        """
        logging.info("action: download_results | result: in-progress")
        attempt = 0
        last_error: Optional[Exception] = None
        while attempt < MAX_ATTEMPTS_DOWNLOAD_RESULTS:
            try:
                self._client_socket_to_results_storage = self.create_client_socket(results_storage_address)

                client_protocol_to_results_storage = ClientProtocol(self._client_socket_to_results_storage)
                client_protocol_to_results_storage.download_results(
                    self._output_dir,
                    client_id,
                    self.__open_output_file,
                    self.__close_file,
                )
                self.__try_close(self._client_socket_to_results_storage, 'socket_to_results_storage')
                self._client_socket_to_results_storage = None
                logging.info("action: download_results | result: success")
                return
            except Exception as e:
                last_error = e
                logging.error(
                    f"action: download_results | result: fail | attempt n°: {attempt + 1} | error: {e}"
                )
                attempt += 1
                time.sleep(attempt * 1)
                self.__try_close(self._client_socket_to_results_storage, 'socket_to_results_storage')
                self._client_socket_to_results_storage = None

        logging.error("action: download_results | result: fail | error: failed all attempts")
        raise last_error

    def graceful_shutdown(
        self, _signal_number: int, _current_stack_frame: Optional[FrameType]
    ) -> None:
        """
        On a signal, gracefully shutdown the client

        Closes the client socket and close the file descriptors
        """
        self.__try_close(self._client_socket_to_server, 'socket_to_server')
        self._client_socket_to_server = None
        self.__try_close(self._client_socket_to_dispatcher, 'socket_to_dispatcher')
        self._client_socket_to_dispatcher = None
        self.__try_close(self._client_socket_to_results_storage, 'socket_to_results_storage')
        self._client_socket_to_results_storage = None

        for file_descriptor in list(self._file_descriptors):
            try:
                file_descriptor.close()
            except Exception:
                pass
        self._file_descriptors.clear()

        logging.info("action: exit | result: success")

    def __open_input_file(self, file_path: Path) -> BufferedReader:
        reader = file_path.open("rb")
        self._file_descriptors.append(reader)
        return reader

    def __open_output_file(self, file_path: Path) -> BufferedWriter:
        writer = file_path.open("wb")
        self._file_descriptors.append(writer)
        return writer

    def __close_file(
        self, file_descriptor: Union[BufferedWriter, BufferedReader]
    ) -> None:
        self._file_descriptors.remove(file_descriptor)
        file_descriptor.close()
