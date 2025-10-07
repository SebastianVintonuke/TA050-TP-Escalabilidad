import errno
import logging
import os
import socket
import time
from io import BufferedReader, BufferedWriter
from pathlib import Path
from types import FrameType
from typing import List, Optional, Tuple, Union

from common.protocol.client import ClientProtocol

MAX_ATTEMPTS = 3
NO_ATTEMPTS = 0
DEFAULT_EXECUTIONS = 1


class Client:
    @staticmethod
    def decode(address: str) -> Tuple[str, int]:
        host, port_str = address.split(":")
        port = int(port_str)
        return host, port

    @staticmethod
    def __try_close(a_socket: socket.socket, socket_name_to_log: str) -> None:
        """
        Attempt to gracefully close a given socket and log the result

        If the socket was already closed, for example by a signal, it will be silently ignored
        Any other errors will be logged
        """
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
        self._client_socket_to_server: Optional[socket.socket]
        self._client_socket_to_dispatcher: Optional[socket.socket]
        self._client_socket_to_results_storage: Optional[socket.socket]
        self._input_dir = Path(input_dir)
        self._output_dir = Path(output_dir)
        self._file_descriptors: List[Union[BufferedWriter, BufferedReader]] = []

    @staticmethod
    def open_file(file_path: Path) -> Optional[int]:
        try:
            return os.open(file_path, os.O_RDONLY)
        except Exception as e:
            logging.critical(f"action: open_file | result: fail | error: {e}")
            raise

    def create_client_socket(self, address: str) -> Optional[socket.socket]:
        try:
            server_host, server_port = self.decode(address)
            return socket.create_connection((server_host, server_port))
        except Exception as e:
            logging.critical(
                f"action: connect | result: fail | address: {address} | error: {e}"
            )
            return None

    def start(self, executions: int = DEFAULT_EXECUTIONS) -> None:
        for number_of_execution in range(executions):
            logging.info(
                f"action: start_execution | execution: {number_of_execution + 1}/{executions}"
            )

            # Flujo del cliente
            self.exec()

            # Pausa entre ejecuciones
            if number_of_execution < executions - 1:
                time.sleep(1)

        logging.info(
            f"action: all_executions_completed | total_executions: {executions}"
        )

    def exec(self) -> None:
        # 1. Obtengo del servidor la dirección de un dispatcher
        self._client_socket_to_server = self.create_client_socket(self._server_address)
        if not self._client_socket_to_server:
            return
        client_protocol_to_server = ClientProtocol(self._client_socket_to_server)
        dispatcher_address = client_protocol_to_server.request_dispatcher_address()
        self._client_socket_to_server.close()
        logging.info(
            f"action: request_dispatcher | result: success | dispatcher: {dispatcher_address}"
        )

        # 2. Subo los datos al dispatcher y obtengo mi id
        self._client_socket_to_dispatcher = self.create_client_socket(
            dispatcher_address
        )
        if not self._client_socket_to_dispatcher:
            return
        client_protocol_to_dispatcher = ClientProtocol(
            self._client_socket_to_dispatcher
        )
        client_id = client_protocol_to_dispatcher.upload_files(
            self._input_dir, self.__open_input_file, self.__close_file
        )
        self._client_socket_to_dispatcher.close()
        logging.info(f"action: data_upload | result: success | client_id: {client_id}")

        time.sleep(10)  # TODO sacar

        # 3. Obtengo del servidor la dirección de un results storage
        self._client_socket_to_server = self.create_client_socket(self._server_address)
        if not self._client_socket_to_server:
            return
        client_protocol_to_server = ClientProtocol(self._client_socket_to_server)
        results_storage_address = (
            client_protocol_to_server.request_results_storage_address(client_id)
        )
        self._client_socket_to_server.close()
        logging.info(
            f"action: request_results_storage | result: success | results_storage: {results_storage_address}"
        )

        # 4. Descargo los resultados del results storage
        self._client_socket_to_results_storage = self.create_client_socket(
            results_storage_address
        )
        if not self._client_socket_to_results_storage:
            return
        client_protocol_to_results_storage = ClientProtocol(
            self._client_socket_to_results_storage
        )
        logging.info("action: data_download | result: in-progress")

        attempts = MAX_ATTEMPTS
        while attempts > NO_ATTEMPTS:
            try:
                client_protocol_to_results_storage.download_results(
                    self._output_dir,
                    client_id,
                    self.__open_output_file,
                    self.__close_file,
                )
                logging.info("action: data_download | result: success")
                break
            except Exception as e:
                attempts -= 1
                logging.error(
                    f"action: data_download | result: failure | error: {e} | attempts left: {attempts}"
                )
                time.sleep(1)

        self._client_socket_to_results_storage.close()

        if attempts <= NO_ATTEMPTS:
            logging.info("action: data_download | result: failed all attempts")

    def graceful_shutdown(
        self, _signal_number: int, _current_stack_frame: Optional[FrameType]
    ) -> None:
        """
        On a signal, gracefully shutdown the client

        Closes the client socket and close the file descriptors
        """
        if self._client_socket_to_server:
            self._client_socket_to_server.close()
        if self._client_socket_to_dispatcher:
            self._client_socket_to_dispatcher.close()
        if self._client_socket_to_results_storage:
            self._client_socket_to_results_storage.close()

        for file_descriptor in self._file_descriptors:
            file_descriptor.close()

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
