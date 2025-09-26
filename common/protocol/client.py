import socket
import time
from io import BufferedWriter, BufferedReader
from pathlib import Path
from typing import Callable, Union

from common.protocol.batch import BatchProtocol
from common.protocol.byte import ByteProtocol
from common.protocol.signal import SignalProtocol
from common.protocol.server import ServerOperations


class ClientProtocol:
    @staticmethod
    def __is_valid_or_raise(address: str) -> None:
        """
        If the address is in the form <host:port> it does nothing
        Otherwise it throws a ValueError
        """
        try:
            host, port_str = address.split(":", 1)
        except ValueError:
            raise ValueError(f"Invalid address format: {address}")
        if not host:
            raise ValueError(f"Host part is empty: {address}")
        try:
            port = int(port_str)
        except ValueError:
            raise ValueError(f"Port is not a number: {address}")
        if not (1 <= port <= 65535):
            raise ValueError(f"Port out of range: {address}")

    def __init__(self, a_socket: socket.socket) -> None:
        self._byte_protocol = ByteProtocol(a_socket)
        self._signal_protocol = SignalProtocol(a_socket)
        self._batch_protocol = BatchProtocol(a_socket)

    def close_with(self, closure_to_close: Callable[[socket.socket], None]) -> None:
        """
        Check ByteProtocol.close_with method
        """
        self._byte_protocol.close_with(closure_to_close)

    def upload_files(self,
                     input_dir: Path,
                     open_file: Callable[[Path], BufferedReader],
                     close_file: Callable[[Union[BufferedWriter, BufferedReader]], None]
    ) -> str:
        """
        Upload the files to a dispatcher
        Return the client_id assigned by the dispatcher
        """

        time.sleep(10) # simulamos que toma tiempo subir el archivo
        return "test_user_id_"

    def download_results(self,
                         output_dir: Path,
                         client_id: str,
                         open_output_file: Callable[[Path], BufferedWriter],
                         close_file: Callable[[Union[BufferedWriter, BufferedReader]], None]
    ) -> None:
        """
        Receives a client_id from a previous upload operation
        Download the results from a results storage using the client_id
        """
        self._byte_protocol.send_bytes(client_id.encode("utf-8"))
        self._signal_protocol.wait_signal()

        dir_file_query_1 = output_dir / "query_1.csv"
        self.__try_download_results(dir_file_query_1, open_output_file, close_file)
        dir_file_query_2bs = output_dir / "query_2_best_selling.csv"
        self.__try_download_results(dir_file_query_2bs, open_output_file, close_file)
        dir_file_query_2mp = output_dir / "query_2_most_profits.csv"
        self.__try_download_results(dir_file_query_2mp, open_output_file, close_file)
        dir_file_query_3 = output_dir / "query_3.csv"
        self.__try_download_results(dir_file_query_3, open_output_file, close_file)
        dir_file_query_4 = output_dir / "query_4.csv"
        self.__try_download_results(dir_file_query_4, open_output_file, close_file)

    def request_dispatcher_address(self) -> str:
        """
        Request a dispatcher address to the server
        Return the address of a random dispatcher
        """
        self._byte_protocol.send_uint8(ServerOperations.AssignDispatcherAddress)
        try:
            address = self._byte_protocol.wait_bytes().decode("utf-8")
            self.__is_valid_or_raise(address)
            self._signal_protocol.send_ack()
            return address
        except Exception as e:
            self._signal_protocol.send_error(str(e))
            raise e

    def request_results_storage_address(self, client_id: str) -> str:
        """
        Receives a client_id, request a results storage address to the server
        Return the result storage address that contains the results associated with the client_id
        """
        self._byte_protocol.send_uint8(ServerOperations.AssignResultsStorageAddress)
        try:
            self._byte_protocol.send_bytes(client_id.encode("utf-8"))
            address = self._byte_protocol.wait_bytes().decode("utf-8")
            self.__is_valid_or_raise(address)
            self._signal_protocol.send_ack()
            return address
        except Exception as e:
            self._signal_protocol.send_error(str(e))
            raise e

    def __try_download_results(self,
                               dir_file_query: Path,
                               open_output_file: Callable[[Path], BufferedWriter],
                               close_file: Callable[[Union[BufferedWriter, BufferedReader]], None]
    ) -> None:
        file_query = open_output_file(dir_file_query)
        try:
            self.__receive_result_and_store_into(file_query)
        finally:
            close_file(file_query)

    def __receive_result_and_store_into(self, file: BufferedWriter) -> None:
        batch = self._batch_protocol.wait_batch()
        while batch:
            file.write(b"".join(batch))
            batch = self._batch_protocol.wait_batch()