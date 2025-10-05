import logging
import random
import socket
from enum import Enum
from typing import Callable, List

from common.protocol.byte import ByteProtocol
from common.protocol.signal import SignalProtocol
from common.utils import stable_hash


class ServerOperations(int, Enum):
    AssignDispatcherAddress = 0x01
    AssignResultsStorageAddress = 0x02

class ServerProtocol:
    def __init__(self, a_socket: socket.socket, dispatchers: List[str], results_stores: List[str]) -> None:
        self._byte_protocol = ByteProtocol(a_socket)
        self._signal_protocol = SignalProtocol(a_socket)
        self._dispatchers: List[str] = sorted(dispatchers)
        self._results_storages: List[str] = sorted(results_stores)

    def close_with(self, closure_to_close: Callable[[socket.socket], None]) -> None:
        """
        Check ByteProtocol.close_with method
        """
        self._byte_protocol.close_with(closure_to_close)

    def handle_requests(self) -> None:
        operation_code = self._byte_protocol.wait_uint8()
        if operation_code == ServerOperations.AssignDispatcherAddress:
            self.__assign_dispatcher_address()
        elif operation_code == ServerOperations.AssignResultsStorageAddress:
            self.__assign_results_storage_address()
        else:
            raise ValueError(f"Unknown operation code {operation_code}")

    def __assign_dispatcher_address(self) -> None:
        address: str = random.choice(self._dispatchers)
        self._byte_protocol.send_bytes(address.encode("utf-8"))
        self._signal_protocol.wait_signal()
        logging.info(f"action: assign_dispatcher_address | result: success | assigned: {address}")

    def __assign_results_storage_address(self) -> None:
        client_id = self._byte_protocol.wait_bytes().decode("utf-8")
        index = stable_hash(client_id) % len(self._results_storages)
        address = self._results_storages[index]
        self._byte_protocol.send_bytes(address.encode("utf-8"))
        self._signal_protocol.wait_signal()
        logging.info(f"action: assign_results_storage_address | result: success | assigned: {address}")