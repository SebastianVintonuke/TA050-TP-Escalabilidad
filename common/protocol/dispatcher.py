import socket
from typing import Callable

from .byte import ByteProtocol
from .signal import SignalProtocol
from .batch import BatchProtocol

class DispatcherProtocol:
    def __init__(self, a_socket: socket.socket):
        self._byte_protocol = ByteProtocol(a_socket)
        self._signal_protocol = SignalProtocol(a_socket)
        self._batch_protocol = BatchProtocol(a_socket)

    def close_with(self, closure_to_close: Callable[[socket.socket], None]) -> None:
        """
        Check ByteProtocol.close_with method
        """
        self._byte_protocol.close_with(closure_to_close)

    def handle_requests(self) -> None:
        pass