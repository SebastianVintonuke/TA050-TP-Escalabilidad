import socket
from typing import Callable

from .byte import ByteProtocol

ERROR_CODE = 0x00
ACK_CODE = 0xFF

class SignalProtocol:
    def __init__(self, a_socket: socket.socket):
        self._byte_protocol = ByteProtocol(a_socket)

    def close_with(self, closure_to_close: Callable[[socket.socket], None]) -> None:
        """
        Check ByteProtocol.close_with method
        """
        self._byte_protocol.close_with(closure_to_close)

    def send_ack(self) -> None:
        """
        Send an ACK signal
        """
        self._byte_protocol.send_uint8(ACK_CODE)

    def send_error(self, error: str) -> None:
        """
        Send an ERROR signal with a message
        """
        self._byte_protocol.send_uint8(ERROR_CODE)
        self._byte_protocol.send_bytes(error.encode("utf-8"))

    def wait_signal(self) -> None:
        """
        Wait for a signal from the other side.
        - If ACK received → do nothing
        - If ERROR received → raise Exception with the message
        - Otherwise → raise Exception with unknown code
        """
        code = self._byte_protocol.wait_uint8()
        if code == ACK_CODE:
            return
        elif code == ERROR_CODE:
            raise Exception(self.__wait_error_msg())
        else:
            raise Exception(f"Unknown code: {code}")

    def __wait_error_msg(self) -> str:
        return self._byte_protocol.wait_bytes().decode("utf-8")