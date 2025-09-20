import socket
from typing import Callable


class ByteProtocol:
    def __init__(self, a_socket: socket.socket) -> None:
        self._socket = a_socket

    def close_with(self, closure_to_close: Callable[[socket.socket], None]) -> None:
        """
        Double Dispatch
        Close this protocol's underlying socket using the given closer function

        :param closure_to_close: Function that accepts a socket and handles its closing logic
        """
        closure_to_close(self._socket)

    def send_bytes(self, buf: bytes) -> None:
        """
        Sends a byte array through the socket

        First, sends 2 bytes (big endian) that indicate the length, then sends the payload of that length
        If the byte array is too long raise a ValueError exception
        """
        payload_size = len(buf)
        if payload_size > 0xFFFF:
            raise ValueError("Payload too big, must fit in 2 bytes")
        size_bytes = payload_size.to_bytes(2, byteorder="big")
        self.__send_all(size_bytes + buf)

    def wait_bytes(self) -> bytes:
        """
        Receive a byte array from the socket

        First, reads 2 bytes (big endian) that indicate the length, then reads the payload of that length
        Returns the byte array
        """
        size_bytes = self.__recv_all(2)
        payload_size = int.from_bytes(size_bytes, byteorder="big")
        payload = self.__recv_all(payload_size)
        return payload

    def send_uint8(self, uint8: int) -> None:
        """
        Send a single unsigned byte (0-255) through the socket

        If it is not between 0 and 255 raise a ValueError exception
        """
        if not 0 <= uint8 <= 255:
            raise ValueError("Value must be an uint8 (0-255)")
        self.__send_all(bytes([uint8]))

    def wait_uint8(self) -> int:
        """
        Receive a single unsigned byte (0-255) through the socket
        """
        return int(self.__recv_all(1)[0])

    def __send_all(self, buf: bytes) -> None:
        """
        Send the entire contents of buf through the socket

        Retries until the entire buffer is sent
        Raises BrokenPipeError if the connection is closed unexpectedly
        """
        sz = 0
        while sz < len(buf):
            written = self._socket.send(buf[sz:])
            if not written:
                raise BrokenPipeError
            sz += written

    def __recv_all(self, n: int) -> bytes:
        """
        Read exactly n bytes from the socket

        Retries until the requested number of bytes is received
        Raises BrokenPipeError if the connection is closed unexpectedly
        """
        msg = b''
        while len(msg) < n:
            read = self._socket.recv(n - len(msg))
            if not read:
                raise BrokenPipeError
            msg += read
        return msg