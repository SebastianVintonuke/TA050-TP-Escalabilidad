import logging
import socket
from typing import Callable, List, Iterable

from .byte import ByteProtocol

class BatchProtocol:
    _MAX_BATCH_SIZE = 256 * 1024  # 8 kB
    _MAX_BATCH_COUNT = 8196

    def __init__(self, a_socket: socket.socket):
        self._byte_protocol = ByteProtocol(a_socket)

    def close_with(self, closure_to_close: Callable[[socket.socket], None]) -> None:
        """
        Check ByteProtocol.close_with method
        """
        self._byte_protocol.close_with(closure_to_close)

    def send_batch(self, batch: List[bytes]) -> None:
        """
        Send a batch of bytes arrays:
        - First send the batch size (as uint8)
        - Then send each item in the batch
        """
        if len(batch) > self._MAX_BATCH_COUNT:
            raise ValueError("Batch size too large for uint8")

        total_size = sum(len(item) for item in batch)
        if total_size > self._MAX_BATCH_SIZE:
            raise ValueError(
                f"Batch total size {total_size} bytes exceeds maximum {self._MAX_BATCH_SIZE}kB"
            )

        self._byte_protocol.send_uint32(len(batch))
        for item in batch:
            self._byte_protocol.send_all(item)

    def send_all(self, reader) -> None:
        buffer = bytearray(self._MAX_BATCH_SIZE)
        cached_start = 0  # How much of the buffer is already filled

        while True:
            # Read into the remaining space in buffer
            view = memoryview(buffer)[cached_start:]
            read_len = reader.readinto(view)
            if read_len == 0:
                break  # EOF

            total_len = cached_start + read_len
            tmp = buffer[:total_len]

            # Look for last newline within the buffer
            split_index = tmp.rfind(b'\n')
            if split_index == -1:
                self._byte_protocol.send_uint32(total_len)
                self._byte_protocol.send_all(tmp)
                cached_start = 0
                continue

            # Send everything up to and including the last full line
            self._byte_protocol.send_uint32(split_index+1)
            self._byte_protocol.send_all(tmp[:split_index+1])

            # Move the remainder (after the split) to the front of the buffer
            remaining = total_len - (split_index + 1)
            buffer[:remaining] = buffer[split_index + 1:total_len]
            cached_start = remaining


    def wait_batch(self) -> List[bytes]:
        """
        Wait for a batch of bytes arrays
        Reads batch `size` and returns them as a list.
        """
        size = self._byte_protocol.wait_uint32()
        if size == 0:
            return []

        batch = self._byte_protocol.recv_all(size)

        return [line for line in batch.split(b"\n") if line !=b""]