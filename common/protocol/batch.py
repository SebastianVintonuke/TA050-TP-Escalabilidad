import socket
from typing import Callable, List

from .byte import ByteProtocol

class BatchProtocol:
    _MAX_BATCH_SIZE = 8 * 1024  # 8 kB

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
        if len(batch) > 255:
            raise ValueError("Batch size too large for uint8")

        total_size = sum(len(item) for item in batch)
        if total_size > self._MAX_BATCH_SIZE:
            raise ValueError(
                f"Batch total size {total_size} bytes exceeds maximum {self._MAX_BATCH_SIZE}kB"
            )

        self._byte_protocol.send_uint8(len(batch))
        for item in batch:
            self._byte_protocol.send_bytes(item)

    def wait_batch(self) -> List[bytes]:
        """
        Wait for a batch of bytes arrays
        Reads batch `size` and returns them as a list.
        """
        batch = []
        size = self._byte_protocol.wait_uint8()
        for item in range(0, size):
            batch.append(self._byte_protocol.wait_bytes())
        return batch