import logging
import socket
from io import BufferedReader
from typing import Callable, List

from .byte import ByteProtocol
from .signal import SignalProtocol
from .batch import BatchProtocol

class ResultsProtocol:
    def __init__(self, a_socket: socket.socket) -> None:
        self._byte_protocol = ByteProtocol(a_socket)
        self._signal_protocol = SignalProtocol(a_socket)
        self._batch_protocol = BatchProtocol(a_socket)

    def close_with(self, closure_to_close: Callable[[socket.socket], None]) -> None:
        """
        Check ByteProtocol.close_with method
        """
        self._byte_protocol.close_with(closure_to_close)

    def handle_requests(
            self,
            assert_exists: Callable[[str], None],
            do_with_results_when_ready: Callable[[str, Callable[[BufferedReader], None]], None]
    ) -> None:
        client_id = self._byte_protocol.wait_bytes().decode("utf-8")
        try:
            assert_exists(client_id)
            self._signal_protocol.send_ack()
            do_with_results_when_ready(client_id, self._send_in_batches_and_eof)
        except Exception as err:
            self._signal_protocol.send_error(str(err))


    def _send_in_batches_and_eof(self, reader: BufferedReader) -> None:
        self._batch_protocol.send_all(reader)
        self._batch_protocol.send_batch([])