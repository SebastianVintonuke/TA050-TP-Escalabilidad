import socket
from typing import Callable

from common.models.model import Model
from common.utils import new_uuid

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
        user_id = new_uuid()

        recv_batch = self._batch_protocol.wait_batch()
        
        while recv_batch:
            header = recv_batch.pop(0)

            model = Model.model_for(header)
            print(model)

            while recv_batch:
                for line in recv_batch:
                    print(model.from_bytes_and_project(line)) # Dispatch task (model)
                
                recv_batch = self._batch_protocol.wait_batch()

            recv_batch = self._batch_protocol.wait_batch()

        self._byte_protocol.send_bytes(user_id.encode())
