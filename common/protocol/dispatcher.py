import socket
from typing import Callable

from common.models.model import Model
from common.models.transaction import Transaction
from common.utils import new_uuid

from common.protocol.byte import ByteProtocol
from common.protocol.signal import SignalProtocol
from common.protocol.batch import BatchProtocol
from middleware.src.routing.csv_message import CSVMessageBuilder
from middleware.src.select_tasks_middleware import SelectTasksMiddleware


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
        middleware = SelectTasksMiddleware()

        msg_build = CSVMessageBuilder([user_id], ["query_1"])

        recv_batch = self._batch_protocol.wait_batch()
        while recv_batch:
            header = recv_batch.pop(0)
            model = Transaction

            while recv_batch:
                for line in recv_batch:
                    item: Transaction = model.from_bytes_and_project(line)
                    msg_build.add_row([item.transaction_id, item.created_at.year, item.created_at.hour, item.final_amount])
                    middleware.send(msg_build)

                recv_batch = self._batch_protocol.wait_batch()
            recv_batch = self._batch_protocol.wait_batch()

        self._byte_protocol.send_bytes(user_id.encode())
