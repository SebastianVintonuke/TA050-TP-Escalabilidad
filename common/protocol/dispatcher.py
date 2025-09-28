import socket
from typing import Callable

from common.models.menuitem import MenuItem
from common.models.model import Model
from common.models.store import Store
from common.models.transaction import Transaction
from common.models.transactionitem import TransactionItem
from common.models.user import User
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
        select_middleware = SelectTasksMiddleware()

        batch = self._batch_protocol.wait_batch()
        while batch:
            header = batch.pop(0)
            model = Model.model_for(header)

            while batch: # While file
                if model is Transaction:
                    select_task = CSVMessageBuilder([user_id], ["query_1"])
                    for line in batch: # While batch
                        transaction = model.from_bytes_and_project(line)
                        select_task.add_row([
                            transaction.transaction_id,
                            transaction.created_at.year,
                            transaction.created_at.hour,
                            transaction.final_amount,
                        ])
                    select_middleware.send(select_task)
                elif model is TransactionItem:
                    pass # Mandar a select
                elif model is MenuItem:
                    pass # Mandar a join
                elif model is User:
                    pass # Mandar a join
                elif model is Store:
                    pass # Mandar a join
                else:
                    raise Exception(f"Unknown model: {model}")

                batch = self._batch_protocol.wait_batch() # End of batch
            batch = self._batch_protocol.wait_batch() # End of file

        self._byte_protocol.send_bytes(user_id.encode())
