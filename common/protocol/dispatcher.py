import logging
import socket
from typing import Callable, List, Union

from common.middleware.middleware import MessageMiddlewareQueue
from common.middleware.tasks.result import ResultTask
from common.models.menuitem import MenuItem
from common.models.model import Model
from common.models.store import Store
from common.models.transaction import Transaction
from common.models.transactionitem import TransactionItem
from common.models.user import User
from common.utils import new_uuid, QueryId

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

        # Touch para crear el archivo resultado en el storage para cuando el usuario pregunte
        #results_middleware = MessageMiddlewareQueue("middleware", "results")
        #results_middleware.send(ResultTask(user_id, QueryId.Query1, False, False, []).to_bytes())

        select_middleware = SelectTasksMiddleware()

        batch = self._batch_protocol.wait_batch()
        while batch: # While files
            header = batch.pop(0)
            model = Model.model_for(header)
            logging.info(f"action: receive_file | result: in_progress | data_type: {model.__name__}")

            while batch: # While batch of file
                if model is Transaction:
                    self.__send_task_to_select_transaction(user_id, select_middleware, model, batch)
                if model is TransactionItem:
                    self.__send_task_to_select_transaction_item(user_id, select_middleware, model, batch)
                elif model is MenuItem:
                    pass # TODO Mandar a join
                elif model is User:
                    pass # TODO Mandar a join
                elif model is Store:
                    pass # TODO Mandar a join
                else:
                    raise Exception(f"Unknown model: {model}")

                batch = self._batch_protocol.wait_batch() # End of batch
            batch = self._batch_protocol.wait_batch() # End of file

        self.__send_task_eof_to_select(user_id, select_middleware)

        self._byte_protocol.send_bytes(user_id.encode())

    @staticmethod
    def __send_task_to_select_transaction(user_id: str, select_middleware: SelectTasksMiddleware, model: Transaction, batch: List[bytes]) -> None:
        transaction_task = CSVMessageBuilder([user_id], ["transactions"])
        for line in batch:
            transaction: Transaction = model.from_bytes_and_project(line)
            transaction_task.add_row([
                transaction.transaction_id,
                transaction.created_at.year,
                transaction.store_id,
                transaction.user_id,
                transaction.created_at.month,
                transaction.created_at.hour,
                transaction.final_amount,
            ])
        select_middleware.send(transaction_task)

    @staticmethod
    def __send_task_to_select_transaction_item(user_id: str, select_middleware: SelectTasksMiddleware, model: TransactionItem, batch: List[bytes]) -> None:
        transaction_item_task = CSVMessageBuilder([user_id], ["transactions_items"])
        for line in batch:
            transaction_item: TransactionItem = model.from_bytes_and_project(line)
            transaction_item_task.add_row([
                transaction_item.transaction_id,
                transaction_item.item_id,
                transaction_item.quantity,
                transaction_item.unit_price,
                transaction_item.created_at.year,
                transaction_item.created_at.month,
                transaction_item.created_at.hour,
            ])
        select_middleware.send(transaction_item_task)

    @staticmethod
    def __send_task_eof_to_select(user_id: str, select_middleware: SelectTasksMiddleware) -> None:
        eof_task = CSVMessageBuilder([user_id, user_id], ["query_1", "query_3"])
        eof_task.set_as_eof()
        select_middleware.send(eof_task)
