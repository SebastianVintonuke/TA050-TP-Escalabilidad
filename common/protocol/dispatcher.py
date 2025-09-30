import logging
import socket
from typing import Callable, List, Union, Optional

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
from middleware.src.join_tasks_middleware import JoinTasksMiddleware
from middleware.src.routing.csv_message import CSVMessageBuilder, CSVHashedMessageBuilder
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
        join_middleware = JoinTasksMiddleware(1)

        batch = self._batch_protocol.wait_batch()
        last_model: Optional[Model] = None
        while batch: # While files
            header = batch.pop(0)
            model = Model.model_for(header)

            if model != last_model and last_model is not None:
                self.__send_EOF_for(user_id, last_model, select_middleware, join_middleware)

            last_model = model

            logging.info(f"action: receive_file | result: in_progress | data_type: {model.__name__}")

            while batch: # While batch of file
                if model is Transaction:
                    self.__send_task_to_select_transaction(user_id, select_middleware, model, batch)
                elif model is TransactionItem:
                    self.__send_task_to_select_transaction_item(user_id, select_middleware, model, batch)
                elif model is MenuItem:
                    self.__send_task_to_join_menu_item(user_id, join_middleware, model, batch)
                elif model is User:
                    self.__send_task_to_join_user(user_id, join_middleware, model, batch)
                elif model is Store:
                    self.__send_task_to_join_store(user_id, join_middleware, model, batch)
                else:
                    raise Exception(f"Unknown model: {model}")

                batch = self._batch_protocol.wait_batch() # End of batch
            batch = self._batch_protocol.wait_batch() # End of file

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
        transaction_item_task = CSVMessageBuilder([user_id], ["query_2"])
        for line in batch:
            transaction_item: TransactionItem = model.from_bytes_and_project(line)
            transaction_item_task.add_row([
                transaction_item.item_id,
                transaction_item.created_at.year,
                transaction_item.created_at.month,
                transaction_item.subtotal,
            ])
        select_middleware.send(transaction_item_task)

    @staticmethod
    def __send_task_to_join_menu_item(user_id: str, join_middleware: JoinTasksMiddleware, model: MenuItem, batch: List[bytes]) -> None:
        menu_item_task = CSVHashedMessageBuilder([user_id], ["query_product_names"], user_id)
        for line in batch:
            menu_item: MenuItem = model.from_bytes_and_project(line)
            menu_item_task.add_row([
                menu_item.item_id,
                menu_item.item_name,
            ])
        join_middleware.send(menu_item_task)

    @staticmethod
    def __send_task_to_join_user(user_id: str, join_middleware: JoinTasksMiddleware, model: User,
                                  batch: List[bytes]) -> None:
        user_task = CSVHashedMessageBuilder([user_id], ["query_users"], user_id)
        for line in batch:
            user: User = model.from_bytes_and_project(line)
            user_task.add_row([
                user.user_id,
                user.birthdate,
            ])
        join_middleware.send(user_task)

    @staticmethod
    def __send_task_to_join_store(user_id: str, join_middleware: JoinTasksMiddleware, model: Store, batch: List[bytes]) -> None:
        store_task = CSVHashedMessageBuilder([user_id], ["query_store_names"], user_id)
        for line in batch:
            store: Store = model.from_bytes_and_project(line)
            store_task.add_row([
                store.store_id,
                store.store_name,
            ])
        join_middleware.send(store_task)

    @staticmethod
    def __send_EOF_for(user_id: str, model: Model, select_middleware: SelectTasksMiddleware, join_middleware: JoinTasksMiddleware):
        if model is Transaction:
            logging.info(f"EOF FOR TRANSACTIONS")
            eof_task = CSVMessageBuilder([user_id, user_id, user_id],
                                         ["query_1", "query_3", "query_4"])
            eof_task.set_as_eof()
            eof_task.set_finish_signal()
            select_middleware.send(eof_task)
        elif model is TransactionItem:
            logging.info(f"EOF FOR TRANSACTIONS_ITEMS")
            eof_task = CSVMessageBuilder([user_id],
                                         ["query_2"])
            eof_task.set_as_eof()
            eof_task.set_finish_signal()
            select_middleware.send(eof_task)

        elif model is MenuItem:
            logging.info(f"EOF FOR MENU_ITEMS")
            eof_product_task = CSVHashedMessageBuilder([user_id], ["query_product_names"], user_id)
            eof_product_task.set_as_eof()
            eof_product_task.set_finish_signal()
            join_middleware.send(eof_product_task)
    
        elif model is User:
            logging.info(f"EOF FOR USER")
            eof_user_task = CSVHashedMessageBuilder([user_id], ["query_users"], user_id)
            eof_user_task.set_as_eof()
            eof_user_task.set_finish_signal()
            join_middleware.send(eof_user_task)

        elif model is Store:
            logging.info(f"EOF FOR STORES")
            eof_store_task = CSVHashedMessageBuilder([user_id], ["query_store_names"], user_id)
            eof_store_task.set_as_eof()
            eof_store_task.set_finish_signal()
            join_middleware.send(eof_store_task)