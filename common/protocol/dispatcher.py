import logging
import socket
from typing import Callable, List, Optional

from common.models.menuitem import MenuItem
from common.models.model import Model
from common.models.store import Store
from common.models.transaction import Transaction
from common.models.transactionitem import TransactionItem
from common.models.user import User

from common.protocol.byte import ByteProtocol
from common.protocol.signal import SignalProtocol
from common.protocol.batch import BatchProtocol
from common.state_storage.query_state_storage import QueryStateStorage
from dispatcher.src.dispatcher_state_handler import DispatcherNodeStateManager

from middleware.src.join_tasks_middleware import JoinTasksMiddleware
from middleware.src.routing.csv_message import CSVMessageBuilder, CSVHashedMessageBuilder
from middleware.src.select_tasks_middleware import SelectTasksMiddleware

from common.utils import new_uuid


class OutMiddleware:
    def __init__(self):
        self.select_middleware = SelectTasksMiddleware()
        self.join_middleware = JoinTasksMiddleware(2)

    def send_abort_for(self, user_id: str) -> None:
        eof_task = CSVMessageBuilder.with_credentials([user_id, user_id, user_id],["query_1", "query_3", "query_4"])
        eof_task.set_error()
        self.select_middleware.send(eof_task)

        eof_task = CSVMessageBuilder.with_credentials([user_id], ["query_2"])
        eof_task.set_error()
        self.select_middleware.send(eof_task)

        eof_product_task = CSVHashedMessageBuilder.with_credentials([user_id], ["query_product_names"], user_id)
        eof_task.set_error()
        self.join_middleware.send(eof_product_task)

        eof_user_task = CSVHashedMessageBuilder.with_credentials([user_id], ["query_users"], user_id)
        eof_task.set_error()
        self.join_middleware.send(eof_user_task)

        eof_store_task = CSVHashedMessageBuilder.with_credentials([user_id], ["query_store_names"], user_id)
        eof_task.set_error()
        self.join_middleware.send(eof_store_task)
        logging.info(f"action: abort | result: success | user_id: {user_id}")


class Counter:
    def __init__(self):
        self.counter_transactions = 0
        self.counter_transaction_items = 0
        self.counter_menu_items = 0
        self.counter_user = 0
        self.counter_store = 0


class DispatcherProtocol:
    def __init__(self, a_socket: socket.socket, node_id: int, client_count: int, state_storage: QueryStateStorage):
        self._byte_protocol = ByteProtocol(a_socket)
        self._signal_protocol = SignalProtocol(a_socket)
        self._batch_protocol = BatchProtocol(a_socket)
        self._node_id = node_id
        self._client_count = client_count
        self.out_middleware = OutMiddleware()
        self._state_storage = state_storage


    def close_with(self, closure_to_close: Callable[[socket.socket], None]) -> None:
        """
        Check ByteProtocol.close_with method
        """
        self._byte_protocol.close_with(closure_to_close)


    def handle_requests(self) -> None:
        user_id = f"{new_uuid()}_{self._node_id + self._client_count}"
        logging.info(f"action: handle_request | result: in-progress | user_id: {user_id}")
        try:
            self.__add_request_register_to_local_storage(user_id)
            self.__receive_files(user_id)
            self._byte_protocol.send_bytes(user_id.encode())
        except Exception as e:
            logging.error(f"action: handle_request | result: fail | error: {e}")
            self.out_middleware.send_abort_for(user_id)
        finally:
            self.__remove_request_register_from_local_storage(user_id)
            logging.info(f"action: handle_request | result: success")


    def __receive_files(self, user_id: str) -> None:
        counter = Counter()
        last_model: Optional[Model] = None
        file = self._batch_protocol.wait_batch()
        while len(file) != 0:
            header = file.pop(0)
            model = Model.model_for(header)
            if last_model is None:
                last_model = model
                logging.info(f"action: receive_files | result: in_progress | data_type: {model.__name__}")
            elif model != last_model:
                self.__send_eof_for(user_id, last_model, counter)
                last_model = model
                logging.info(f"action: receive_files | result: in_progress | data_type: {model.__name__}")
            self.__receive_batches(user_id, model, file, counter)
            file = self._batch_protocol.wait_batch()
        if last_model is not None:
            self.__send_eof_for(user_id, last_model, counter)


    def __receive_batches(self, user_id: str, model: Model, batch: List[bytes], counter: Counter) -> None:
        while len(batch) != 0:
            self.__dispatch_batch(user_id, model, batch, counter)
            batch = self._batch_protocol.wait_batch()


    def __dispatch_batch(self, user_id: str, model: Model, batch: List[bytes], counter: Counter) -> None:
        if model is Transaction:
            self.__send_task_to_select_transaction(user_id, model, batch)
            counter.counter_transactions += 1
        elif model is TransactionItem:
            self.__send_task_to_select_transaction_item(user_id, model, batch)
            counter.counter_transaction_items += 1
        elif model is MenuItem:
            self.__send_task_to_join_menu_item(user_id, model, batch)
            counter.counter_menu_items += 1
        elif model is User:
            self.__send_task_to_join_user(user_id, model, batch)
            counter.counter_user += 1
        elif model is Store:
            self.__send_task_to_join_store(user_id, model, batch)
            counter.counter_store += 1
        else:
            raise Exception(f"Unknown model: {model}")


    def __send_task_to_select_transaction(self, user_id: str, model: Transaction, batch: List[bytes]) -> None:
        transaction_task = CSVMessageBuilder.with_credentials([user_id], ["transactions"])
        for line in batch:
            transaction_task.add_row_bytes(model.parse_row(line))
        self.out_middleware.select_middleware.send(transaction_task)


    def __send_task_to_select_transaction_item(self, user_id: str, model: TransactionItem, batch: List[bytes]) -> None:
        transaction_item_task = CSVMessageBuilder.with_credentials([user_id], ["query_2"])
        for line in batch:
            transaction_item_task.add_row_bytes(model.parse_row(line))
        self.out_middleware.select_middleware.send(transaction_item_task)


    def __send_task_to_join_menu_item(self, user_id: str, model: MenuItem, batch: List[bytes]) -> None:
        menu_item_task = CSVHashedMessageBuilder.with_credentials([user_id], ["query_product_names"], user_id)
        for line in batch:
            menu_item_task.add_row_bytes(model.parse_row(line))
        self.out_middleware.join_middleware.send(menu_item_task)


    def __send_task_to_join_user(self, user_id: str, model: User, batch: List[bytes]) -> None:
        user_task = CSVHashedMessageBuilder.with_credentials([user_id], ["query_users"], user_id)
        for line in batch:
            user_task.add_row_bytes(model.parse_row(line))
        self.out_middleware.join_middleware.send(user_task)


    def __send_task_to_join_store(self, user_id: str, model: Store, batch: List[bytes]) -> None:
        store_task = CSVHashedMessageBuilder.with_credentials([user_id], ["query_store_names"], user_id)
        for line in batch:
            store_task.add_row_bytes(model.parse_row(line))
        self.out_middleware.join_middleware.send(store_task)


    def __send_eof_for(self, user_id: str, model: Model, counter: Counter):
        if model is Transaction:
            logging.info(f"action: eof_transaction | result: success | count: {counter.counter_transactions}")
            eof_task = CSVMessageBuilder.with_credentials([user_id, user_id, user_id], ["query_1", "query_3", "query_4"])
            eof_task.set_as_eof(count= counter.counter_transactions) # If set as 0 assumes all messages were sent. Since it checks if msg received < expected. If it is > then fine
            self.out_middleware.select_middleware.send(eof_task)

        elif model is TransactionItem:
            logging.info(f"action: eof_transaction_item | result: success | count: {counter.counter_transaction_items}")
            eof_task = CSVMessageBuilder.with_credentials([user_id],["query_2"])
            eof_task.set_as_eof(counter.counter_transaction_items)
            self.out_middleware.select_middleware.send(eof_task)

        elif model is MenuItem:
            logging.info(f"action: eof_menu_item | result: success | count: {counter.counter_menu_items}")
            eof_product_task = CSVHashedMessageBuilder.with_credentials([user_id], ["query_product_names"], user_id)
            eof_product_task.set_as_eof(counter.counter_menu_items)
            self.out_middleware.join_middleware.send(eof_product_task)
    
        elif model is User:
            logging.info(f"action: eof_user | result: success | count: {counter.counter_user}")
            eof_user_task = CSVHashedMessageBuilder.with_credentials([user_id], ["query_users"], user_id)
            eof_user_task.set_as_eof(counter.counter_user)
            self.out_middleware.join_middleware.send(eof_user_task)

        elif model is Store:
            logging.info(f"action: eof_store | result: success | count: {counter.counter_store}")
            eof_store_task = CSVHashedMessageBuilder.with_credentials([user_id], ["query_store_names"], user_id)
            eof_store_task.set_as_eof(counter.counter_store)
            self.out_middleware.join_middleware.send(eof_store_task)


    def __add_request_register_to_local_storage(self, user_id: str) -> None:
        self._state_storage.register_query(user_id, "")
        logging.info(f"action: add_request_register | result: success")


    def __remove_request_register_from_local_storage(self, user_id: str) -> None:
        self._state_storage.unregister_packet(user_id)
        logging.info(f"action: remove_request_register | result: success")
