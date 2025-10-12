import logging
import socket
import threading
from typing import Callable, List, Union, Optional

from common.models.menuitem import MenuItem
from common.models.model import Model
from common.models.store import Store
from common.models.transaction import Transaction
from common.models.transactionitem import TransactionItem
from common.models.user import User

from common.protocol.byte import ByteProtocol
from common.protocol.signal import SignalProtocol
from common.protocol.batch import BatchProtocol

from middleware.src.join_tasks_middleware import JoinTasksMiddleware
from middleware.src.routing.csv_message import CSVMessageBuilder, CSVHashedMessageBuilder
from middleware.src.select_tasks_middleware import SelectTasksMiddleware

from common.utils import new_uuid, QueryId

class OutMiddleware:
    def __init__(self):
        self.select_middleware = SelectTasksMiddleware()
        self.join_middleware = JoinTasksMiddleware(2)


class Counter:
    def __init__(self):
        self.counter_transactions = 0
        self.counter_transactions_rows = 0
        self.counter_transaction_items = 0
        self.counter_menu_items = 0
        self.counter_user = 0
        self.counter_store = 0


class DispatcherProtocol:
    def __init__(self, a_socket: socket.socket):
        self._byte_protocol = ByteProtocol(a_socket)
        self._signal_protocol = SignalProtocol(a_socket)
        self._batch_protocol = BatchProtocol(a_socket)

        self._pending_threads = {
            Transaction: [],
            TransactionItem: [],
        }

        self._select_lock = threading.Lock()
        self.out_middleware = OutMiddleware()

    def close_with(self, closure_to_close: Callable[[socket.socket], None]) -> None:
        """
        Check ByteProtocol.close_with method
        """
        self._byte_protocol.close_with(closure_to_close)

    def _join_pending_threads(self, model: Optional[Model]) -> None:
        if model in (Transaction, TransactionItem):
            threads = self._pending_threads.get(model, [])
            for t in threads:
                t.join()
            threads.clear()


    def handle_requests(self) -> None:
        user_id = new_uuid()


        #select_middleware = SelectTasksMiddleware()
        #join_middleware = JoinTasksMiddleware(1)
        counter = Counter()
        batch = self._batch_protocol.wait_batch()
        last_model: Optional[Model] = None
        
        while batch: # While files
            header = batch.pop(0)
            model = Model.model_for(header)

            if last_model is None:
                last_model = model # Initialize it
            elif model != last_model:
                self._join_pending_threads(last_model)
                self.__send_EOF_for(user_id, last_model, counter)
                last_model = model # Only change it If it is new.

            logging.info(f"action: receive_file | result: in_progress | data_type: {model.__name__}")

            while batch: # While batch of file
                if model is Transaction:
                    #self.__send_task_to_select_transaction(user_id, model, batch)

                    self.__send_task_to_select_transaction(user_id, model, batch)
                    counter.counter_transactions_rows += len(batch)
                    counter.counter_transactions+=1
                elif model is TransactionItem:
                    #self.__send_task_to_select_transaction_item(user_id, model, batch)
                    self.__send_task_to_select_transaction_item(user_id, model, batch)
                    counter.counter_transaction_items+=1
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

                batch = self._batch_protocol.wait_batch() # End of batch
            batch = self._batch_protocol.wait_batch() # End of file

        self._join_pending_threads(last_model)
        # Allegedly not sent eof for very last model. 
        self.__send_EOF_for(user_id, last_model, counter)

        self._byte_protocol.send_bytes(user_id.encode())

    def __send_task_to_select_transaction(self, user_id: str, model: Transaction, batch: List[bytes]) -> None:
        transaction_task = CSVMessageBuilder.with_credentials([user_id], ["transactions"])

        for line in batch:
            transaction_task.add_row_bytes(model.parse_row(line))
        self.out_middleware.select_middleware.send(transaction_task)


    def __send_task_to_select_transaction_item(self, user_id: str, model: TransactionItem, batch: List[bytes]) -> None:
        transaction_item_task = CSVMessageBuilder.with_credentials([user_id], ["query_2"])
        for line in batch:
            transaction_item_task.add_row_bytes(model.parse_row(line))

        #logging.info(f"-->Sending len transaction item {transaction_item_task.len_payload()}")
        self.out_middleware.select_middleware.send(transaction_item_task)

    
    def __send_task_to_join_menu_item(self, user_id: str, model: MenuItem, batch: List[bytes]) -> None:
        menu_item_task = CSVHashedMessageBuilder.with_credentials([user_id], ["query_product_names"], user_id)
        for line in batch:
            menu_item_task.add_row_bytes(model.parse_row(line))
        self.out_middleware.join_middleware.send(menu_item_task)

    
    def __send_task_to_join_user(self, user_id: str, model: User,
                                  batch: List[bytes]) -> None:
        user_task = CSVHashedMessageBuilder.with_credentials([user_id], ["query_users"], user_id)
        for line in batch:
            user_task.add_row_bytes(model.parse_row(line))
            
        self.out_middleware.join_middleware.send(user_task)

    
    def __send_task_to_join_store(self, user_id: str, model: Store, batch: List[bytes]) -> None:
        store_task = CSVHashedMessageBuilder.with_credentials([user_id], ["query_store_names"], user_id)
        for line in batch:
            store: Store = model.from_bytes_and_project(line)
            store_task.add_row_bytes(model.parse_row(line))
        self.out_middleware.join_middleware.send(store_task)

    
    def __send_EOF_for(self, user_id: str, model: Model, counter: Counter):
        if model is Transaction:
            logging.info(f"EOF FOR TRANSACTIONS sent message count {counter.counter_transactions}  rows sent: {counter.counter_transactions_rows}")
            eof_task = CSVMessageBuilder.with_credentials([user_id, user_id, user_id],
                                         ["query_1", "query_3", "query_4"])
            eof_task.set_as_eof(count= counter.counter_transactions) # If set as 0 assumes all messages were sent. Since it checks if msg received < expected. If it is > then fine
            self.out_middleware.select_middleware.send(eof_task)
        elif model is TransactionItem:
            logging.info(f"EOF FOR TRANSACTIONS_ITEMS sent message count {counter.counter_transaction_items}")
            eof_task = CSVMessageBuilder.with_credentials([user_id],
                                         ["query_2"])
            eof_task.set_as_eof(counter.counter_transaction_items)
            self.out_middleware.select_middleware.send(eof_task)

        elif model is MenuItem:
            logging.info(f"EOF FOR MENU_ITEMS message count {counter.counter_menu_items}")
            eof_product_task = CSVHashedMessageBuilder.with_credentials([user_id], ["query_product_names"], user_id)
            eof_product_task.set_as_eof(counter.counter_menu_items)
            self.out_middleware.join_middleware.send(eof_product_task)
    
        elif model is User:
            logging.info(f"EOF FOR USER message count {counter.counter_user}")
            eof_user_task = CSVHashedMessageBuilder.with_credentials([user_id], ["query_users"], user_id)
            eof_user_task.set_as_eof(counter.counter_user)
            self.out_middleware.join_middleware.send(eof_user_task)

        elif model is Store:
            logging.info(f"EOF FOR STORES message count {counter.counter_store}")
            eof_store_task = CSVHashedMessageBuilder.with_credentials([user_id], ["query_store_names"], user_id)
            eof_store_task.set_as_eof(counter.counter_store)
            self.out_middleware.join_middleware.send(eof_store_task)