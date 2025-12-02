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
from concurrent.futures import ThreadPoolExecutor

NO_NEXT_BATCH_TYPE = 255

import threading
import queue


class MessageCounter:
    def __init__(self, count = 0):
        self.count_messages = count

    def clone(self):
        return MessageCounter(self.count_messages)

class Counter:
    def __init__(self):
        self.counter_transactions = 0
        self.counter_transaction_items = 0
        self.counter_menu_items = 0
        self.counter_user = 0
        self.counter_store = 0

    def clone(self):
        res = Counter()
        res.counter_transactions = self.counter_transactions
        res.counter_transaction_items = self.counter_transaction_items
        res.counter_menu_items = self.counter_menu_items
        res.counter_user = self.counter_user
        res.counter_store = self.counter_store
        return res




TYPE_ACTIONS_INDEXS = [
    Transaction,
    TransactionItem,
    MenuItem,
    User,
    Store
]

DEF_IND_EOF_SEND = -1

class MessagePublisher:
    def __init__(self, out_middleware, ind = 0):
        self.queue = queue.Queue()
        self.out_middleware = out_middleware
        self.thread = None
        self._running = threading.Event()
        self.ind = ind
        self.type_action_map = [
            self.__send_task_to_select_transaction,
            self.__send_task_to_select_transaction_item,
            self.__send_task_to_join_menu_item,
            self.__send_task_to_join_user,
            self.__send_task_to_join_store,
            self.__send_eof_for
        ]

    def graceful_stop(self):
        self.queue.put(None)
        self.thread.join()
        self._running.clear()

    def full_abort(self, user_id):
        self._running.clear()
        self.thread.join()
        self.out_middleware.send_abort_for(user_id)


    def force_abort(self):
        self._running.clear()
        self.thread.join()

    def close(self):
        self._running.clear()
        self.thread.join()
        self.out_middleware.select_middleware.close()
        self.out_middleware.join_middleware.close()


    def start(self):
        if self._running.is_set():
            return

        self._running.set()

        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()


    def run(self):
        try:
            while self._running.is_set():
                params = self.queue.get()
                if params == None:
                    logging.info(f"Graceful stop of MessagePublisher {self.ind} ")
                    return

                # action_type, user_id, model, batch, pck_id
                if len(params) ==5:
                    logging.info(f"MessagePublisher {self.ind} should send '{params[0]}' {params[1]} {params[2]} len:{len(params[3])}, {params[4]}")
                else:
                    logging.info(f"MessagePublisher {self.ind} should send '{params}'")

                action_type= params[0]

                self.type_action_map[action_type](*params[1:])
        except Exception as e:
            logging.error(f"Error at MessagePublisher {self.ind}: {e}")

    def __send_task_to_select_transaction(self, user_id: str, model: Transaction, batch: List[bytes], pck_id: int) -> None:
        transaction_task = CSVMessageBuilder.with_credentials([user_id], ["transactions"], pck_id)
        for line in batch:
            transaction_task.add_row_bytes(model.parse_row(line))
        self.out_middleware.select_middleware.send(transaction_task)


    def __send_task_to_select_transaction_item(self, user_id: str, model: TransactionItem, batch: List[bytes], pck_id: int) -> None:
        transaction_item_task = CSVMessageBuilder.with_credentials([user_id], ["query_2"], pck_id)
        for line in batch:
            transaction_item_task.add_row_bytes(model.parse_row(line))
        self.out_middleware.select_middleware.send(transaction_item_task)


    def __send_task_to_join_menu_item(self, user_id: str, model: MenuItem, batch: List[bytes], pck_id: int) -> None:
        menu_item_task = CSVHashedMessageBuilder.with_credentials([user_id], ["query_product_names"], user_id, pck_id)
        for line in batch:
            menu_item_task.add_row_bytes(model.parse_row(line))
        self.out_middleware.join_middleware.send(menu_item_task)


    def __send_task_to_join_user(self, user_id: str, model: User, batch: List[bytes], pck_id: int) -> None:
        user_task = CSVHashedMessageBuilder.with_credentials([user_id], ["query_users"], user_id, pck_id)
        for line in batch:
            user_task.add_row_bytes(model.parse_row(line))
        self.out_middleware.join_middleware.send(user_task)


    def __send_task_to_join_store(self, user_id: str, model: Store, batch: List[bytes], pck_id: int) -> None:
        store_task = CSVHashedMessageBuilder.with_credentials([user_id], ["query_store_names"], user_id, pck_id)
        for line in batch:
            store_task.add_row_bytes(model.parse_row(line))
        self.out_middleware.join_middleware.send(store_task)    




    def __send_eof_for(self, user_id: str, model: Model, counter: MessageCounter):
        if model is Transaction:
            logging.info(f"action: eof_transaction | result: success | count: {counter.count_messages}")
            eof_task = CSVMessageBuilder.with_credentials([user_id, user_id, user_id], ["query_1", "query_3", "query_4"], counter.count_messages)
            eof_task.set_as_eof() # If set as 0 assumes all messages were sent. Since it checks if msg received < expected. If it is > then fine
            self.out_middleware.select_middleware.send(eof_task)

        elif model is TransactionItem:
            logging.info(f"action: eof_transaction_item | result: success | count: {counter.count_messages}")
            eof_task = CSVMessageBuilder.with_credentials([user_id],["query_2"],counter.count_messages)
            eof_task.set_as_eof()
            self.out_middleware.select_middleware.send(eof_task)

        elif model is MenuItem:
            logging.info(f"action: eof_menu_item | result: success | count: {counter.count_messages}")
            eof_product_task = CSVHashedMessageBuilder.with_credentials([user_id], ["query_product_names"], user_id, counter.count_messages)
            eof_product_task.set_as_eof()
            self.out_middleware.join_middleware.send(eof_product_task)
    
        elif model is User:
            logging.info(f"action: eof_user | result: success | count: {counter.count_messages}")
            eof_user_task = CSVHashedMessageBuilder.with_credentials([user_id], ["query_users"], user_id, counter.count_messages)
            eof_user_task.set_as_eof()
            self.out_middleware.join_middleware.send(eof_user_task)

        elif model is Store:
            logging.info(f"action: eof_store | result: success | count: {counter.count_messages}")
            eof_store_task = CSVHashedMessageBuilder.with_credentials([user_id], ["query_store_names"], user_id, counter.count_messages)
            eof_store_task.set_as_eof()
            self.out_middleware.join_middleware.send(eof_store_task)



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




class DispatcherProtocol:
    def __init__(self, a_socket: socket.socket, node_id: int, client_count: int, state_storage: QueryStateStorage):
        self._byte_protocol = ByteProtocol(a_socket)
        self._batch_protocol = BatchProtocol(a_socket)
        self._node_id = node_id
        self._client_count = client_count



        self._state_storage = state_storage

        self.publishers = [
            MessagePublisher(OutMiddleware()),
            MessagePublisher(OutMiddleware(), ind = 1)
        ]

        for send_thread in self.publishers:
            send_thread.start()



    def close_with(self, closure_to_close: Callable[[socket.socket], None]) -> None:
        """
        Check ByteProtocol.close_with method
        """
        self._byte_protocol.close_with(closure_to_close)

        for publisher in self.publishers:
            publisher.close()

    def handle_requests(self) -> None:
        user_id = f"{new_uuid()}_{self._node_id + self._client_count}"
        logging.info(f"action: handle_request | result: in-progress | user_id: {user_id}")
        try:
            self.__add_request_register_to_local_storage(user_id)
            self.__receive_files(user_id)

            for publisher in self.publishers:
                publisher.graceful_stop()

            self._byte_protocol.send_bytes(user_id.encode())
        except Exception as e:
            logging.error(f"action: handle_request | result: fail | error: {e}")
            self.publishers[0].full_abort(user_id)

            for publisher in self.publishers[1:]:
                publisher.force_abort()
        finally:
            self.__remove_request_register_from_local_storage(user_id)
            logging.info(f"action: handle_request | result: success")


    def __receive_files(self, user_id: str) -> None:
        count_rb_send_threads = len(self.publishers)
        curr_batch_type = self._byte_protocol.wait_uint8()
        map_batch_models = {}

        while curr_batch_type != NO_NEXT_BATCH_TYPE:
            file = self._batch_protocol.wait_batch()
            # logging.info(f"File for batch {curr_batch_type}: len: {len(file)}")
            
            if len(file) == 0:
                ## Ya termino el batch type 
                model, ind_publisher, counter = map_batch_models.get(curr_batch_type, (None, 0, None))
                logging.info(f"EOF For {curr_batch_type}: {model} {ind_publisher} {counter.count_messages}")

                if model:
                    # Put in task/send thread the eof task.
                    self.publishers[ind_publisher].queue.put((DEF_IND_EOF_SEND, user_id, model, counter.clone() ))
                    # self.__send_eof_for(user_id, model, counter)

                curr_batch_type = self._byte_protocol.wait_uint8()
                continue

            model, ind_publisher, counter = map_batch_models.get(curr_batch_type, (None, 0, None))

            if not model:
                
                header = file.pop(0)
                model = Model.model_for(header)
                counter = MessageCounter()

                ind_publisher = len(map_batch_models) % count_rb_send_threads

                map_batch_models[curr_batch_type] = (model, ind_publisher, counter)

                logging.info(f"Creating/parsing headers for {curr_batch_type}: {model} send_thread: {ind_publisher}")

            else:
                file.pop(0) # Skip headers...

            self.__receive_batches_file(user_id, model, ind_publisher, file, counter)
            
            # logging.info(f"Wait for next file/batch type... {user_id}")
            
            curr_batch_type = self._byte_protocol.wait_uint8()
        
        # logging.info(f"Finished files from user... {user_id}")


    def __receive_batches_file(self, user_id: str, model: Model, ind_publisher: int, batch: List[bytes], counter: Counter) -> None:
        publisher = self.publishers[ind_publisher].queue
        action_type = TYPE_ACTIONS_INDEXS.index(model)

        while len(batch) != 0:
            # self.__dispatch_batch(user_id, model, batch, counter)
            pck_id = counter.count_messages
            counter.count_messages+=1

            #action_type, user_id, model, batch, pck_id
            publisher.put((action_type, user_id, model, batch, pck_id))

            # logging.info(f"Wait for next batch in file... {user_id} {model}")
            batch = self._batch_protocol.wait_batch()

    def __add_request_register_to_local_storage(self, user_id: str) -> None:
        self._state_storage.register_query(user_id, "")
        logging.info(f"action: add_request_register | result: success")


    def __remove_request_register_from_local_storage(self, user_id: str) -> None:
        self._state_storage.unregister_query(user_id)
        logging.info(f"action: remove_request_register | result: success")
