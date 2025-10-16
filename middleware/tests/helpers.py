import threading
import time
from typing import Any, Callable, Iterable

import queue

from middleware.src.routing.csv_message import CSVMessageBuilder,CSVMessage

def wrap_callback_with_ack_handling(original_callback: Callable[[Any], bool]) -> Callable[[Any], bool]:
    """Envuelve un callback y asegura ACK del mensaje después de ejecutarlo."""
    def _wrapped(headers, message: Any) -> bool:
        print(f"WRAPPED RECV MESSAGE {headers}")
        #message= CSVMessage(message)
        result: bool = original_callback(message)
        return False
    return _wrapped


def start_consumer_in_thread(middleware_creator: Any, callback: Callable[[Any], bool]) -> threading.Thread:
    signal_created = threading.Event()
    middleware= None
    def run_consumer(callback):
        middleware = middleware_creator()
        signal_created.set()
        middleware.start_consuming(callback)

    thread = threading.Thread(target=run_consumer, args=(callback,), daemon=True)
    thread.start()
    print("WAITING SIGNAL MIDDLEWARE CREATED")
    signal_created.wait()
    print("WAITING SOME TIME FOR START CONSUMING?")
    time.sleep(0.3)
    return (middleware, thread)

def build_message(rows: Iterable[Iterable[str]]) -> CSVMessageBuilder:
    message_builder = CSVMessageBuilder.with_credentials(["id"], ["type"])
    for row in rows:
        message_builder.add_row(row)
    return message_builder

def collecting_callback(target_queue: "queue.Queue[Any]") -> Callable[[Any], bool]:
    def collect_message(message: Any) -> bool:
        print(f"COLLECT RECEIVED MSG {message}")
        target_queue.put(message)
        return False
    return collect_message

def safe_close(middleware):
    try:
        middleware.close()
    except Exception:
        pass

def stop_consumer_and_join(middleware, thread: threading.Thread, timeout: float):
    try:
        middleware.stop_consuming()
    except Exception:
        pass
    thread.join(timeout=timeout)

def safe_async_stop_consumer_and_join(middleware, thread: threading.Thread, timeout: float):
    try:
        #Not the best?!
        middleware._rabbit_manager.async_stop_consuming();

        #middleware.stop_consuming()
    except Exception:
        pass
    thread.join(timeout=timeout)

