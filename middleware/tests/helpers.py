import threading
import time
from typing import Any, Callable, Iterable

import queue

from middleware.src.routing.csv_message import CSVMessageBuilder


def wrap_callback_with_ack_handling(original_callback: Callable[[Any], bool]) -> Callable[[Any], bool]:
    """Envuelve un callback y asegura ACK del mensaje después de ejecutarlo."""
    def _wrapped(message: Any) -> bool:
        result: bool = original_callback(message)
        if hasattr(message, "delivery_tag"):
            message.ch.basic_ack(delivery_tag=message.delivery_tag)
        elif hasattr(message, "method") and hasattr(message.method, "delivery_tag"):
            message.ch.basic_ack(delivery_tag=message.method.delivery_tag)
        else:
            message.ch.basic_ack(delivery_tag=1)
        return result
    return _wrapped


def start_consumer_in_thread(mw: Any, callback: Callable[[Any], bool]) -> None:
    thread = threading.Thread(target=mw.start_consuming, args=(callback,), daemon=True)
    thread.start()
    time.sleep(0.3)
    return thread

def build_message(rows: Iterable[Iterable[str]]) -> CSVMessageBuilder:
    message_builder = CSVMessageBuilder(["id"], ["type"])
    for row in rows:
        message_builder.add_row(row)
    return message_builder

def collecting_callback(target_queue: "queue.Queue[Any]") -> Callable[[Any], bool]:
    def collect_message(message: Any) -> bool:
        target_queue.put(message)
        return True
    return collect_message