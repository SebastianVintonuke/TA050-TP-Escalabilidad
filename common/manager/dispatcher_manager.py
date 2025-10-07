import threading
from typing import List

# thread-safe class to get the next dispatcher using round-robin styled method
class DispatcherManager:    
    def __init__(self, dispatchers: List[str]):
        self._dispatchers = sorted(dispatchers)
        self._current_index = 0
        self._lock = threading.Lock()
        
    def get_next_dispatcher(self) -> str:
        with self._lock:
            address = self._dispatchers[self._current_index]
            self._current_index = (self._current_index + 1) % len(self._dispatchers)
            return address
        