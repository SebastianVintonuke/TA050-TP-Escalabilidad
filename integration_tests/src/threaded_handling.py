import threading
import queue

class AsyncConsumerListener:
    def __init__(self, listener, max_size = 0):
        self.listener = listener
        self._queue = queue.Queue(maxsize= max_size)
        self._consumer_thread = None
        self._stopped_event = threading.Event()
        #self._should_run = threading.Event()
        self._stopped_event.set()

    def start(self):
        if not self._stopped_event.is_set():
            raise Exception("Invalid state, trying to start when listener was not stopped properly ")
        self._consumer_thread = threading.Thread(target=self._run, daemon=True)
        self._stopped_event.clear()
        #self._should_run.set()

        self._consumer_thread.start()

    def _run(self):
        #total=0
        try:
            msg = self._queue.get() #timeout=0.5 instead of timeout pushing None for throwing graceful stop?
            while True:#self._should_run.is_set():
                #print(f"Handling message? {msg}")
                self.listener(**msg)
                msg = self._queue.get()
        except queue.ShutDown:
            print(f"Queue was closed")
        except Exception as e:
            print(f"Error on Async consume listener {e}")
        finally:
            self._stopped_event.set()

    def put(self, obj):
        #print(f"running {not self._stopped_event.is_set() } DO PUT? ", obj)
        self._queue.put(obj)
        
    def __call__(self, **kwargs):
        #print(f"running {not self._stopped_event.is_set() } DO PUT kwargs? ", kwargs)
        self._queue.put(kwargs)

    def wait_stop(self):
        self._stopped_event.wait() # Wait for stope event
        #self._should_run.clear()
        self._consumer_thread.join() # Then join thread just in case. Just joining should be enough tbh

    def stop_no_wait(self):
        self._queue.shutdown(immediate = False)
    def stop(self):
        self._queue.shutdown(immediate = False)
        self.wait_stop()


    def force_stop_no_Wait(self):
        self._queue.shutdown(immediate = True)

    def force_stop(self):
        self._queue.shutdown(immediate = True)
        self.wait_stop()

