import errno
import logging
import socket
import threading
from types import FrameType
from typing import List, Optional, Tuple

from common import DispatcherProtocol


class DispatcherServer:
    @staticmethod
    def __try_close(a_socket: socket.socket, socket_name_to_log: str) -> None:
        """
        Attempt to gracefully close a given socket and log the result

        If the socket was already closed, for example by a signal, it will be silently ignored
        Any other errors will be logged
        """
        try:
            a_socket.close()
            logging.info(f"action: close_{socket_name_to_log} | result: success")
        except OSError as e:
            if e.errno == errno.EBADF:
                pass  # The socket was already closed by the graceful shutdown
            else:
                logging.error(
                    f"action: close_{socket_name_to_log} | result: fail | error: {e}"
                )

    def __init__(self, port: int, listen_backlog: int) -> None:
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(("", port))
        self._server_socket.listen(listen_backlog)
        self._was_stopped = False
        self._clients: List[Tuple[threading.Thread, socket.socket]] = []

    def run(self) -> None:
        """
        Multithreaded Server loop

        Server that accepts new connections and spawns a new thread
        for each client. Each client is handled independently.
        """
        while not self._was_stopped:
            self.__cleanup_client_threads()
            client_socket = self.__accept_new_connection()
            if client_socket:
                client_thread = threading.Thread(
                    target=self.__handle_client_connection, args=(client_socket,)
                )
                self._clients.append((client_thread, client_socket))
                client_thread.start()

    def graceful_shutdown(
        self, _signal_number: int, _current_stack_frame: Optional[FrameType]
    ) -> None:
        """
        On a signal, gracefully shutdown the server

        Stops the main loop and closes the server socket finally close and join the clients
        """
        self._was_stopped = True
        self.__try_close(self._server_socket, "server_socket")
        for client_thread, client_socket in self._clients:
            self.__try_close(client_socket, "client_socket")
            client_thread.join()
        logging.info("action: exit | result: success")

    def __handle_client_connection(self, client_socket: socket.socket) -> None:
        """
        Handle a client connection in a dedicated thread

        Read message, process it and close the socket
        """
        protocol = DispatcherProtocol(client_socket)
        try:
            protocol.handle_requests()
        except Exception as e:
            logging.error(f"action: error | result: fail | error: {e}")
        finally:

            def closure_to_close(sock: socket.socket) -> None:
                return self.__try_close(sock, "client_socket")

            protocol.close_with(closure_to_close)

    def __accept_new_connection(self) -> Optional[socket.socket]:
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """
        logging.info("action: accept_connections | result: in_progress")
        try:
            c, addr = self._server_socket.accept()
            logging.info(
                f"action: accept_connections | result: success | ip: {addr[0]}"
            )
            return c
        except OSError as e:
            if e.errno == errno.EBADF:
                return None  # The server socket was closed by the graceful shutdown
            else:
                logging.info(f"action: accept_connections | result: fail | error: {e}")
        return None

    def __cleanup_client_threads(self) -> None:
        """
        Join the finished threads, if it finished, the socket was already closed
        To free resources we just need to join the threads and clean the references
        """
        alive_clients = []
        for thread, sock in self._clients:
            if thread.is_alive():
                alive_clients.append((thread, sock))
            else:
                thread.join()
        self._clients = alive_clients
