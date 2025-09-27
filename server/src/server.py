import errno
import logging
import os
import socket
import threading
import uuid
from configparser import ConfigParser
from types import FrameType
from typing import Dict, List, Optional, Tuple

from common.protocol.clientserver import ClientServerOperation, ServerProtocol

CONFIG_PATH = "config.ini"

DISPATCHER_IP = "dispatcher"
DISPATCHER_PORT = "12347"


class Server:
    def __init__(self, config_path: str = CONFIG_PATH):
        self.config = self.initialize_config(config_path)
        self.initialize_log()        
        # Log config parameters at the beginning of the program to verify the configuration of the component
        logging.debug(f"action: config | result: success | port: {self.config["port"]} | listen_backlog: {self.config["listen_backlog"]} | logging_level: {self.config["logging_level"]}")

        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(("", self.config["port"]))
        self._server_socket.listen(self.config["listen_backlog"])
        self._was_stopped = False
        self._clients: List[Tuple[threading.Thread, socket.socket]] = []
        
        self.dispatchers = self.load_dispatchers()
        self.lock = threading.Lock()


    def initialize_config(self, config_path: str) -> dict:
        """Parse env variables or config file to find program config params

        Function that search and parse program configuration parameters in the
        program environment variables first and the in a config file.
        If at least one of the config parameters is not found a KeyError exception
        is thrown. If a parameter could not be parsed, a ValueError is thrown.
        If parsing succeeded, the function returns a ConfigParser object
        with config parameters
        """
        config = ConfigParser(os.environ)
        # If config.ini does not exists original config object is not modified
        config.read(config_path)

        config_params = {}
        try:
            config_params["port"] = int(os.getenv("PORT", config["DEFAULT"]["PORT"]))
            config_params["listen_backlog"] = int(
                os.getenv("LISTEN_BACKLOG", config["DEFAULT"]["LISTEN_BACKLOG"])
            )
            config_params["logging_level"] = os.getenv(
                "LOGGING_LEVEL", config["DEFAULT"]["LOGGING_LEVEL"]
            )
        except KeyError as e:
            raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
        except ValueError as e:
            raise ValueError(
                "Key could not be parsed. Error: {}. Aborting server".format(e)
            )

        return config_params


    def initialize_log(self) -> None:
        """
        Python custom logging initialization

        Current timestamp is added to be able to identify in docker
        compose logs the date when the log has arrived
        """
        logging_level = self.config["logging_level"]
        logging.basicConfig(
            format="%(asctime)s %(levelname)-8s %(message)s",
            level=logging_level,
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    
    
    def load_dispatchers(self) -> List[Dict[str, str]]:
        # ¿por ahora con uno basta?
        return [{"ip": DISPATCHER_IP, "port": DISPATCHER_PORT}]
        
    
    def select_dispatcher(self) -> Dict[str, str]:
        return self.dispatchers[0]
            
    
    def generate_request_id(self) -> str:
        return str(uuid.uuid4())
        
    
    def handle_upload_request(self, client_socket: socket.socket) -> None:
        server_protocol = ServerProtocol(client_socket)
        
        request_id = self.generate_request_id()
        dispatcher = self.select_dispatcher()
        
        server_protocol.send_dispatcher_info(
            dispatcher['ip'],
            dispatcher['port'],
            request_id
        )
        
    
    def handle_results_request(self, client_socket: socket.socket, request_id: str) -> None:
        #server_protocol = ServerProtocol(client_socket)

        # TODO: Obtener resultados --> enviarlos al cliente
        # results = result_pooler.get_results() 
        # server_protocol.send_results()
        pass   

    
    def handle_client(self, client_socket: socket.socket) -> None:
        try:
            server_protocol = ServerProtocol(client_socket)
            
            operation, request_id = server_protocol.wait_request()
            
            if operation == ClientServerOperation.Upload:
                self.handle_upload_request(client_socket)
            elif operation == ClientServerOperation.Results:
                self.handle_results_request(client_socket, request_id)
            else:
                server_protocol.send_error("Unknown op")
                
        except Exception as e:
            logging.exception(f"Error: {str(e)}")
            server_protocol = ServerProtocol(client_socket)
            server_protocol.send_error(str(e))
        finally:
                client_socket.shutdown(socket.SHUT_RDWR)
                client_socket.close()


    def accept_new_connection(self) -> Optional[socket.socket]:
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


    def cleanup_client_threads(self) -> None:
        alive_clients = []
        for thread, sock in self._clients:
            if thread.is_alive():
                alive_clients.append((thread, sock))
            else:
                thread.join()
        self._clients = alive_clients
        

    def graceful_shutdown(self, _signal_number: int, _current_stack_frame: Optional[FrameType]) -> None:
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


    def run(self) -> None:
        # TODO: ¿thread para polling de resultados
        while not self._was_stopped:
            self.cleanup_client_threads()
            client_socket = self.accept_new_connection()
            if client_socket:
                client_thread = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket,)
                )
                self._clients.append((client_thread, client_socket))
                client_thread.start()
