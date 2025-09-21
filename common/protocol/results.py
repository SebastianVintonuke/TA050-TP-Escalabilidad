import socket
from typing import Callable, List, Tuple, Sequence
from enum import Enum

from .byte import ByteProtocol
from .signal import SignalProtocol
from .batch import BatchProtocol
from ..models.queryresult import QueryResult, QueryResult1, QueryResult2BestSelling, QueryResult2MostProfit, QueryResult3, QueryResult4

class QueryId(str, Enum):
    Query1 = "1"
    Query2BestSelling = "2BS"
    Query2MostProfit = "2MP"
    Query3 = "3"
    Query4 = "4"

def query_id_from(string: str) -> QueryId:
    if string == QueryId.Query1:
        return QueryId.Query1
    elif string == QueryId.Query2BestSelling:
        return QueryId.Query2BestSelling
    elif string == QueryId.Query2MostProfit:
        return QueryId.Query2MostProfit
    elif string == QueryId.Query3:
        return QueryId.Query3
    elif string == QueryId.Query4:
        return QueryId.Query4
    else:
        raise ValueError(f"Invalid query id {string}")

class ResultOperation(int, Enum):
    NotifyResults = 0x01
    AppendResults = 0x02
    NotifyEOF = 0x03
    PollResults = 0x04

class ResultsProtocol:
    def __init__(self, a_socket: socket.socket):
        self._byte_protocol = ByteProtocol(a_socket)
        self._signal_protocol = SignalProtocol(a_socket)
        self._batch_protocol = BatchProtocol(a_socket)

    def close_with(self, closure_to_close: Callable[[socket.socket], None]) -> None:
        """
        Check ByteProtocol.close_with method
        """
        self._byte_protocol.close_with(closure_to_close)

    def handle_requests(self, append_closure: Callable[[str, QueryId, Sequence[QueryResult]], None], eof_closure: Callable[[str, QueryId], None]) -> None:
        operation_code = self._byte_protocol.wait_uint8()
        if operation_code == ResultOperation.NotifyResults:

            user_id, query_id = self.__operation_notify_results()
            while True:
                operation_code = self._byte_protocol.wait_uint8()
                if operation_code == ResultOperation.AppendResults:
                    self.__operation_append_results(append_closure, user_id, query_id)
                elif operation_code == ResultOperation.NotifyEOF:
                    self.__operation_notify_eof(eof_closure, user_id, query_id)
                    break
                elif operation_code == ResultOperation.PollResults:
                    raise ValueError(f"Invalid operation, a ResultNode shouldn't poll for results")

        elif operation_code == ResultOperation.PollResults:
            self.__operation_poll_results()
        else:
            raise ValueError(f"Unknown operation code {operation_code}")

    def notify_results_for(self, user_id: str, query_id: QueryId) -> None:
        """
        To be used by a result node
        Specifies the user and the query
        If is not an associated user, creates a new one
        Args:
            user_id (str): Unique identifier for the user
            query_id (str): Unique identifier for the query (1, 2A, 2B, 3, 4)
        """
        self._byte_protocol.send_uint8(ResultOperation.NotifyResults)
        self._byte_protocol.send_bytes(user_id.encode("utf-8"))
        self._byte_protocol.send_bytes(query_id.encode("utf-8")) # TODO validar parametro y enviar error no ack
        self._signal_protocol.wait_signal()

    def append_results(self, partial_results: Sequence[QueryResult]) -> None:
        """
        To be used by a result node
        Appends the partial results
        Args:
            partial_results (Sequence[QueryResult]): List of partial results
        """
        self._byte_protocol.send_uint8(ResultOperation.AppendResults)
        for result in partial_results:
            self._batch_protocol.send_batch([result.to_bytes()]) # TODO no mandar de batchs de a 1, calcular el numero optimo de results por batch
        self._batch_protocol.send_batch([]) # Batch vacío para notificar el fin
        self._signal_protocol.wait_signal()

    def notify_eof_results(self) -> None:
        """
        To be used by a result node
        Notify the result storage that the results for that query are ready, close the socket
        """
        self._byte_protocol.send_uint8(ResultOperation.NotifyEOF)
        self._signal_protocol.wait_signal()

    def poll_result_for(self, user_id: str) -> None:
        """
        To be used by the server
        Poll all the query results for the specified user
        """
        self._byte_protocol.send_uint8(ResultOperation.PollResults)
        self._signal_protocol.wait_signal()

    def __operation_notify_results(self) -> Tuple[str, QueryId]:
        user_id = self._byte_protocol.wait_bytes().decode("utf-8")
        query_id = self._byte_protocol.wait_bytes().decode("utf-8")
        # TODO chekear si es valido si no, enviar error en lugar de ack
        self._signal_protocol.send_ack()
        return user_id, query_id_from(query_id)

    def __operation_append_results(self, append_closure: Callable[[str, QueryId, Sequence[QueryResult]], None], user_id: str, query_id: QueryId) -> None:
        while True:
            results = self._batch_protocol.wait_batch()

            if len(results) == 0:
                self._signal_protocol.send_ack()
                break # Batch vacío para notificar el fin

            if query_id == QueryId.Query1:
                append_closure(user_id, query_id, [QueryResult1.from_bytes(r) for r in results])
            elif query_id == QueryId.Query2BestSelling:
                append_closure(user_id, query_id, [QueryResult2BestSelling.from_bytes(r) for r in results])
            elif query_id == QueryId.Query2MostProfit:
                append_closure(user_id, query_id, [QueryResult2MostProfit.from_bytes(r) for r in results])
            elif query_id == QueryId.Query3:
                append_closure(user_id, query_id, [QueryResult3.from_bytes(r) for r in results])
            elif query_id == QueryId.Query4:
                append_closure(user_id, query_id, [QueryResult4.from_bytes(r) for r in results])
            else:
                self._signal_protocol.send_error(f"Invalid query id {query_id}")

    def __operation_notify_eof(self, eof_closure: Callable[[str, QueryId], None], user_id: str, query_id: QueryId) -> None:
        eof_closure(user_id, query_id)
        self._signal_protocol.send_ack()

    def __operation_poll_results(self) -> None:
        return
