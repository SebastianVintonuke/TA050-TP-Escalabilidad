import socket

from pathlib import Path
from enum import Enum
from typing import Callable, List, Tuple

from .byte import ByteProtocol
from .signal import SignalProtocol
from .batch import BatchProtocol

from ..models.model import Model

DISPATCHER_IP = 0
DISPATCHER_PORT = 1
REQUEST_ID = 2

MAX_NUMBER_OF_ITEMS = 255

MAX_BATCH_SIZE = 8 * 1024  # 8KB

class ClientDispatcherOperation(int, Enum):
    DefineQueries = 0x01    # Especifica las queries a ejecutar
    FileHeader = 0x02       # Define un nuevo archivo
    FileBatch = 0x03        # Envía un batch de datos de archivo
    EndTransmission = 0x04  # Señala fin de transmisión
    Error = 0xFF            # Error en cualquier operación



class ClientServerOperation(int, Enum):
    Upload = 0x01           # Cliente solicita subir archivos
    DispatcherInfo = 0x02   # Servidor envia info del dispatcher [ip, puerto, request_id]
    Results = 0x03          # Cliente solicita resultados
    ResultsBatch = 0x04     # Servidor envía un batch de resultados
    ResultsEOF = 0x05       # Servidor indica fin de resultados
    Notification = 0x06     # Dispatcher notifica resultados al servidor
    Error = 0xFF            # Error en cualquier operación


class ProtocolBase:

    def __init__(self, a_socket: socket.socket):
        self._byte_protocol = ByteProtocol(a_socket)
        self._signal_protocol = SignalProtocol(a_socket)
        self._batch_protocol = BatchProtocol(a_socket)
        self._socket = a_socket
        
    def close_with(self, closure_to_close: Callable[[socket.socket], None]) -> None:
        self._byte_protocol.close_with(closure_to_close)
        
    def send_error(self, error_msg: str) -> None:
        self._byte_protocol.send_uint8(ClientServerOperation.Error.value)
        self._byte_protocol.send_bytes(error_msg.encode('utf-8'))


class ClientProtocol(ProtocolBase):
    
    def request_upload(self) -> Tuple[str, int, str]:
        self._byte_protocol.send_uint8(ClientServerOperation.Upload.value)
        self._signal_protocol.wait_signal()
        
        op_code = self._byte_protocol.wait_uint8()
        if op_code != ClientServerOperation.DispatcherInfo.value:
            error_msg = "Dispatcher info is missing"
            if op_code == ClientServerOperation.Error.value:
                error_msg = self._byte_protocol.wait_bytes().decode('utf-8')
            raise RuntimeError(f"Error: {error_msg}")
        
        dispatcher_info = self._batch_protocol.wait_batch()
        self._signal_protocol.send_ack()
        
        if len(dispatcher_info) != 3:
            raise RuntimeError(f"Invalid Dispatcher info: {dispatcher_info}")
            
        return (
            dispatcher_info[DISPATCHER_IP].decode(),
            int(dispatcher_info[DISPATCHER_PORT].decode()),
            dispatcher_info[REQUEST_ID].decode()
        )
    
    def request_results(self, request_id: str) -> List[bytes]:
        self._byte_protocol.send_uint8(ClientServerOperation.Results.value)
        self._byte_protocol.send_bytes(request_id.encode('utf-8'))
        self._signal_protocol.wait_signal()
        
        # recv until EOF
        results = []
        while True:
            op_code = self._byte_protocol.wait_uint8()
            
            if op_code == ClientServerOperation.ResultsEOF.value:
                # end
                break
                
            elif op_code == ClientServerOperation.ResultsBatch.value:
            
                batch = self._batch_protocol.wait_batch()
                results.extend(batch)
                self._signal_protocol.send_ack()
                
            elif op_code == ClientServerOperation.Error.value:
                # Error
                error_msg = self._byte_protocol.wait_bytes().decode('utf-8')
                raise RuntimeError(f"Error requesting results: {error_msg}")
            
            else:
                raise RuntimeError(f"Unknown code: {op_code}")
        
        return results
    
    # communication with dispatcher

    def define_queries(self, queries: List[str]) -> None:
        self._batch_protocol.send_batch([
            ClientDispatcherOperation.DefineQueries.value.to_bytes(),
            ",".join(queries).encode('utf-8')
        ])
        self._signal_protocol.wait_signal()
        
    def send_file_header(self, filename: str, header: bytes) -> None:
        self._batch_protocol.send_batch([
            ClientDispatcherOperation.FileHeader.value.to_bytes(),
            filename.encode('utf-8'),
            header
        ])
        self._signal_protocol.wait_signal()
        
    def send_file_batch(self, batch: List[bytes]) -> None:
        complete_batch = [ClientDispatcherOperation.FileBatch.value.to_bytes()]
        complete_batch.extend(batch)

        self._batch_protocol.send_batch(complete_batch)
        self._signal_protocol.wait_signal()
        
    def end_transmission(self) -> None:
        self._batch_protocol.send_batch([])
        
    def process_and_send_file(self, path: Path) -> bool:
        if not path.exists():
            return False

        with open(path, "rb") as csv_file:
            header = csv_file.readline().rstrip(b"\r\n")
            if not header:
                return False

            model_cls = Model.model_for(header)
            
            self.send_file_header(path.name, header)
                
            current_batch = []
            total_size = 0
            
            for line in csv_file:
                model = model_cls.from_bytes(line)
                serialized = model.to_bytes()
                
                if (total_size + len(serialized) > MAX_BATCH_SIZE or len(current_batch) >= MAX_NUMBER_OF_ITEMS):
                    self.send_file_batch(current_batch)
                    current_batch = []
                    total_size = 0
                    
                current_batch.append(serialized)
                total_size += len(serialized)
            
            if current_batch:
                self.send_file_batch(current_batch)
                
            return True


class ServerProtocol(ProtocolBase):
    
    def wait_request(self) -> Tuple[ClientServerOperation, str]:
        op_code = self._byte_protocol.wait_uint8()
        
        if op_code == ClientServerOperation.Upload.value:
            self._signal_protocol.send_ack()
            return ClientServerOperation.Upload, None
            
        elif op_code == ClientServerOperation.Results.value:
            request_id = self._byte_protocol.wait_bytes().decode('utf-8')
            self._signal_protocol.send_ack()
            return ClientServerOperation.Results, request_id
            
        elif op_code == ClientServerOperation.Notification.value:
            request_id = self._byte_protocol.wait_bytes().decode('utf-8')
            return ClientServerOperation.Notification, request_id
            
        else:
            self._signal_protocol.send_error(f"Operación desconocida: {op_code}")
            return ClientServerOperation.Error, None
    
    def send_dispatcher_info(self, dispatcher_ip: str, dispatcher_port: str, request_id: str) -> None:
        self._byte_protocol.send_uint8(ClientServerOperation.DispatcherInfo.value)
        self._batch_protocol.send_batch([
            dispatcher_ip.encode('utf-8'),
            dispatcher_port.encode('utf-8'),
            request_id.encode('utf-8')
        ])
        self._signal_protocol.wait_signal()
    
    def send_results(self, results: List[bytes]) -> None:
        for i in range(0, len(results), MAX_NUMBER_OF_ITEMS):
            batch = results[i:i+MAX_NUMBER_OF_ITEMS]
            self._byte_protocol.send_uint8(ClientServerOperation.ResultsBatch.value)
            self._batch_protocol.send_batch(batch)
            self._signal_protocol.wait_signal()
        
        self._byte_protocol.send_uint8(ClientServerOperation.ResultsEOF.value)
    
    def wait_notification_data(self) -> List[bytes]:
        batch = self._batch_protocol.wait_batch()
        self._signal_protocol.send_ack()
        return batch


class DispatcherProtocol(ProtocolBase):

    # server --> completion of processing
    def notify_completion(self, request_id: str, results: List[bytes]) -> None:
        self._byte_protocol.send_uint8(ClientServerOperation.Notification.value)
        self._byte_protocol.send_bytes(request_id.encode('utf-8'))
        self._batch_protocol.send_batch(results)
        self._signal_protocol.wait_signal()

    # client --> wait for the csv's
    def wait_command(self) -> Tuple[int, List[bytes]]:
        batch = self._batch_protocol.wait_batch()
    
        if not batch:
            return (ClientDispatcherOperation.EndTransmission.value, [])
        
        try:
            op_code = int.from_bytes(batch[0], byteorder='big')
            data = batch[1:] if len(batch) > 1 else []
            
            if op_code == ClientDispatcherOperation.DefineQueries.value:
                self._signal_protocol.send_ack()
                return (op_code, data)
            elif op_code == ClientDispatcherOperation.FileHeader.value:
                self._signal_protocol.send_ack()
                return (op_code, data)
            elif op_code == ClientDispatcherOperation.FileBatch.value:
                self._signal_protocol.send_ack()
                return (op_code, data)
            else:
                self._signal_protocol.send_error(f"Unknown operation: {op_code}")
                return (ClientDispatcherOperation.Error.value, [])
        except Exception as e:
            self._signal_protocol.send_error(f"Error parsing command: {e}")
            return (ClientDispatcherOperation.Error.value, [])