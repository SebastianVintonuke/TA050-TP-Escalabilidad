import struct
import time
from typing import Optional

# HeartbeatProtocol
MSG_TYPE_NODE_ALIVE_RESPONSE = 1
MSG_TYPE_NODE_ALIVE_CHECK = 2
MSG_TYPE_HEARTBEAT_ACK = 6

# LeaderElectionProtocol
MSG_TYPE_WHO_IS_LEADER = 3
MSG_TYPE_LEADER_INFO = 4
MSG_TYPE_NO_LEADER_YET = 5


class HeartbeatProtocol:
    @staticmethod
    def encode_node_alive_response(node_id: str, timestamp: Optional[float] = None) -> bytes:
        if timestamp is None:
            timestamp = time.time()
        
        node_id_bytes = node_id.encode('utf-8')
        node_id_len = len(node_id_bytes)
        
        if node_id_len > 255:
            raise ValueError("node_id too long (max 255 bytes)")
        
        msg = bytearray()
        msg.append(MSG_TYPE_NODE_ALIVE_RESPONSE)
        msg.append(node_id_len)
        msg.extend(node_id_bytes)
        msg.extend(struct.pack('>d', timestamp))
        
        return bytes(msg)

    @staticmethod
    def encode_node_alive_check(node_id: str) -> bytes:
        node_id_bytes = node_id.encode('utf-8')
        node_id_len = len(node_id_bytes)
        
        if node_id_len > 255:
            raise ValueError("node_id too long (max 255 bytes)")
        
        msg = bytearray()
        msg.append(MSG_TYPE_NODE_ALIVE_CHECK)
        msg.append(node_id_len)
        msg.extend(node_id_bytes)
        
        return bytes(msg)

    @staticmethod
    def decode_message(data: bytes) -> tuple:
        if len(data) < 3:
            return (None, None, None)
        
        msg_type = data[0]
        node_id_len = data[1]
        
        if len(data) < 2 + node_id_len:
            return (None, None, None)
        
        node_id = data[2:2+node_id_len].decode('utf-8', errors='ignore')
        
        if msg_type == MSG_TYPE_NODE_ALIVE_RESPONSE:
            if len(data) < 2 + node_id_len + 8:
                return (None, None, None)
            
            timestamp_bytes = data[2+node_id_len:2+node_id_len+8]
            timestamp = struct.unpack('>d', timestamp_bytes)[0]
            
            return (MSG_TYPE_NODE_ALIVE_RESPONSE, node_id, timestamp)
        
        elif msg_type == MSG_TYPE_NODE_ALIVE_CHECK:
            return (MSG_TYPE_NODE_ALIVE_CHECK, node_id, None)
        
        else:
            return (None, None, None)

    @staticmethod
    def decode_heartbeat_ack(data: bytes) -> Optional[str]:
        if len(data) < 3:
            return None
        
        idx = 1
        node_id_len = data[idx]
        idx += 1
        
        if len(data) < idx + node_id_len:
            return None
        
        node_id = data[idx:idx+node_id_len].decode('utf-8', errors='ignore')
        return node_id

    @staticmethod
    def encode_heartbeat_ack(node_id: str) -> bytes:
        node_id_bytes = node_id.encode('utf-8')
        
        if len(node_id_bytes) > 255:
            raise ValueError("node_id too long (max 255 bytes)")
        
        msg = bytearray()
        msg.append(MSG_TYPE_HEARTBEAT_ACK)
        msg.append(len(node_id_bytes))
        msg.extend(node_id_bytes)
        
        return bytes(msg)


class LeaderElectionProtocol:
    @staticmethod
    def encode_who_is_leader(node_id: str, request_id: str = "") -> bytes:
        node_id_bytes = node_id.encode('utf-8')
        request_id_bytes = request_id.encode('utf-8')
        
        if len(node_id_bytes) > 255 or len(request_id_bytes) > 255:
            raise ValueError("node_id or request_id too long (max 255 bytes each)")
        
        msg = bytearray()
        msg.append(MSG_TYPE_WHO_IS_LEADER)
        msg.append(len(node_id_bytes))
        msg.extend(node_id_bytes)
        msg.append(len(request_id_bytes))
        msg.extend(request_id_bytes)
        
        return bytes(msg)

    @staticmethod
    def encode_leader_info(request_id: str = "", leader_id: int = 0, leader_host: str = "", leader_port: int = 0) -> bytes:
        request_id_bytes = request_id.encode('utf-8')
        leader_host_bytes = leader_host.encode('utf-8')
        
        if len(request_id_bytes) > 255 or len(leader_host_bytes) > 255:
            raise ValueError("request_id or leader_host too long (max 255 bytes each)")
        
        msg = bytearray()
        msg.append(MSG_TYPE_LEADER_INFO)
        msg.append(len(request_id_bytes))
        msg.extend(request_id_bytes)
        msg.extend(struct.pack('>I', leader_id))
        msg.append(len(leader_host_bytes))
        msg.extend(leader_host_bytes)
        msg.extend(struct.pack('>H', leader_port))
        
        return bytes(msg)

    @staticmethod
    def encode_no_leader_yet(request_id: str = "") -> bytes:
        request_id_bytes = request_id.encode('utf-8')
        
        if len(request_id_bytes) > 255:
            raise ValueError("request_id too long (max 255 bytes)")
        
        msg = bytearray()
        msg.append(MSG_TYPE_NO_LEADER_YET)
        msg.append(len(request_id_bytes))
        msg.extend(request_id_bytes)
        
        return bytes(msg)

    @staticmethod
    def decode_who_is_leader(data: bytes) -> tuple:
        if len(data) < 4:
            return (None, None)
        
        idx = 1
        node_id_len = data[idx]
        idx += 1
        
        if len(data) < idx + node_id_len + 1:
            return (None, None)
        
        node_id = data[idx:idx+node_id_len].decode('utf-8', errors='ignore')
        idx += node_id_len
        
        request_id_len = data[idx]
        idx += 1
        
        if len(data) < idx + request_id_len:
            return (None, None)
        
        request_id = data[idx:idx+request_id_len].decode('utf-8', errors='ignore')
        
        return (node_id, request_id)

    @staticmethod
    def decode_leader_info(data: bytes) -> tuple:
        if len(data) < 9:
            return (None, None, None)
        
        idx = 1
        request_id_len = data[idx]
        idx += 1
        
        if len(data) < idx + request_id_len + 4:
            return (None, None, None)
        
        
        idx += request_id_len
        
        leader_id = struct.unpack('>I', data[idx:idx+4])[0]
        idx += 4
        
        if len(data) < idx + 1:
            return (None, None, None)
        
        host_len = data[idx]
        idx += 1
        
        if len(data) < idx + host_len + 2:
            return (None, None, None)
        
        leader_host = data[idx:idx+host_len].decode('utf-8', errors='ignore')
        idx += host_len
        
        leader_port = struct.unpack('>H', data[idx:idx+2])[0]
        
        return (leader_id, leader_host, leader_port)

    @staticmethod
    def decode_no_leader_yet(data: bytes) -> Optional[str]:
        if len(data) < 3:
            return None
        
        idx = 1
        request_id_len = data[idx]
        idx += 1
        
        if len(data) < idx + request_id_len:
            return None
        
        request_id = data[idx:idx+request_id_len].decode('utf-8', errors='ignore')
        return request_id
