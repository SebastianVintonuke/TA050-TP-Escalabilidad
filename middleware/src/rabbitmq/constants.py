from pika.exceptions import AMQPConnectionError, ConnectionClosed, ChannelClosed
from pika.exceptions import AMQPError, StreamLostError, ChannelClosedByBroker, ChannelWrongStateError, ConnectionWrongStateError

import socket

RABBITMQ_HOST = "middleware"

RoutingRestartError = (
	StreamLostError,
	ChannelClosedByBroker,
	ConnectionResetError,
	ConnectionWrongStateError,
	OSError,
	BrokenPipeError,
    socket.timeout,
	ChannelWrongStateError,
	AMQPError
)
RoutingConnectionErrors = (
    AMQPConnectionError,
    ChannelClosed,
    ConnectionClosed,
    StreamLostError,
)
