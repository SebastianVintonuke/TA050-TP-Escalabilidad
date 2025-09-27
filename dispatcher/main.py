#!/usr/bin/env python3

import signal

from src.dispatcher import Dispatcher


def main() -> None:
    dispatcher_server = Dispatcher()

    signal.signal(signal.SIGTERM, dispatcher_server.graceful_shutdown)

    dispatcher_server.run()


if __name__ == "__main__":
    main()
