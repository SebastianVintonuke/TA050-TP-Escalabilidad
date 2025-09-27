#!/usr/bin/env python3

import signal

from src.server import Server


def main() -> None:
    server = Server()

    signal.signal(signal.SIGTERM, server.graceful_shutdown)

    server.run()


if __name__ == "__main__":
    main()
