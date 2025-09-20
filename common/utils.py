import logging
import uuid

def test_shared(name: str) -> None:
    logging.debug(f"Hello World! This is the {name}")

def new_uuid() -> str:
    return str(uuid.uuid4())