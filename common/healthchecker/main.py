import logging
import os
from configparser import ConfigParser

from common.healthchecker.src.healthchecker import Healthchecker


def parse_supervisors(supervisors_str: str):
    """
    Parse supervisors string format: "id:host:port,id:host:port,..."
    Returns list of dicts: [{"id": 1, "host": "sup1", "port": 5000}, ...]
    """
    supervisors = []
    for entry in supervisors_str.split(","):
        parts = entry.strip().split(":")
        if len(parts) != 3:
            raise ValueError(f"Invalid supervisor format: {entry}")
        supervisors.append({
            "id": int(parts[0]),
            "host": parts[1],
            "port": int(parts[2])
        })
    return supervisors


def initialize_config():
    config = ConfigParser(os.environ)
    config.read("config_health.ini")

    config_params = {}
    try:
        config_params["node_id"] = os.getenv(
            "NODE_ID", config["DEFAULT"]["NODE_ID"]
        )
        supervisors_str = os.getenv(
            "SUPERVISORS", config["DEFAULT"]["SUPERVISORS"]
        )
        config_params["supervisors"] = parse_supervisors(supervisors_str)
        
        config_params["node_health_port"] = int(
            os.getenv("NODE_HEALTH_PORT", config["DEFAULT"]["NODE_HEALTH_PORT"])
        )
        config_params["heartbeat_interval"] = float(
            os.getenv("HEARTBEAT_INTERVAL", config["DEFAULT"]["HEARTBEAT_INTERVAL"])
        )
        config_params["heartbeat_timeout"] = float(
            os.getenv("HEARTBEAT_TIMEOUT", config["DEFAULT"]["HEARTBEAT_TIMEOUT"])
        )
        config_params["discovery_timeout"] = float(
            os.getenv("DISCOVERY_TIMEOUT", config["DEFAULT"]["DISCOVERY_TIMEOUT"])
        )
        config_params["discovery_retries"] = int(
            os.getenv("DISCOVERY_RETRIES", config["DEFAULT"]["DISCOVERY_RETRIES"])
        )
        config_params["logging_level"] = os.getenv(
            "LOGGING_LEVEL", config["DEFAULT"]["LOGGING_LEVEL"]
        )
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting healthchecker".format(e))
    except ValueError as e:
        raise ValueError(
            "Key could not be parsed. Error: {}. Aborting healthchecker".format(e)
        )

    return config_params


def initialize_log(logging_level):
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging_level,
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main():
    config_params = initialize_config()
    node_id = config_params["node_id"]
    supervisors = config_params["supervisors"]
    node_health_port = config_params["node_health_port"]
    heartbeat_interval = config_params["heartbeat_interval"]
    heartbeat_timeout = config_params["heartbeat_timeout"]
    discovery_timeout = config_params["discovery_timeout"]
    discovery_retries = config_params["discovery_retries"]
    logging_level = config_params["logging_level"]

    initialize_log(logging_level)

    logging.debug(
        f"action: config | result: success | node_id: {node_id} | "
        f"supervisors: {supervisors} | node_health_port: {node_health_port} | "
        f"heartbeat_interval: {heartbeat_interval} | logging_level: {logging_level}"
    )

    healthchecker = Healthchecker(
        node_id=node_id,
        supervisors=supervisors,
        node_health_port=node_health_port,
        heartbeat_interval=heartbeat_interval,
        heartbeat_timeout=heartbeat_timeout,
        discovery_timeout=discovery_timeout,
        discovery_retries=discovery_retries,
    )
    
    try:
        healthchecker.run()
    except Exception as e:
        logging.error(f"Healthchecker error: {e}", exc_info=True)
    finally:
        logging.shutdown()

import threading
def start_on_thread():
    config_params = initialize_config()
    node_id = config_params["node_id"]
    supervisors = config_params["supervisors"]
    node_health_port = config_params["node_health_port"]
    heartbeat_interval = config_params["heartbeat_interval"]
    heartbeat_timeout = config_params["heartbeat_timeout"]
    discovery_timeout = config_params["discovery_timeout"]
    discovery_retries = config_params["discovery_retries"]
    logging_level = config_params["logging_level"]

    initialize_log(logging_level)

    logging.debug(
        f"action: config | result: success | node_id: {node_id} | "
        f"supervisors: {supervisors} | node_health_port: {node_health_port} | "
        f"heartbeat_interval: {heartbeat_interval} | logging_level: {logging_level}"
    )

    healthchecker = Healthchecker(
        node_id=node_id,
        supervisors=supervisors,
        node_health_port=node_health_port,
        heartbeat_interval=heartbeat_interval,
        heartbeat_timeout=heartbeat_timeout,
        discovery_timeout=discovery_timeout,
        discovery_retries=discovery_retries,
    )
    
    def run_checker():
        try:
            healthchecker.run()
        except Exception as e:
            logging.error(f"Healthchecker error: {e}", exc_info=True)
        finally:
            logging.shutdown()


    thread_checker = threading.Thread(target=run_checker, daemon= True)
    thread_checker.start()

    return thread_checker, healthchecker


if __name__ == "__main__":
    main()
