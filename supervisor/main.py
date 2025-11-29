import logging
import os
import time
from configparser import ConfigParser
from typing import Dict, Tuple

from supervisor.src.supervisor import Supervisor


def initialize_config():
    """Parse env variables or config file to find program config params"""
    config = ConfigParser(os.environ)
    config.read("config.ini")

    config_params = {}
    try:
        config_params["nodes_config"] = os.getenv(
            "NODES_CONFIG", config["DEFAULT"]["NODES_CONFIG"]
        )
        config_params["supervisor_port"] = int(
            os.getenv("SUPERVISOR_PORT", config["DEFAULT"]["SUPERVISOR_PORT"])
        )
        config_params["heartbeat_tick"] = float(
            os.getenv("HEARTBEAT_TICK", config["DEFAULT"]["HEARTBEAT_TICK"])
        )
        config_params["soft_threshold"] = int(
            os.getenv("SOFT_THRESHOLD", config["DEFAULT"]["SOFT_THRESHOLD"])
        )
        config_params["hard_threshold"] = int(
            os.getenv("HARD_THRESHOLD", config["DEFAULT"]["HARD_THRESHOLD"])
        )
        config_params["logging_level"] = os.getenv(
            "LOGGING_LEVEL", config["DEFAULT"]["LOGGING_LEVEL"]
        )
        config_params["enable_restart"] = os.getenv(
            "ENABLE_RESTART", config.get("DEFAULT", "ENABLE_RESTART", fallback="true")
        ).lower() in ("true", "1", "yes")
        config_params["max_restart_attempts"] = int(
            os.getenv("MAX_RESTART_ATTEMPTS", config.get("DEFAULT", "MAX_RESTART_ATTEMPTS", fallback="5"))
        )
        config_params["restart_window"] = float(
            os.getenv("RESTART_WINDOW", config.get("DEFAULT", "RESTART_WINDOW", fallback="300"))
        )
        
        # Leader election config
        config_params["supervisor_id"] = os.getenv(
            "SUPERVISOR_ID", config.get("DEFAULT", "SUPERVISOR_ID", fallback=None)
        )
        if config_params["supervisor_id"]:
            config_params["supervisor_id"] = int(config_params["supervisor_id"])
        
        config_params["supervisor_peers"] = os.getenv(
            "SUPERVISOR_PEERS", config.get("DEFAULT", "SUPERVISOR_PEERS", fallback=None)
        )
        
        config_params["enable_leader_election"] = os.getenv(
            "ENABLE_LEADER_ELECTION", config.get("DEFAULT", "ENABLE_LEADER_ELECTION", fallback="false")
        ).lower() in ("true", "1", "yes")
        
        config_params["election_port"] = int(
            os.getenv("ELECTION_PORT", config.get("DEFAULT", "ELECTION_PORT", fallback="6000"))
        )
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting supervisor".format(e))
    except ValueError as e:
        raise ValueError(
            "Key could not be parsed. Error: {}. Aborting supervisor".format(e)
        )

    return config_params


def parse_nodes_config(env_var: str) -> Dict[str, Tuple[str, int]]:
    nodes_config = {}
    if not env_var:
        return nodes_config
    
    for node_spec in env_var.split(','):
        parts = node_spec.strip().split(':')
        if len(parts) == 3:
            node_id, host, port = parts
            nodes_config[node_id] = (host, int(port))
    
    return nodes_config


def parse_supervisor_peers(env_var: str) -> Dict[int, Tuple[str, int]]:
    """
    Format: "id1:host1:port1,id2:host2:port2,..."
    
    Returns: {supervisor_id: (host, election_port)}
    """
    peers = {}
    if not env_var:
        return peers
    
    for peer_spec in env_var.split(','):
        parts = peer_spec.strip().split(':')
        if len(parts) == 3:
            peer_id, host, port = parts
            peers[int(peer_id)] = (host, int(port))
    
    return peers


def main():
    config_params = initialize_config()
    nodes_config_str = config_params["nodes_config"]
    supervisor_port = config_params["supervisor_port"]
    heartbeat_tick = config_params["heartbeat_tick"]
    soft_threshold = config_params["soft_threshold"]
    hard_threshold = config_params["hard_threshold"]
    logging_level = config_params["logging_level"]
    enable_restart = config_params["enable_restart"]
    max_restart_attempts = config_params["max_restart_attempts"]
    restart_window = config_params["restart_window"]
    
    supervisor_id = config_params["supervisor_id"]
    supervisor_peers_str = config_params["supervisor_peers"]
    enable_leader_election = config_params["enable_leader_election"]
    election_port = config_params["election_port"]


    logging.debug(
        f"action: config | result: success | supervisor_port: {supervisor_port} | "
        f"heartbeat_tick: {heartbeat_tick} | soft_threshold: {soft_threshold} | "
        f"hard_threshold: {hard_threshold} | logging_level: {logging_level} | "
        f"enable_restart: {enable_restart} | max_restart_attempts: {max_restart_attempts} | "
        f"restart_window: {restart_window} | enable_leader_election: {enable_leader_election}"
    )

    nodes_config = parse_nodes_config(nodes_config_str)

    logging.info(f"Monitoring {len(nodes_config)} nodes: {list(nodes_config.keys())}")
    
    supervisor_peers = parse_supervisor_peers(supervisor_peers_str)
    
    logging.info(f"Leader election enabled - Supervisor ID: {supervisor_id}, Peers: {list(supervisor_peers.keys())}")

    supervisor = Supervisor(
        nodes_config=nodes_config,
        supervisor_id=supervisor_id,
        supervisor_peers=supervisor_peers,
        enable_leader_election=enable_leader_election,
        election_port=election_port,
        supervisor_port=supervisor_port,
        heartbeat_tick=heartbeat_tick,
        soft_threshold=soft_threshold,
        hard_threshold=hard_threshold,
        enable_restart=enable_restart,
        max_restart_attempts=max_restart_attempts,
        restart_window=restart_window,
    )
    
    supervisor.start()

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        supervisor.stop()


if __name__ == "__main__":
    main()
