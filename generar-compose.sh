#!/bin/bash

output_file_name=$1
result_node_number=$2
select_node_number=$3

if [ $# -ne 3 ] || ! [[ "$result_node_number" =~ ^[0-9]+$ ]] || ! [[ "$select_node_number" =~ ^[0-9]+$ ]]; then
    echo "$0 <Nombre del archivo de salida> <Cantidad de result nodes> <Cantidad de select nodes>"
    exit 1
fi

echo "Nombre del archivo de salida: $1"
echo "Cantidad de result nodes: $2"
echo "Cantidad de select nodes: $3"

echo "name: TP Escalabilidad - Coffee Shop Analysis" > "$output_file_name"
echo "services:" >> "$output_file_name"

echo "  middleware:
    container_name: middleware
    image: middleware:latest
    entrypoint: python3 /middleware/main.py
    environment:
      - PYTHONUNBUFFERED=1
    networks:
      - testing_net
    volumes:
      - ./middleware/config.ini:/config.ini:ro
" >> "$output_file_name"

echo "  results:
    container_name: results
    image: results:latest
    entrypoint: python3 /results/main.py
    environment:
      - PYTHONUNBUFFERED=1
      - LISTEN_BACKLOG=$result_node_number
    networks:
      - testing_net
    depends_on:
      - middleware
    volumes:
      - ./results/config.ini:/config.ini:ro
" >> "$output_file_name"

echo "  server:
    container_name: server
    image: server:latest
    entrypoint: python3 /server/main.py
    environment:
      - PYTHONUNBUFFERED=1
    networks:
      - testing_net
    volumes:
      - ./server/config.ini:/config.ini:ro
" >> "$output_file_name"

echo "  client:
    container_name: client
    image: client:latest
    entrypoint: python3 /client/main.py
    environment:
      - PYTHONUNBUFFERED=1
      - DATA_DIR=/archive
    networks:
      - testing_net
    depends_on:
      - server
    volumes:
      - ./client/config.ini:/config.ini:ro
      - ./.data/archive:/archive:ro
" >> "$output_file_name"

echo "  dispatcher:
    container_name: dispatcher
    image: dispatcher:latest
    entrypoint: python3 /dispatcher/main.py
    environment:
      - PYTHONUNBUFFERED=1
    networks:
      - testing_net
    depends_on:
      - middleware
    volumes:
      - ./dispatcher/config.ini:/config.ini:ro
" >> "$output_file_name"

for ((i=1; i<=result_node_number; i++)); do
  echo "  resultnode$i:
    container_name: resultnode$i
    image: resultnode:latest
    entrypoint: python3 /resultnode/main.py
    environment:
      - RESULT_NODE_ID=$i
    networks:
      - testing_net
    depends_on:
      - middleware
    volumes:
      - ./resultnode/config.ini:/config.ini:ro
" >> "$output_file_name"
done

for ((i=1; i<=select_node_number; i++)); do
  echo "  selectnode$i:
    container_name: selectnode$i
    image: selectnode:latest
    entrypoint: python3 /selectnode/main.py
    environment:
      - SELECT_NODE_ID=$i
    networks:
      - testing_net
    depends_on:
      - middleware
    volumes:
      - ./selectnode/config.ini:/config.ini:ro
" >> "$output_file_name"
done

echo "networks:
  testing_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24" >> "$output_file_name"