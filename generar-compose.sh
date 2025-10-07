#!/bin/bash

output_file_name=$1
result_node_number=$2
select_node_number=$3
client_number=$4
dispatcher_number=$5

if [ $# -ne 5 ] || ! [[ "$result_node_number" =~ ^[0-9]+$ ]] || ! [[ "$select_node_number" =~ ^[0-9]+$ ]] || ! [[ "$client_number" =~ ^[0-9]+$ ]] || ! [[ "$dispatcher_number" =~ ^[0-9]+$ ]]; then
    echo "$0 <Nombre del archivo de salida> <Cantidad de result nodes> <Cantidad de select nodes> <Cantidad de clientes> <Cantidad de dispatchers>"
    exit 1
fi

echo "Nombre del archivo de salida: $1"
echo "Cantidad de result nodes: $2"
echo "Cantidad de select nodes: $3"
echo "Cantidad de clientes: $4"
echo "Cantidad de dispatchers: $5"

echo "name: TP Escalabilidad - Coffee Shop Analysis" > "$output_file_name"
echo "services:" >> "$output_file_name"

echo "  middleware:
    container_name: middleware
    build:
      context: ./middleware
      dockerfile: Dockerfile
    ports:
      - '15672:15672'
    networks:
      - testing_net
    volumes:
      - ./middleware/rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf:ro
" >> "$output_file_name"

echo "  results:
    container_name: results
    image: results:latest
    entrypoint: python3 /results/main.py
    environment:
      - PYTHONUNBUFFERED=1
      - DIR_PATH=/storage
    networks:
      - testing_net
    depends_on:
      - middleware
    volumes:
      - ./results/config.ini:/config.ini:ro
      - ./.data/storage:/results/storage
" >> "$output_file_name"

dispatcher_list=""
for ((i=1; i<=dispatcher_number; i++)); do
  if [ "$i" -gt 1 ]; then
    dispatcher_list+=","
  fi
  dispatcher_list+="dispatcher$i:1234$((6+i))"
done

echo "  server:
    container_name: server
    image: server:latest
    entrypoint: python3 /server/main.py
    environment:
      - PYTHONUNBUFFERED=1
      - DISPATCHERS=$dispatcher_list
    networks:
      - testing_net
    depends_on:" >> "$output_file_name"

for ((i=1; i<=dispatcher_number; i++)); do
  echo "      - dispatcher$i" >> "$output_file_name"
done
echo "    volumes:
      - ./server/config.ini:/config.ini:ro
" >> "$output_file_name"

for ((i=1; i<=client_number; i++)); do
  executions=$((i+1))
  echo "  client$i:
    container_name: client$i
    image: client:latest
    entrypoint: python3 /client/main.py
    environment:
      - PYTHONUNBUFFERED=1
      - INPUT_DIR=/input
      - OUTPUT_DIR=/output
      - EXECUTIONS=$executions
    networks:
      - testing_net
    depends_on:
      - server
    volumes:
      - ./client/config.ini:/config.ini:ro
      - ./.data/archive:/input:ro
      - ./.data/results/client$i:/output:rw
" >> "$output_file_name"
done

for ((i=1; i<=dispatcher_number; i++)); do
  port=$((12347 + i - 1))
  echo "  dispatcher$i:
    container_name: dispatcher$i
    image: dispatcher:latest
    entrypoint: python3 /dispatcher/main.py
    environment:
      - PYTHONUNBUFFERED=1
      - PORT=$port
    networks:
      - testing_net
    depends_on:
      - middleware
    volumes:
      - ./dispatcher/config.ini:/config.ini:ro
" >> "$output_file_name"
done

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
      - results
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
      - GROUPBY_NODE_COUNT=1
      - JOIN_NODE_COUNT=1
    networks:
      - testing_net
    depends_on:
      - middleware
    links:
      - middleware
    volumes:
      - ./selectnode/config.ini:/config.ini:ro
" >> "$output_file_name"
done

echo "  groupbynode1:
    container_name: groupbynode1
    image: groupbynode:latest
    entrypoint: python3 /groupbynode/main.py
    environment:
      - NODE_IND=0
      - NODE_COUNT=1
      - JOIN_NODE_COUNT=1
    networks:
      - testing_net
    depends_on:
      - middleware
    links:
      - middleware
    volumes:
      - ./groupbynode/config.ini:/config.ini:ro
" >> "$output_file_name"

echo "  joinnode1:
    container_name: joinnode1
    image: joinnode:latest
    entrypoint: python3 /joinnode/main.py
    environment:
      - NODE_IND=0
      - NODE_COUNT=1
      - NODE_ID=0
    networks:
      - testing_net
    depends_on:
      - middleware
    links:
      - middleware
    volumes:
      - ./joinnode/config.ini:/config.ini:ro
      
" >> "$output_file_name"

echo "networks:
  testing_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24" >> "$output_file_name"