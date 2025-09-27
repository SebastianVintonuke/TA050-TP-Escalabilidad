SHELL := /bin/bash
PWD := $(shell pwd)

default: build

all:

deps:

build: deps

.PHONY: build

docker-image:
	docker build -f ./client/Dockerfile -t "client:latest" .
	docker build -f ./dispatcher/Dockerfile -t "dispatcher:latest" .
	docker build -f ./middleware/Dockerfile -t "middleware:latest" .
	docker build -f ./resultnode/Dockerfile -t "resultnode:latest" .
	docker build -f ./results/Dockerfile -t "results:latest" .
	docker build -f ./selectnode/Dockerfile -t "selectnode:latest" .
	docker build -f ./groupbynode/Dockerfile -t "groupbynode:latest" .
	docker build -f ./server/Dockerfile -t "server:latest" .

	# Execute this command from time to time to clean up intermediate stages generated 
	# during client build (your hard drive will like this). Don't left uncommented if you 
	# want to avoid rebuilding client image every time the docker-compose-up command 
	# is executed, even when client code has not changed
	# docker rmi `docker images --filter label=intermediateStageToBeDeleted=true -q`

.PHONY: docker-image

docker-compose-up: docker-image
	docker compose -f docker-compose-dev.yaml up -d --build
.PHONY: docker-compose-up

docker-compose-down:
	docker compose -f docker-compose-dev.yaml stop -t 1
	docker compose -f docker-compose-dev.yaml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker compose -f docker-compose-dev.yaml logs -f
.PHONY: docker-compose-logs
