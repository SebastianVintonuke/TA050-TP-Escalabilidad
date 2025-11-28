FROM python:3.12-slim

WORKDIR /healthchecker

COPY healthchecker /healthchecker
COPY common /common

ENV PYTHONPATH=/

ENTRYPOINT ["python", "/healthchecker/main.py"]
