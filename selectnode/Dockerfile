FROM python:3.9.7-slim
#FROM rabbitmq-python-base:0.0.1
RUN pip install pika

WORKDIR /
COPY selectnode /selectnode
COPY common /common
ENV PYTHONPATH=/
RUN python -m unittest selectnode/tests/tests.py
ENTRYPOINT ["/bin/sh"]
