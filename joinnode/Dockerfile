FROM python:3.9.7-slim
#FROM rabbitmq-python-base:0.0.1
RUN pip install pika

WORKDIR /
COPY selectnode /selectnode
COPY common /common
COPY middleware/src /middleware
ENV PYTHONPATH=/

RUN python -m unittest discover -s selectnode/tests -p 'test_*.py'

#RUN python -m unittest selectnode/tests/
ENTRYPOINT ["/bin/sh"]
