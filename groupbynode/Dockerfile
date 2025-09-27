FROM python:3.9.7-slim
#FROM rabbitmq-python-base:0.0.1
RUN pip install pika

WORKDIR /
COPY groupbynode /groupbynode
COPY common /common
COPY middleware/src /middleware
ENV PYTHONPATH=/

RUN python -m unittest discover -s groupbynode/tests -p 'test_*.py'

#RUN python -m unittest groupbynode/tests/
ENTRYPOINT ["/bin/sh"]
