FROM python:3.9.7-slim
#FROM rabbitmq-python-base:0.0.1
RUN pip install pika

WORKDIR /
COPY selectnode /selectnode
COPY groupbynode /groupbynode
COPY joinnode /joinnode
COPY integration_tests /integration_tests
COPY common /common
COPY middleware/src /middleware

ENV PYTHONPATH=/

#RUN python -m unittest discover -s integration_tests/tests -p 'test_*.py'
#RUN python -m unittest integration_tests/tests/
ENTRYPOINT ["/bin/sh"]
