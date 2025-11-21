FROM python:3.9.7-slim
#FROM rabbitmq-python-base:0.0.1
RUN pip install pika

WORKDIR /
COPY selectnode/src /selectnode/src
COPY groupbynode/src /groupbynode/src
COPY joinnode/src /joinnode/src
COPY integration_tests/src /integration_tests/src
COPY integration_tests/main.py /main.py
COPY common /common
COPY middleware/src /middleware

ENV PYTHONPATH=/

#RUN python -m unittest discover -s integration_tests/tests -p 'test_*.py'
#RUN python -m unittest integration_tests/tests/
ENTRYPOINT ["/bin/sh"]
