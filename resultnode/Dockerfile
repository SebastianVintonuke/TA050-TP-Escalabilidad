FROM python:3.9.7-slim
RUN pip install pika


WORKDIR /
COPY resultnode /resultnode
COPY common /common
COPY middleware /middleware

ENV PYTHONPATH=/
RUN python -m unittest resultnode/tests/tests.py
ENTRYPOINT ["/bin/sh"]
