FROM python:3.9.7-slim
RUN pip install --no-cache-dir pika
WORKDIR /
COPY dispatcher /dispatcher
COPY common /common
COPY middleware /middleware
ENV PYTHONPATH=/
RUN python -m unittest dispatcher/tests/tests.py
ENTRYPOINT ["/bin/sh"]
