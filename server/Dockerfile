FROM python:3.9.7-slim
RUN pip install --no-cache-dir pika
WORKDIR /
COPY server /server
COPY common /common
COPY middleware /middleware
ENV PYTHONPATH=/
RUN python -m unittest server/tests/tests.py
ENTRYPOINT ["/bin/sh"]
