FROM python:3.9.7-slim
RUN pip install --no-cache-dir pika
WORKDIR /
COPY results /results
COPY common /common
COPY middleware /middleware
ENV PYTHONPATH=/
RUN python -m unittest results/tests/tests.py
ENTRYPOINT ["/bin/sh"]
