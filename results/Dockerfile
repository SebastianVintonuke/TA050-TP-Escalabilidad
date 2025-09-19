FROM python:3.9.7-slim
WORKDIR /
COPY results /results
COPY common /common
ENV PYTHONPATH=/
RUN python -m unittest results/tests/tests.py
ENTRYPOINT ["/bin/sh"]
