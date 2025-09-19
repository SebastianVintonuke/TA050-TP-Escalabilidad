FROM python:3.9.7-slim
WORKDIR /
COPY dispatcher /dispatcher
COPY common /common
ENV PYTHONPATH=/
RUN python -m unittest dispatcher/tests/tests.py
ENTRYPOINT ["/bin/sh"]
