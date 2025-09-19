FROM python:3.9.7-slim
WORKDIR /
COPY selectnode /selectnode
COPY common /common
ENV PYTHONPATH=/
RUN python -m unittest selectnode/tests/tests.py
ENTRYPOINT ["/bin/sh"]
