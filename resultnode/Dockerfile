FROM python:3.9.7-slim
WORKDIR /
COPY resultnode /resultnode
COPY common /common
ENV PYTHONPATH=/
RUN python -m unittest resultnode/tests/tests.py
ENTRYPOINT ["/bin/sh"]
