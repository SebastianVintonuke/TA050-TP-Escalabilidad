FROM python:3.9.7-slim
WORKDIR /
COPY server /server
COPY common /common
ENV PYTHONPATH=/
RUN python -m unittest server/tests/tests.py
ENTRYPOINT ["/bin/sh"]
