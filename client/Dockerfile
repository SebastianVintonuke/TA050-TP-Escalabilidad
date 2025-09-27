FROM python:3.9.7-slim
WORKDIR /
COPY client /client
COPY common /common
COPY middleware /middleware
ENV PYTHONPATH=/
RUN python -m unittest client/tests/tests.py
ENTRYPOINT ["/bin/sh"]
