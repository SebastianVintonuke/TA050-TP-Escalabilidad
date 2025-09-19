FROM python:3.9.7-slim
WORKDIR /
COPY middleware /middleware
COPY common /common
ENV PYTHONPATH=/
RUN python -m unittest middleware/tests/tests.py
ENTRYPOINT ["/bin/sh"]
