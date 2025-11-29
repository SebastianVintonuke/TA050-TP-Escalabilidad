#FROM python:3.12-slim
FROM python:3.9.7-slim

WORKDIR /

COPY supervisor/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /
COPY supervisor /supervisor
COPY common /common
ENV PYTHONPATH=/

#RUN python -m unittest discover -s supervisor/tests -p 'test_*.py'

ENTRYPOINT ["/bin/sh"]
