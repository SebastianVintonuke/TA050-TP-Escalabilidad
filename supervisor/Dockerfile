FROM python:3.12-slim

WORKDIR /

COPY supervisor/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY supervisor /supervisor
COPY common /common

ENV PYTHONPATH=/

RUN python -m pytest supervisor/tests/ -v

ENTRYPOINT ["python", "/supervisor/main.py"]
