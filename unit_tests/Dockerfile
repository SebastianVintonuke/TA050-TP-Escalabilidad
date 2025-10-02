FROM python:3.9.7-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app

COPY unit_tests/requirements.txt /tmp/requirements.txt
RUN python -m pip install --upgrade pip \
 && pip install -r /tmp/requirements.txt

CMD ["pytest", "-v"]
