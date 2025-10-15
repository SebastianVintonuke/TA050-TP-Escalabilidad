FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

RUN pip install pandas

WORKDIR /app

COPY pandas_comparator.py /app/pandas_comparator.py

ENTRYPOINT ["python", "/app/pandas_comparator.py"]
