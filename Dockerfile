# Minimal container for ETL
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY src ./src
COPY data ./data

ENV PYTHONPATH=/app/src

CMD ["python", "-m", "etl.cli", "run", "--source", "data/raw/orders.csv", "--db", "data/warehouse.db"]
