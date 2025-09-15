# Simple helpers
.PHONY: install run test clean

install:
	python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt || true

run:
	python -m etl.cli run --source data/raw/orders.csv --db data/warehouse.db

test:
	pytest -q

clean:
	rm -rf __pycache__ .pytest_cache data/warehouse.db data/processed/*
