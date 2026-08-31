.PHONY: help setup infra-up infra-down init-db ingest process dashboard synthetic-spike synthetic-editwar synthetic-normal test purge

help:
	@echo "WikiPulse local development commands"
	@echo "  make setup             Install Python dependencies"
	@echo "  make infra-up          Start Kafka + PostgreSQL via Docker Compose"
	@echo "  make infra-down        Stop and remove infrastructure containers"
	@echo "  make init-db           Apply the PostgreSQL schema"
	@echo "  make ingest            Run the Wikimedia SSE ingestion service"
	@echo "  make process           Run the Spark Structured Streaming job"
	@echo "  make dashboard         Run the Streamlit dashboard"
	@echo "  make synthetic-normal  Publish steady synthetic background edits"
	@echo "  make synthetic-spike   Publish a synthetic activity spike"
	@echo "  make synthetic-editwar Publish a synthetic reciprocal-revert edit war"
	@echo "  make test              Run the test suite"
	@echo "  make purge             Purge old PostgreSQL rows per retention policy"

setup:
	pip install -r requirements.txt

infra-up:
	docker compose up -d kafka kafka-init-topics postgres

infra-down:
	docker compose down

init-db:
	python -m scripts.init_db

ingest:
	python -m ingestion.sse_consumer

process:
	python -m processing.streaming_job

dashboard:
	streamlit run dashboard/app.py

synthetic-normal:
	python -m scripts.generate_synthetic_events --scenario normal --duration 120

synthetic-spike:
	python -m scripts.generate_synthetic_events --scenario spike --page "Synthetic Demo Page" --count 40

synthetic-editwar:
	python -m scripts.generate_synthetic_events --scenario edit_war --page "Synthetic Demo Page" --rounds 6

test:
	pytest -v

purge:
	python -m scripts.purge_old_data
