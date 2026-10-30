# Everything runs inside the project folder: .venv (uv), .local/ (lake, logs, caches), Docker (Colima VM).
SHELL := /bin/bash
export UV_CACHE_DIR := $(CURDIR)/.local/uv-cache
# Uses an installed Python 3.12 if there is one; otherwise uv downloads it into the project folder.
export UV_PYTHON_INSTALL_DIR := $(CURDIR)/.local/python

COMPOSE := $(shell docker compose version >/dev/null 2>&1 && echo "docker compose" || echo docker-compose)
AIRFLOW := $(COMPOSE) exec -T airflow-scheduler
DEMO := $(AIRFLOW) python /opt/airflow/scripts/demo_versioning.py
DATE ?= 2026-10-29
TABLE ?= daily_quotes
VOLUME ?= 12345678

.PHONY: help setup env lint test dag-test fixtures deploy deploy-from-ci build up down logs trigger status \
	demo-snapshots demo-asof demo-correct demo-rollback demo-evolve dev-backfill clean \
	bigdata-up bigdata-down trino-sql

help: ## list targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-16s %s\n", $$1, $$2}'

setup: ## create .venv with runtime + dev dependencies
	uv sync

env: ## create .env with fresh local-only secrets (gitignored)
	@if [ -f .env ]; then echo ".env exists, keeping it"; else \
	  { echo "AIRFLOW_UID=$$(id -u)"; \
	    echo "AIRFLOW_JWT_SECRET=$$(openssl rand -hex 32)"; \
	    echo "AIRFLOW_FERNET_KEY=$$(python3 -c 'import base64,os;print(base64.urlsafe_b64encode(os.urandom(32)).decode())')"; \
	    echo "TWSE_SOURCE=twse"; \
	    echo "# ANTHROPIC_API_KEY=   # enables the LLM unscheduled-closure check"; } > .env; \
	  echo "created .env"; fi

lint: ## ruff lint + format check
	uv run ruff check . && uv run ruff format --check .

test: ## unit + end-to-end tests on recorded fixtures (no network)
	uv run pytest

dag-test: ## DAG imports without errors and has no cycles, inside the production image
	docker run --rm -v "$(CURDIR)":/repo -w /repo -e PYTHONPATH=/repo/src twse-airflow:local \
	  bash -c "pip install -q pytest && python -m pytest -o addopts='' -m airflow tests/test_dag_integrity.py"

fixtures: ## re-record raw TWSE payloads into fixtures/ (network)
	uv run python scripts/record_fixtures.py

deploy: ## CD: sync dags/ + twse_etl/ into the folder Airflow mounts
	scripts/deploy_dags.sh

deploy-from-ci: ## pull the bundle GitHub CD built for origin/main into the local Airflow mount
	git fetch -q origin main
	rm -rf .local/ci-bundle
	gh run download --name "airflow-dags-$$(git rev-parse origin/main)" --dir .local/ci-bundle
	rsync -a --delete .local/ci-bundle/airflow_dags/ deploy/airflow_dags_mount/
	@echo "mounted bundle $$(cat deploy/airflow_dags_mount/DEPLOYED_VERSION) from GitHub CD"; cat .local/ci-bundle/deploy.log

build: ## build the Airflow image
	$(COMPOSE) build

up: env deploy build ## start Airflow on http://localhost:8080
	mkdir -p .local/airflow/logs .local/lake
	$(COMPOSE) up -d --wait

down: ## stop Airflow (data in .local/ is kept)
	$(COMPOSE) down

logs: ## follow scheduler logs
	$(COMPOSE) logs -f airflow-scheduler

trigger: ## run the DAG for DATE (default 2026-10-29)
	$(AIRFLOW) airflow dags unpause twse_daily_etl
	$(AIRFLOW) airflow dags trigger twse_daily_etl --conf '{"trade_date": "$(DATE)"}'

status: ## task states of recent runs
	$(AIRFLOW) airflow dags list-runs twse_daily_etl

demo-snapshots: ## [required] snapshot list of TABLE
	$(DEMO) snapshots --table $(TABLE)

demo-asof: ## [required] time travel: SNAPSHOT=<id> or AT='2026-10-30 18:00'
	$(if $(or $(SNAPSHOT),$(AT)),,$(error give SNAPSHOT=<id> or AT='YYYY-MM-DD HH:MM'))
	$(DEMO) as-of --table $(TABLE) $(if $(SNAPSHOT),--snapshot-id $(SNAPSHOT),--at "$(AT)") --stock 2330 2317

demo-correct: ## [required] simulate a TWSE correction of DATE (2330 volume -> VOLUME), show before/after
	$(DEMO) correct --date $(DATE) --stock 2330 --volume $(VOLUME)

demo-rollback: ## [optional] undo the last run in every table it wrote (or only TABLE=...)
	$(DEMO) rollback $(if $(filter command line,$(origin TABLE)),--table $(TABLE))

demo-evolve: ## [optional] add a column; old rows and snapshots stay queryable
	$(DEMO) add-column --table $(TABLE)

dev-backfill: ## run the pipeline without Airflow on fixtures into .local/dev
	TWSE_SOURCE=fixture uv run python -m twse_etl.pipeline backfill --start 2026-10-22 --end 2026-10-29

clean: ## delete local lake + logs (asks first)
	@read -p "delete .local/lake .local/dev .local/airflow? [y/N] " ok && [ "$$ok" = y ] && rm -rf .local/lake .local/dev .local/airflow

BIGDATA := $(COMPOSE) -f docker-compose.yaml -f docker-compose.bigdata.yaml

bigdata-up: env deploy build ## big-data layout: S3 (SeaweedFS) + Postgres Iceberg catalog + Trino on :8081
	mkdir -p .local/airflow/logs .local/lake .local/object-store
	$(BIGDATA) up -d --wait

bigdata-down: ## stop the big-data layout
	$(BIGDATA) down

# $(value SQL) passes the text unexpanded, so metadata tables like "daily_quotes$snapshots" survive make
trino-sql: export TRINO_SQL = $(value SQL)
trino-sql: ## SQL on Trino: make trino-sql SQL='select * from iceberg.twse."daily_quotes$snapshots"'
	set -o pipefail; $(BIGDATA) exec -T -e TRINO_SQL trino sh -c 'trino --output-format ALIGNED --execute "$$TRINO_SQL"' \
		2>&1 | grep -v -e 'org.jline' -e 'Unable to create a system terminal'
