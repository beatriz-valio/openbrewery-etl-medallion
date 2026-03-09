.PHONY: help airflow-build airflow-init airflow-up airflow-start airflow-down airflow-reset airflow-logs test lint format clean

help:
	@echo "Commands:"
	@echo "  make airflow-build   - build Docker images"
	@echo "  make airflow-init    - init Airflow DB and admin user"
	@echo "  make airflow-up      - start Airflow services"
	@echo "  make airflow-start   - build, init, and start everything"
	@echo "  make airflow-down    - stop containers"
	@echo "  make airflow-reset   - stop containers and remove volumes"
	@echo "  make airflow-logs    - tail logs"
	@echo "  make test           - run tests"
	@echo "  make lint           - run linter"
	@echo "  make format         - format code"
	@echo "  make clean          - remove python caches"

airflow-build:
	docker compose build

airflow-init:
	docker compose up --build --abort-on-container-exit --remove-orphans airflow-init

airflow-up:
	docker compose up -d --remove-orphans

airflow-start: airflow-init airflow-up

airflow-down:
	docker compose down

airflow-reset:
	docker compose down -v --remove-orphans

airflow-logs:
	docker compose logs -f --tail=200

test:
	pytest -q

lint:
	flake8 . --max-line-length=100

format:
	black .

clean:
	python -c "import shutil; from pathlib import Path; [shutil.rmtree(p, ignore_errors=True) for p in Path('.').rglob('__pycache__') if p.is_dir()]" && \
	python -c "import shutil; from pathlib import Path; [shutil.rmtree(p, ignore_errors=True) for p in Path('.').rglob('.pytest_cache') if p.is_dir()]" && \
	python -c "import shutil; from pathlib import Path; [shutil.rmtree(p, ignore_errors=True) for p in Path('.').rglob('.mypy_cache') if p.is_dir()]" && \
	python -c "import shutil; from pathlib import Path; [shutil.rmtree(p, ignore_errors=True) for p in Path('.').rglob('.ruff_cache') if p.is_dir()]"
