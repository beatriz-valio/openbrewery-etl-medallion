.PHONY: help airflow-init airflow-up airflow-down airflow-logs test lint format clean

help:
	@echo "Commands:"
	@echo "  make airflow-init   - init Airflow DB and admin user"
	@echo "  make airflow-up     - start Airflow (webserver + scheduler + postgres)"
	@echo "  make airflow-down   - stop containers"
	@echo "  make airflow-logs   - tail logs"
	@echo "  make test           - run tests"
	@echo "  make lint           - run linter"
	@echo "  make format         - format code"
	@echo "  make clean          - remove python caches"

airflow-init:
	docker compose up --abort-on-container-exit --remove-orphans airflow-init

airflow-up:
	docker compose up -d

airflow-down:
	docker compose down -v

airflow-logs:
	docker compose logs -f --tail=200

test:
	pytest -q

lint:
	flake8 . --max-line-length=100

format:
	black .

clean:
	python -c "import shutil; from pathlib import Path; [shutil.rmtree(p) for p in Path('.').glob('__pycache__') if p.is_dir()]" \
	&& python -c "import shutil; from pathlib import Path; [shutil.rmtree(p) for p in Path('.').glob('.pytest_cache') if p.is_dir()]" \
	&& python -c "import shutil; from pathlib import Path; [shutil.rmtree(p) for p in Path('.').glob('.mypy_cache') if p.is_dir()]" \
	&& python -c "import shutil; from pathlib import Path; [shutil.rmtree(p) for p in Path('.').glob('.ruff_cache') if p.is_dir()]"
