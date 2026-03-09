import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.jobs.bronze import extract_to_bronze
from src.jobs.silver import bronze_to_silver
from src.jobs.gold import silver_to_gold
from src.jobs.publish import publish_gold
from src.data_quality.checks import run_dq_checks

logger = logging.getLogger("airflow.task")


def _log_context(context) -> None:
    task_instance = context["ti"]
    logger.info(
        "Context: dag_id=%s task_id=%s run_id=%s logical_date=%s try=%s/%s",
        task_instance.dag_id,
        task_instance.task_id,
        context.get("run_id"),
        context.get("ds"),
        task_instance.try_number,
        task_instance.max_tries,
    )
    logger.info("Base path: %s", context["params"]["base_path"])


def _on_failure_callback(context) -> None:
    task_instance = context.get("ti")
    exc = context.get("exception")
    logger.error(
        "Task failed: dag_id=%s task_id=%s run_id=%s ds=%s try=%s log_url=%s exception=%r",
        getattr(task_instance, "dag_id", None),
        getattr(task_instance, "task_id", None),
        context.get("run_id"),
        context.get("ds"),
        getattr(task_instance, "try_number", None),
        getattr(task_instance, "log_url", None),
        exc,
    )


BASE_PATH = str(os.getenv("DATA_LAKE_BASE_PATH", "/opt/airflow/data"))
DEFAULT_ARGS = {
    "owner": "beatriz-valio",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "on_failure_callback": _on_failure_callback,
}


with DAG(
    dag_id="openbrewery_medallion",
    description="Brewery Medallion Pipeline: ETL from brewery api to a medallion architecture",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 3, 3),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["breweries", "medallion"],
    params={"base_path": BASE_PATH},
) as dag:
    run_bronze = PythonOperator(
        task_id="extract_bronze",
        python_callable=extract_to_bronze,
        op_kwargs={
            "base_path": "{{ params.base_path }}",
            "ds": "{{ ds }}",
            "run_id": "{{ run_id }}",
        },
        pre_execute=_log_context,
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=5),
    )

    run_silver = PythonOperator(
        task_id="transform_silver",
        python_callable=bronze_to_silver,
        op_kwargs={
            "base_path": "{{ params.base_path }}",
            "ds": "{{ ds }}",
            "run_id": "{{ run_id }}",
        },
        pre_execute=_log_context,
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=5),
    )

    run_gold = PythonOperator(
        task_id="aggregate_gold",
        python_callable=silver_to_gold,
        op_kwargs={
            "base_path": "{{ params.base_path }}",
            "ds": "{{ ds }}",
            "run_id": "{{ run_id }}",
        },
        pre_execute=_log_context,
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=3),
    )

    run_data_quality_checks = PythonOperator(
        task_id="data_quality_checks",
        python_callable=run_dq_checks,
        op_kwargs={
            "base_path": "{{ params.base_path }}",
            "ds": "{{ ds }}",
            "run_id": "{{ run_id }}",
        },
        pre_execute=_log_context,
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=2),
    )

    publish_gold_task = PythonOperator(
        task_id="publish_gold",
        python_callable=publish_gold,
        op_kwargs={
            "base_path": "{{ params.base_path }}",
            "ds": "{{ ds }}",
            "run_id": "{{ run_id }}",
        },
        pre_execute=_log_context,
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=2),
    )

    run_bronze >> run_silver >> run_gold >> run_data_quality_checks >> publish_gold_task
