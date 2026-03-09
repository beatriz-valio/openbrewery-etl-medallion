FROM apache/airflow:2.8.4

ENV PYTHONPATH=/opt/airflow

USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
