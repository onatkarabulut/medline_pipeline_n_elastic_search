from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email': ['airflow@example.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='extended_scraping_http_dag',
    default_args=default_args,
    schedule_interval=timedelta(hours=5),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['scraping','fastapi']
) as dag:
    trigger_scraping = SimpleHttpOperator(
        task_id='trigger_scraping',
        http_conn_id='my_fastapi_conn',
        endpoint='/scraping/start',
        method='POST',
        data='{"start":0,"end":-1}',
        headers={'Content-Type': 'application/json'},
        log_response=True
    )

    wait_scraping = HttpSensor(
        task_id='wait_scraping',
        http_conn_id='my_fastapi_conn',
        endpoint='/scraping/status',
        request_params={'check': 'done'},
        method='GET',
        poke_interval=30,
        timeout=1800
    )

    finish = BashOperator(
        task_id='finish',
        bash_command='echo "Scraping pipeline completed at $(date)"'
    )

    trigger_scraping >> wait_scraping >> finish
