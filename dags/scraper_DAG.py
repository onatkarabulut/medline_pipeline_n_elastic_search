from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.bash import BashOperator
import json

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=10)
}

with DAG(
    dag_id='advanced_scraping_dag',
    default_args=default_args,
    schedule='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    tags=['production', 'scraping']
) as dag:

    start_pipeline = BashOperator(
        task_id='start_pipeline',
        bash_command='echo "Scraping STARTED at $(date)"'
    )

    trigger_scraping = HttpOperator(
        task_id='trigger_scraping',
        http_conn_id='my_fastapi_conn',
        endpoint='extra/scraping/start',
        method='POST',
        data=json.dumps({"end": -1}),
        headers={
            'accept': 'application/json',
            'Content-Type': 'application/json'
        },
        response_check=lambda response: response.status_code == 200,
        log_response=True,
        extra_options={'timeout': 300}
    )

    monitor_scraping = HttpSensor(
        task_id='monitor_scraping',
        http_conn_id='my_fastapi_conn',
        endpoint='extra/scraping/status',
        method='GET',
        response_check=lambda response: 'status' in response.json() and response.json()['status'] == 'completed',
        poke_interval=60,
        timeout=3600,
        mode='reschedule'
    )

    cleanup_resources = BashOperator(
        task_id='cleanup_resources',
        bash_command='echo "Cleaning temporary files..." && sleep 10'
    )

    send_notification = BashOperator(
        task_id='send_notification',
        bash_command='echo "Scraping COMPLETED at $(date) | mail -s \'Scraping Report\' admin@example.com"'
    )

    start_pipeline >> trigger_scraping >> monitor_scraping 
    monitor_scraping >> [cleanup_resources, send_notification]