from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import sys
import time
import urllib3
import json
import logging
from operators.fetch_api_operator import FetchApiOperator


def fetch_api(url: str, max_retries: int = 5, wait_time: int = 5, type_request: str = 'GET'):
    retries = 0
    while retries < max_retries:
        logging.info(f"Attempt: {retries + 1}")

        headers_authentication = {
            'Content-Type': 'application/json',
        }

        try:
            http = urllib3.PoolManager()
            resp = http.request(type_request, url, headers=headers_authentication)
            if resp.status > 199 and resp.status < 300:
                retries = max_retries + 1
                return resp
            else:
                logging.warning(f"Status error: {resp.status}")
                logging.warning("Running again")
        except Exception as e:
            logging.error(f"Erro: {e}")
            
        time.sleep(wait_time)
        retries = retries + 1

    logging.error("Limit exception, retries 10")
    sys.exit(1)


def fetch_metadata(**context):
    logging.info("Fetching metadata")
    url = "https://api.openbrewerydb.org/v1/breweries/meta"
    operator = FetchApiOperator(
        task_id='fetch_metadata_task',
        url=url,
        xcom_key="metadata",
        max_retries=5,
        wait_time=5,
        type_request='GET'
    )
    response = operator.execute(context)
    metadata = json.loads(response.data.decode('utf-8'))
    logging.info(f"Metadata: {metadata}")
    

default_args = {
    'owner': 'herculanocm',
    "email": ["herculanocm@outlook.com"],
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'email_on_success': False,
    'on_failure_callback': None,
    'on_success_callback': None,
    'retries': 2,
}

with DAG(
        dag_id='datalake_fetch_raw_data_api_dag',
        schedule_interval=None,
        start_date=datetime(2024, 10, 18),
        default_args=default_args,
        concurrency=30,
        catchup=False,max_active_runs=1,
        tags=['datalake', 'pipe', 'raw', 'api'],
) as dag:
    
    task_init_seq_01 = EmptyOperator(
        task_id='task_init_seq_01'
    )

    task_print_test = PythonOperator(
        task_id='task_print_test',
        provide_context=True,
        python_callable=fetch_metadata
    )

task_init_seq_01 >> task_print_test