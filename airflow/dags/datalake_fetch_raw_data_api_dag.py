from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

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
        # schedule_interval=None,
        start_date=datetime(2023, 10, 18),
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
        python_callable=lambda: print('Hello World teste')
    )

task_init_seq_01 >> task_print_test