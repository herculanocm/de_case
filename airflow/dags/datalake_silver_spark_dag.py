from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
    

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
        dag_id='datalake_silver_spark_dag',
        schedule_interval=None,
        start_date=datetime(2024, 10, 18),
        default_args=default_args,
        concurrency=30,
        catchup=False,max_active_runs=1,
        tags=['datalake', 'pipe', 'silver'],
) as dag:
    
    task_init_seq_01 = EmptyOperator(
        task_id='task_init_seq_01'
    )

    task_run_spark_job_silver = DockerOperator(
        task_id='task_run_spark_job_silver',
        image='bitnami/spark:3.4.1',
        api_version='auto',
        auto_remove=True,
        command='/opt/bitnami/spark/bin/spark-submit /app/data/job_silver.py --arg1 value1',
        docker_url='unix://var/run/docker.sock',
        network_mode='decase',
       mounts=[
            Mount(
                source='/home/hcunha/dev/docker/de_case/spark/jobs',
                target='/app/data',
                type='bind'
            ),
        ],
        environment={
            'SPARK_MASTER_URL': 'spark://spark-master:7077',
            'SPARK_HOME': '/opt/bitnami/spark',
        },
        mount_tmp_dir=False,
    )

task_init_seq_01 >> task_run_spark_job_silver