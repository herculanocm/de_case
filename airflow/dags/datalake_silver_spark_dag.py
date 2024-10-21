from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

MINIO_LAND_BUCKET_NAME = 'datalake-bronze' # Variable.get("MINIO_LAND_BUCKET_NAME")
MINIO_DATASET_NAME = 'brewery' # Variable.get("MINIO_DATASET_NAME")
MINIO_ENDPOINT = 'minio:9000' # Variable.get("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = 'admin' # Variable.get("minio_access_key")
MINIO_SECRET_KEY = 'password' # Variable.get("minio_secret_key")
MINIO_DATALAKE_WAREHOUSE = 's3a://datalake-gold/warehouse' # Variable.get("MINIO_DATALAKE_WAREHOUSE")
NESSIE_URI = 'http://nessie:19120/api/v1' # Variable.get("NESSIE_URI")

def get_datetime_UTC_SaoPaulo(execution_date: datetime) -> str:
    return (execution_date - timedelta(hours=3)).strftime('%Y-%m-%d_%H:%M:%S')

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
        params={"custom_param": "default_value"},
        catchup=False,
        tags=['datalake', 'pipe', 'silver'],
        user_defined_macros={
            'get_datetime_UTC_SaoPaulo': get_datetime_UTC_SaoPaulo,
        }
) as dag:
    
    task_init_seq_01 = EmptyOperator(
        task_id='task_init_seq_01'
    )

    

    task_run_spark_job_silver = DockerOperator(
        task_id='task_run_spark_job_silver',
        image='hcunha/spark:3.4.1',
        api_version='auto',
        auto_remove=True,
        command="""
        /opt/bitnami/spark/bin/spark-submit \
            --master local[*] \
            --deploy-mode client \
            --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
            --conf spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog \
            --conf spark.sql.catalog.nessie.warehouse={{ params.minio_datalake_warehouse }} \
            --conf spark.sql.catalog.nessie.uri={{ params.nessie_uri }} \
            --conf spark.sql.catalog.nessie.ref=main \
            --conf spark.sql.catalog.nessie.auth-type=NONE \
            --conf spark.hadoop.fs.s3a.endpoint=http://{{ params.minio_endpoint }} \
            --conf spark.hadoop.fs.s3a.access.key={{ params.minio_access_key }} \
            --conf spark.hadoop.fs.s3a.secret.key={{ params.minio_secret_key }} \
            --conf spark.hadoop.fs.s3a.path.style.access=true \
            --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
            --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
            --conf spark.job_silver_app.bucket_name={{ params.bucket_name }} \
            --conf spark.job_silver_app.dataset_name={{ params.dataset_name }} \
            --conf spark.job_silver_app.datetime_ref={{ get_datetime_UTC_SaoPaulo(execution_date) }} \
            /app/data/jobs/job_silver.py
        """,
        docker_url='unix://var/run/docker.sock',
        network_mode='decase',
        mounts=[
            Mount(
                source='/home/hcunha/dev/docker/de_case/spark',
                target='/app/data',
                type='bind'
            ),
        ],
        environment={
            'SPARK_HOME': '/opt/bitnami/spark',
        },
        mount_tmp_dir=False,
        params={
            'bucket_name': MINIO_LAND_BUCKET_NAME,
            'dataset_name': MINIO_DATASET_NAME,
            'minio_endpoint': MINIO_ENDPOINT,
            'minio_access_key': MINIO_ACCESS_KEY,
            'minio_secret_key': MINIO_SECRET_KEY,
            'minio_datalake_warehouse': MINIO_DATALAKE_WAREHOUSE,
            'nessie_uri': NESSIE_URI,
        },
    )

task_init_seq_01 >> task_run_spark_job_silver