from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from minio import Minio
from minio.error import S3Error
from airflow.exceptions import AirflowException
from operators.fetch_api_operator import FetchApiOperator
import logging
import json
import io
import math

logging.basicConfig(level=logging.INFO)
STATIC_BASE_URL = 'https://api.openbrewerydb.org' # Best way get from Airflow variable  Variable.get('STATIC_OPEN_BREWERY_DB_BASE_URL'), but for this example we are using a static value
    
MINIO_ACCESS_KEY = 'admin' # Variable.get("minio_access_key")
MINIO_SECRET_KEY = 'password' # Variable.get("minio_secret_key")
MINIO_BUCKET_NAME = 'datalake-bronze' # Variable.get("minio_bucket_name")

INT_NODES = 3 # Number of nodes to distribute the pages

LST_TASKS_NODES = [] # List to store the nodes tasks

def delete_files_from_minio(bucket_name, file_prefix, minio_endpoint, access_key, secret_key, secure=False):
    
    client = Minio(
        minio_endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure
    )
    
    try:
        if client.bucket_exists(bucket_name):
            objects = client.list_objects(bucket_name, prefix=file_prefix, recursive=True)
            for obj in objects:
                client.remove_object(bucket_name, obj.object_name)
                logging.info(f"Deleted {obj.object_name} from {bucket_name}")
    except Exception as e:
        raise AirflowException(f"Error deleting files from Minio: {e}")
    
def save_json_to_minio(json_str, bucket_name, file_name, minio_endpoint, access_key, secret_key, secure=False):
    """
    Save a JSON string as a file on MinIO.

    Parameters:
    - json_str (str): The JSON string to save.
    - bucket_name (str): The name of the MinIO bucket.
    - file_name (str): The name of the file to save (including path if needed).
    - minio_endpoint (str): The MinIO endpoint (e.g., 'minio:9000').
    - access_key (str): MinIO access key.
    - secret_key (str): MinIO secret key.
    - secure (bool): Whether to use HTTPS (True) or HTTP (False).

    Raises:
    - AirflowException: If there is an error saving the file to MinIO.
    """
    # Create a MinIO client
    client = Minio(
        minio_endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure
    )
    
    try:
        # Check if the bucket exists; if not, create it
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logging.info(f"Bucket '{bucket_name}' created.")
        else:
            logging.info(f"Bucket '{bucket_name}' already exists.")
        
        # Convert the JSON string to bytes
        json_bytes = json_str.encode('utf-8')
        json_stream = io.BytesIO(json_bytes)
        file_size = len(json_bytes)
        
        # Upload the JSON file to MinIO
        client.put_object(
            bucket_name=bucket_name,
            object_name=file_name,
            data=json_stream,
            length=file_size,
            content_type='application/json'
        )
        logging.info(f"File '{file_name}' saved to bucket '{bucket_name}'.")
    except S3Error as e:
        raise AirflowException(f"Error saving file to MinIO: {e}")
    
def is_valid_json(json_str) -> bool:
    """
    Check if a given string is valid JSON.

    Parameters:
    - json_str (str): The JSON string to validate.

    Returns:
    - bool: True if the string is valid JSON and list and if list has more than one False otherwise.
    """
    try:
        lst_ret = json.loads(json_str)
        if isinstance(lst_ret, list) and len(lst_ret) > 0:
            return True 
        return False
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON: {e}")
        return False
    
def get_nodes_pages(total_pages: int, total_nodes: int) -> list:
    """
    Distribute pages among nodes as evenly as possible.

    Parameters:
    - total_pages (int): The total number of pages to distribute.
    - total_nodes (int): The total number of nodes.

    Returns:
    - list: A list of dictionaries, each containing a node number and a list of assigned pages.
            Example: [{'node': 1, 'pages': [1, 2, 3]}, {'node': 2, 'pages': [4, 5, 6]}]
    """
    if total_nodes <= 0:
        raise ValueError("Total nodes must be a positive integer.")
    if total_pages < 0:
        raise ValueError("Total pages cannot be negative.")

    # Create a list of page numbers
    pages = list(range(1, total_pages + 1))

    # Calculate the base number of pages per node and the remainder
    pages_per_node, remainder = divmod(len(pages), total_nodes)

    # Distribute pages among nodes
    nodes_pages = []
    start_index = 0
    for node_index in range(total_nodes):
        # Distribute the remainder pages one by one
        extra_page = 1 if node_index < remainder else 0
        end_index = start_index + pages_per_node + extra_page
        assigned_pages = pages[start_index:end_index]
        nodes_pages.append({'node': node_index + 1, 'pages': assigned_pages})
        start_index = end_index

    return nodes_pages

def clean_breweries_meta(**kwargs):
    ti = kwargs['ti']
    breweries_meta_data = json.loads(ti.xcom_pull(task_ids='task_fetch_breweries_meta', key='breweries_meta_data'))
    if 'total' in breweries_meta_data and int(breweries_meta_data['total']) > 0:
        int_pages = math.ceil(int(breweries_meta_data['total']) / 200)
        execution_date = kwargs['execution_date']
        logging.info(f"Total pages of breweries: {int_pages}, execution_date: {execution_date.strftime('%Y-%m-%d')}")
        file_prefix = f"brewery/sys_file_date={execution_date.strftime('%Y-%m-%d')}"
        logging.info(f"Deleting files from Minio with prefix: {file_prefix}")
        delete_files_from_minio(MINIO_BUCKET_NAME, file_prefix, 'minio:9000', MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
        lst_dicts_nodes_pages = get_nodes_pages(int_pages, INT_NODES)
        logging.info(f"Nodes and pages: {lst_dicts_nodes_pages}")
        ti.xcom_push(key='nodes_distribuition_data', value=json.dumps(lst_dicts_nodes_pages))

        return 'task_seq_02'
    return 'task_end_seq_01'

def fech_breweries_node(**kwargs):
    ti = kwargs['ti']
    node = kwargs['node'] + 1
    lst_dicts_nodes_pages = json.loads(ti.xcom_pull(task_ids='task_check_and_clean_breweries_meta', key='nodes_distribuition_data'))
    list_node = list(filter(lambda n: n['node'] == node, lst_dicts_nodes_pages))
    logging.info(f"Node: {node}, Pages: {list_node[0]['pages']}")

    if len(list_node) > 0:
        execution_date = kwargs['execution_date']
        str_prefix = f"brewery/sys_file_date={execution_date.strftime('%Y-%m-%d')}"
        for page in lst_dicts_nodes_pages[0]['pages']:
            logging.info(f"Searching page: {page}")
            response = FetchApiOperator(
                task_id=f'task_fetch_breweries_page_{page}',
                url=f'{STATIC_BASE_URL}/v1/breweries?page={page}&per_page=50',
                xcom_key=f'breweries_page_{page}',
                max_retries=5,
                wait_time=5,
                type_request='GET'
            ).execute(context=kwargs)
            if is_valid_json(response):
                file_key = f"{str_prefix}/node_{node}_page_{page}.json"
                logging.info(f"Saving page {page} from node {node} to Minio with key: {file_key}")
                save_json_to_minio(response, MINIO_BUCKET_NAME, file_key, 'minio:9000', MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
            else:
                raise AirflowException(f"Invalid JSON response from page {page}")


default_args = {
    'owner': 'herculanocm',
    'email': ['herculanocm@outlook.com'],
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'email_on_success': False,
    'on_failure_callback': None,
    'on_success_callback': None,
    'retries': 2,
}

with DAG(
        dag_id='datalake_bronze_fetch_raw_data_api_dag',
        schedule_interval=None,
        start_date=datetime(2024, 6, 18),
        default_args=default_args,
        catchup=False,
        params={"custom_param": "default_value"}, 
        tags=['datalake', 'pipe', 'raw', 'api', 'bronze'],
) as dag:
    
    task_init_seq_01 = EmptyOperator(
        task_id='task_init_seq_01'
    )

    task_fetch_breweries_meta = FetchApiOperator(
        task_id='task_fetch_breweries_meta',
        url=f'{STATIC_BASE_URL}/v1/breweries/meta',
        xcom_key='breweries_meta_data',
        max_retries=5,
        wait_time=5,
        type_request='GET'
    )

    task_check_and_clean_breweries_meta = BranchPythonOperator(
        task_id='task_check_and_clean_breweries_meta',
        python_callable=clean_breweries_meta,
        provide_context=True
    )

    task_seq_02 = EmptyOperator(
        task_id='task_seq_02'
    )

    for node in range(INT_NODES):
        LST_TASKS_NODES.append( 
            PythonOperator(
                task_id=f'task_fech_breweries_node_{node}',
                python_callable=fech_breweries_node,
                provide_context=True,
                op_kwargs={'node': node},
            )
        )
        
    task_end_seq_01 = EmptyOperator(
        task_id='task_end_seq_01',
        trigger_rule=TriggerRule.NONE_FAILED
    )

task_init_seq_01 >> task_fetch_breweries_meta >> task_check_and_clean_breweries_meta >> task_seq_02 >> [tt for tt in LST_TASKS_NODES] >> task_end_seq_01
task_check_and_clean_breweries_meta >> task_end_seq_01