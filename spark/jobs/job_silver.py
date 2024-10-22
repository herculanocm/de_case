from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, DoubleType, DateType, TimestampType, IntegerType, BooleanType, LongType
import logging
import datetime
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import json

def is_valid_json(value: str) -> bool:
    """
    Checks if string is a valid JSON.

    Parameters:
    - value (str): The string to check.

    Returns:
    - bool: True if the string is valid JSON, False otherwise.
    """
    try:
        json.loads(value)
        return True
    except (ValueError, TypeError):
        return False

def check_minio_prefix_exists(endpoint_url: str, access_key: str, secret_key: str, bucket_name: str, prefix: str, logger: logging.Logger = None) -> bool:
    """
    Checks if a given prefix exists in a MinIO bucket by verifying the presence of objects under the prefix.

    Parameters:
    - endpoint_url (str): The endpoint URL for MinIO (e.g., 'http://localhost:9000').
    - access_key (str): Access key for MinIO.
    - secret_key (str): Secret key for MinIO.
    - bucket_name (str): The bucket to check.
    - prefix (str): The prefix to check (e.g., 'warehouse/tab_brewery/').
    - logger (logging.Logger, optional): Logger for logging information and errors.

    Returns:
    - bool: True if the prefix exists (has at least one object), False otherwise.
    """
    try:
        # Initialize S3 client with MinIO configurations
        s3 = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=boto3.session.Config(signature_version='s3v4'),
            verify=False  # Set to True if SSL is enabled
        )
        
        # List objects under the specified prefix with a maximum of 1 key
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, MaxKeys=1)
        
        exists = 'Contents' in response and len(response['Contents']) > 0
        
        if logger:
            if exists:
                logger.info(f"Prefix '{prefix}' exists in bucket '{bucket_name}'.")
            else:
                logger.info(f"Prefix '{prefix}' does not exist or is empty in bucket '{bucket_name}'.")
        
        return exists
    except NoCredentialsError:
        if logger:
            logger.error("MinIO credentials not provided or incorrect.")
        return False
    except ClientError as e:
        if logger:
            logger.error(f"Client error while accessing MinIO: {e}")
        return False
    except Exception as e:
        if logger:
            logger.error(f"Unexpected error while checking prefix: {e}")
        return False

def cast_columns_types_by_schema(df: DataFrame, list_schema: list, logger: logging.Logger) -> DataFrame:
    """
    Casts DataFrame columns to specified data types based on the provided schema.

    Parameters:
    - df (DataFrame): The input Spark DataFrame.
    - list_schema (list): A list of dictionaries with 'col_name' and 'data_type' keys.
    - logger (logging.Logger): Logger for logging information.

    Returns:
    - DataFrame: The transformed DataFrame with columns cast to specified types.
    
    Raises:
    - ValueError: If df or list_schema is None or list_schema is empty.
    """
    if df is None:
        raise ValueError("Input DataFrame 'df' cannot be None.")
    if not list_schema:
        raise ValueError("Input 'list_schema' cannot be None or empty.")
    
    # Create a set of existing columns for faster lookup
    existing_columns = set(df.columns)
    # Create a set of schema-defined columns
    schema_columns = set(col['col_name'] for col in list_schema if '#' not in col['col_name'])
    
    # Columns to add (present in schema but not in DataFrame)
    columns_to_add = schema_columns - existing_columns
    # Columns to drop (present in DataFrame but not in schema)
    columns_to_drop = existing_columns - schema_columns
    
    # Add missing columns with None (null) values
    for col_name in columns_to_add:
        df = df.withColumn(col_name, F.lit(None))
        logger.info(f"Added missing column '{col_name}' with null values.")
    
    # Drop extra columns not present in schema
    if columns_to_drop:
        df = df.drop(*columns_to_drop)
        for col_name in columns_to_drop:
            logger.info(f"Dropped column '{col_name}' as it's not present in the schema.")
    
    # Define type mapping
    type_mapping = {
        'int': IntegerType(),
        'integer': IntegerType(),
        'long': LongType(),
        'bigint': LongType(),
        'bool': BooleanType(),
        'boolean': BooleanType(),
        'double': DoubleType(),
        'float': DoubleType(),  # Using DoubleType for float compatibility
        'decimal': DoubleType(),  # Adjust as needed
        'real': DoubleType(),
        'money': DoubleType(),
        'currency': DoubleType(),
        'datetime': TimestampType(),
        'timestamp': TimestampType(),
        'date': DateType(),
        # Add more mappings as needed
    }
    
    # Iterate over schema and apply casts
    for column in list_schema:
        col_name = column.get('col_name')
        desired_type_str = column.get('data_type', 'string').lower()
        desired_type = type_mapping.get(desired_type_str, StringType())
        
        if col_name not in df.columns:
            logger.warning(f"Column '{col_name}' is missing in DataFrame even after addition.")
            continue  # Skip casting for missing columns
        
        current_type_str = dict(df.dtypes).get(col_name, '').lower()
        
        # Determine if casting is needed
        need_cast = False
        if isinstance(desired_type, IntegerType) and 'int' not in current_type_str:
            need_cast = True
        elif isinstance(desired_type, LongType) and 'int' not in current_type_str:
            need_cast = True
        elif isinstance(desired_type, BooleanType) and 'bool' not in current_type_str:
            need_cast = True
        elif isinstance(desired_type, DoubleType) and current_type_str not in ['double', 'float', 'decimal', 'real', 'money', 'currency']:
            need_cast = True
        elif isinstance(desired_type, DateType) and 'date' not in current_type_str:
            need_cast = True
        elif isinstance(desired_type, TimestampType) and 'timestamp' not in current_type_str and 'datetime' not in current_type_str:
            need_cast = True
        elif isinstance(desired_type, StringType) and current_type_str != 'string':
            need_cast = True
        
        if need_cast:
            try:
                df = df.withColumn(col_name, F.col(col_name).cast(desired_type))
                logger.info(f"Converted column '{col_name}' from '{current_type_str}' to '{desired_type.simpleString()}'.")
            except Exception as e:
                logger.error(f"Failed to cast column '{col_name}' to '{desired_type.simpleString()}': {e}")
        else:
            logger.info(f"No casting needed for column '{col_name}' (current type: '{current_type_str}').")
    
    return df

def get_spark_session() -> SparkSession:
    return SparkSession.builder.appName("job_silver_app") \
        .getOrCreate()

def check_database_table_exists(spark_session: SparkSession):
    spark_session.sql("""
    CREATE NAMESPACE IF NOT EXISTS nessie.dbw
    """)

    spark_session.sql("""DROP TABLE IF EXISTS nessie.dbw.tab_brewery""")
    spark_session.sql("""
        
        CREATE TABLE IF NOT EXISTS nessie.dbw.tab_brewery (
            id STRING COMMENT '{"order_sort": 0, "description": "Unique identifier for the brewery."}',
            name STRING COMMENT '{"order_sort": 1, "description": "Name of the brewery."}',
            brewery_type STRING COMMENT '{"order_sort": 2, "description": "Type of brewery.", "partition": {"enabled": true, "order_sort": 0}}',
            address_1 STRING COMMENT '{"order_sort": 3, "description": "First line of the brewery address."}',
            address_2 STRING COMMENT '{"order_sort": 4, "description": "Second line of the brewery address."}',
            address_3 STRING COMMENT '{"order_sort": 5, "description": "Third line of the brewery address."}',
            city STRING COMMENT '{"order_sort": 6, "description": "City where the brewery is located.", "partition": {"enabled": true, "order_sort": 3}}',
            state_province STRING COMMENT '{"order_sort": 7, "description": "State or province where the brewery is located.", "partition": {"enabled": true, "order_sort": 2}}',
            postal_code STRING COMMENT '{"order_sort": 8, "description": "Postal code of the brewery."}',
            country STRING COMMENT '{"order_sort": 9, "description": "Country where the brewery is located.", "partition": {"enabled": true, "order_sort": 1}}',
            longitude FLOAT COMMENT '{"order_sort": 10, "description": "Longitude of the brewery location."}',
            latitude FLOAT COMMENT '{"order_sort": 11, "description": "Latitude of the brewery location."}',
            phone BIGINT COMMENT '{"order_sort": 12, "description": "Phone number of the brewery."}',
            website_url STRING COMMENT '{"order_sort": 13, "description": "URL of the brewery website."}',
            state STRING COMMENT '{"order_sort": 14, "description": "State of the brewery."}',
            street STRING COMMENT '{"order_sort": 15, "description": "Street of the brewery."}'
        )
        USING iceberg
        PARTITIONED BY (
            brewery_type,
            country,
            state_province,
            city
        )
        LOCATION 's3a://datalake-gold/warehouse/dbw/tab_brewery/'

    """)

def get_columns_partitioned_by_schema(list_schema: list) -> list:
    lst_return = []
    for dcol in list_schema:
        if dcol['comment'].get('partition',{}).get('enabled', False):
            lst_return.append(dcol)

    lst_return.sort(key=lambda x: x['comment']['partition']['order_sort'])
    return [col['col_name'] for col in lst_return]

def add_default_date_columns(list_schema: list) -> list:
    lst_return = list_schema.copy()
    lst_return.append({'col_name': 'year', 'data_type': 'varchar', 'comment': {'order_sort': 10000, 'partition': {'enabled': True, 'order_sort': -3}}})
    lst_return.append({'col_name': 'month', 'data_type': 'varchar', 'comment': {'order_sort': 10001,'partition': {'enabled': True, 'order_sort': -2}}})
    lst_return.append({'col_name': 'day', 'data_type': 'varchar', 'comment': {'order_sort': 10002,'partition': {'enabled': True, 'order_sort': -1}}})
    return lst_return

def adjust_columns_partition(df: DataFrame, lst_partition: list) -> DataFrame:
    for col in lst_partition:
        df = df.withColumn(col, F.upper(F.regexp_replace(F.trim(F.col(col)), r'\s+', '_')))
    return df

def convert_comment_from_json(list_schema: list) -> list:
    for dcol in list_schema:
        if 'comment' in dcol and is_valid_json(dcol['comment']):
            dcol['comment'] = json.loads(dcol['comment'])
        else:
            dcol['comment'] = {
                'partition': {},
                'order_sort': 0,
            }
    return list_schema

def remove_additional_columns(list_schema: list) -> list:
    return [dcol for dcol in list_schema if '#' not in dcol['col_name']]

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.info('Running job_silver_app...')
    spark_session = get_spark_session()
    str_datetime_ref = spark_session.conf.get("spark.job_silver_app.datetime_ref", '1900-01-01 00:00:00')
    str_bucket_name = spark_session.conf.get("spark.job_silver_app.bucket_name", 'undefined')
    str_dataset_name = spark_session.conf.get("spark.job_silver_app.dataset_name", 'undefined')
    str_store_bucket_name = spark_session.conf.get("spark.job_silver_app.store_bucket_name", 'undefined')
    logger.info(f'Str datetime reference: {str_datetime_ref}, bucket name: {str_bucket_name}, dataset name: {str_dataset_name}, store bucket name: {str_store_bucket_name}')
    datetime_ref = datetime.datetime.strptime(str_datetime_ref, '%Y-%m-%d_%H:%M:%S')
    str_prefix_bucket = f"s3a://{str_bucket_name}/{str_dataset_name}/sys_file_date={datetime_ref.strftime('%Y-%m-%d')}/"
    logger.info(f'Str prefix bucket: {str_prefix_bucket}')

    lst_prefix_bucket = str_prefix_bucket.split('/')
    str_bucket_name = lst_prefix_bucket[2]
    str_prefix_path = '/'.join(lst_prefix_bucket[3:])
    logger.info(f'Bucket name: {str_bucket_name}, prefix path: {str_prefix_path}')

    minio_endpoint = spark_session.conf.get("spark.hadoop.fs.s3a.endpoint", 'undefined')
    minio_access_key = spark_session.conf.get("spark.hadoop.fs.s3a.access.key", 'undefined')
    minio_secret_key = spark_session.conf.get("spark.hadoop.fs.s3a.secret.key", 'undefined')
    logger.info(f'MinIO endpoint: {minio_endpoint}, access key: {minio_access_key}')

    if check_minio_prefix_exists(minio_endpoint, minio_access_key, minio_secret_key, str_bucket_name, str_prefix_path, logger):
        logger.info(f"Path {str_prefix_bucket} exists.")
        df = (
            spark_session.read
                .option('inferSchema', 'true')
                .json(str_prefix_bucket)
        )
        df.printSchema()

        int_qtd_lines = df.count()
        logger.info(f"Number of lines: {int_qtd_lines}")
        if int_qtd_lines > 0:

            # Add default date columns to the DataFrame
            df = df.withColumn('year', (F.lit(datetime_ref.strftime('%Y'))))
            df = df.withColumn('month', (F.lit(datetime_ref.strftime('%m'))))
            df = df.withColumn('day', (F.lit(datetime_ref.strftime('%d'))))

            check_database_table_exists(spark_session)

            describe_result = spark_session.sql("DESCRIBE TABLE nessie.dbw.tab_brewery").distinct().collect()
            describe_list = [row.asDict() for row in describe_result]
            describe_list = remove_additional_columns(describe_list)
            describe_list = convert_comment_from_json(describe_list)
            describe_list = add_default_date_columns(describe_list)
            # Log the result
            logger.info(f"Result schema columns: {describe_list}")

            df = cast_columns_types_by_schema(df, describe_list, logger)

            lst_partition = get_columns_partitioned_by_schema(describe_list)
            logger.info(f"Sort column name partition: {lst_partition}")

            # For columns partition TRIM, UPPER AND REPLACE SPACES BETWEEN WORDS
            df = adjust_columns_partition(df, lst_partition)

            describe_list.sort(key=lambda x: x['comment']['order_sort'])
            lst_sorted_columns = [col['col_name'] for col in describe_list]
            logger.info(f"Sorted columns: {lst_sorted_columns}")
            df = df.select(*lst_sorted_columns)

            str_output_path = f"s3a://{str_store_bucket_name}/warehouse/dbw/tab_brewery/"
            logger.info(f"Output path: {str_output_path}")
            df.write.partitionBy(*lst_partition).mode("overwrite").parquet(str_output_path)
            logger.info(f"Data written to {str_output_path} by partitions {lst_partition}.")

            # ONLY FOR CHECKS
            #df = df.drop("year", "month", "day") 
            #df.write.format("iceberg").mode("append").saveAsTable("nessie.dbw.tab_brewery")

        else:
            logger.info("No data to process.")
    else:
        logger.info(f"Path {str_prefix_bucket} does not exist.")

    spark_session.stop()
    logger.info('Job finished.')
