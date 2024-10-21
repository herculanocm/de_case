from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, DoubleType, DateType, TimestampType, IntegerType, BooleanType, LongType
import logging
import datetime

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
            id STRING,
            name STRING,
            brewery_type STRING,
            address_1 STRING,
            address_2 STRING,
            address_3 STRING,
            city STRING,
            state_province STRING, 
            postal_code STRING,
            country STRING,
            longitude FLOAT,
            latitude FLOAT,
            phone BIGINT,
            website_url STRING,
            state STRING,
            street STRING
        )
        USING iceberg
        PARTITIONED BY (
            brewery_type,
            country,
            city,
            state_province
        )
        LOCATION 's3a://datalake-gold/warehouse/dbw/tab_brewery/'

    """)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.info('Running job_silver_app...')
    spark_session = get_spark_session()
    str_datetime_ref = spark_session.conf.get("spark.job_silver_app.datetime_ref", '1900-01-01 00:00:00')
    str_bucket_name = spark_session.conf.get("spark.job_silver_app.bucket_name", 'undefined')
    str_dataset_name = spark_session.conf.get("spark.job_silver_app.dataset_name", 'undefined')
    logger.info(f'Str datetime reference: {str_datetime_ref}, bucket name: {str_bucket_name}, dataset name: {str_dataset_name}')
    datetime_ref = datetime.datetime.strptime(str_datetime_ref, '%Y-%m-%d_%H:%M:%S')
    str_prefix_bucket = f"s3a://{str_bucket_name}/{str_dataset_name}/sys_file_date={datetime_ref.strftime('%Y-%m-%d')}/"
    logger.info(f'Str prefix bucket: {str_prefix_bucket}')

    
    logger.info(f"Path {str_prefix_bucket} exists.")
    df = (
        spark_session.read
            .option("multiline", "true")
            .option('inferSchema', 'true')
            .json(str_prefix_bucket)
    )
    df.printSchema()

    int_qtd_lines = df.count()
    logger.info(f"Number of lines: {int_qtd_lines}")
    if int_qtd_lines > 0:
        check_database_table_exists(spark_session)

        describe_result = spark_session.sql("DESCRIBE TABLE nessie.dbw.tab_brewery").collect()
        describe_list = [row.asDict() for row in describe_result]
        # Log the result
        logger.info(f"Describe result: {describe_list}")

        df = cast_columns_types_by_schema(df, describe_list, logger)
        df.printSchema()
        df.show(5)
