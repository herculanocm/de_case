### Create an external network (Globally recognization for containers)
```
docker network create decase
```

### Setting the right Airflow user
```
mkdir -p ./airflow/dags ./airflow/logs ./airflow/plugins ./airflow/config
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

### Build custom Airflow image
```
cd ./airflow
docker build -t decase/airflow:2.9.3 ./airflow/
```

### Build custom Spark image
```
cd ./spark
docker build -t decase/spark:3.4.1 .
```

### Initialize database for Airflow use
```
docker compose up airflow-init
```

### Local development airflow environment
```
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt-get update
sudo apt-get install python3.12 python3.12-distutils python3.12-venv python3.12-dev
```
Venv
```
python3.12 -m venv ./airflow/venv
```
Airflow 
```
pip install "apache-airflow==2.9.2" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.2/constraints-3.12.txt"
```

### Nessie 
Go to your MINIO BUCKET and SET policy to public
Add new source on Dremio, chosse "Nessie Catalog"
Set properties:
    General:
        - Name: nessie
        - Nessie endpoint URL: http://nessie:19120/api/v2
        - Auth: None for authentication
    Storage:
        - AWS root path: datalake-gold/warehouse
        - S3 auth:
            - key: admin
            - Pas: password
        - Connection properties:
            - fs.s3a.path.style.acces=true
            - fs.s3a.endpoint=minio:9000
            - dremio.s3.compat=true
        - Uncheck enrypt connection
Save the new source, now we have a Nessie catalog, let's create a table (important select a context nessie)



CREATE TABLE tab_brewery (
    id VARCHAR,
    name VARCHAR,
    brewery_type VARCHAR,
    address_1 VARCHAR,
    address_2 VARCHAR,
    address_3 VARCHAR,
    city VARCHAR,
    state_province VARCHAR, 
    postal_code VARCHAR,
    country VARCHAR,
    longitude FLOAT,
    latitude FLOAT,
    phone BIGINT,
    website_url VARCHAR,
    state VARCHAR,
    street VARCHAR
) PARTITION BY (
    brewery_type,
    country,
    city,
    state_province
)
