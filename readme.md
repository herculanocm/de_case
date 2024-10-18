### Setting the right Airflow user
```
mkdir -p ./airflow/dags ./airflow/logs ./airflow/plugins ./airflow/config
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

### Initialize database for Airflow use
```
docker compose up airflow-init
```