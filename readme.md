### Setting the right Airflow user
```
mkdir -p ./airflow/dags ./airflow/logs ./airflow/plugins ./airflow/config
echo -e "AIRFLOW_UID=$(id -u)" > .env
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