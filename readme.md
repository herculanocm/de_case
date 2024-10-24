# Data Pipeline Setup with Docker

This repository provides a comprehensive guide to setting up a data pipeline using the following tools:

- **[Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/index.html)**
- **[MinIO](https://min.io/)**
- **[Apache Spark](https://spark.apache.org/)**
- **[Nessie](https://projectnessie.org/)**
- **[Dremio](https://www.dremio.com/)**

## Table of Contents

- [Prerequisites](#prerequisites)
- [Setup Instructions](#setup-instructions)
  - [1. Run the Setup Script](#1-run-the-setup-script)
  - [2. Configure MinIO](#2-configure-minio)
  - [3. Initialize the Pipeline in Airflow](#3-initialize-the-pipeline-in-airflow)
  - [4. Configure Dremio](#4-configure-dremio)
- [Pipeline Overview](#pipeline-overview)
  - [DAGs Explanation](#dags-explanation)
- [Access Details](#access-details)
- [Troubleshooting](#troubleshooting)
- [License](#license)

## Prerequisites

Ensure you have the following installed on your machine:

- [Docker](https://www.docker.com/get-started)
- [Docker Compose](https://docs.docker.com/compose/install/)
- [Git](https://git-scm.com/downloads)

## Setup Instructions

### 1. Run the Setup Script

A `run.sh` script is provided to automate the setup process. It performs the following actions:

1. **Set Environment Variable:** Saves the current directory path to the `.env` variable.
2. **Create Directories:** Sets up necessary directories for Airflow (`dags`, `logs`, `plugins`, and `config`).
3. **Create Docker Network:** Establishes a Docker network for inter-container communication.
4. **Build Custom Images:** Builds custom Docker images for Airflow and Spark to include additional packages and configurations.
5. **Initialize Airflow Metadata:** Runs the `airflow-init` image to set up Airflow's metadata and database.
6. **Launch Docker Compose:** Starts all Docker containers as defined in the `docker-compose.yml` file.

To execute the script:

```bash
chmod +x run.sh
./run.sh


### 2. Configure MinIO

MinIO serves as the object storage for the pipeline. Follow these steps to set it up:

1. **Access MinIO Interface:**
   - Open your browser and navigate to [http://localhost:9001/login](http://localhost:9001/login).
   - **Credentials:**
     - **Username:** `admin`
     - **Password:** `password`

2. **Create Buckets:**
   - Navigate to the **Buckets** tab.
   - Create the following three buckets:
     - `datalake-bronze`: Stores JSON files downloaded by Airflow workers.
     - `datalake-silver`: Used by Nessie Metastore and Spark to save raw tables.
     - `datalake-gold`: Used by Nessie Metastore and Spark to save aggregate tables.

3. **Set Access Policies:**
   - For each bucket, go to the **Configuration** tab.
   - Under **Access Policy**, set the policy to **Public**.



### 3. Initialize the Pipeline in Airflow

Airflow orchestrates the data pipeline through Directed Acyclic Graphs (DAGs).

1. **Access Airflow Interface:**
   - Open your browser and navigate to [http://localhost:8080/](http://localhost:8080/).
   - **Credentials:**
     - **Username:** `airflow`
     - **Password:** `airflow`

2. **Enable DAGs:**
   - Enable the following DAGs in sequence:
     1. `1_datalake_bronze_fetch_raw_data_api_dag`
     2. `2_datalake_silver_spark_dag`
     3. `3_datalake_gold_spark_dag`
     4. `0_datalake_pipeline_breweries` *(This DAG is triggered by a schedule)*



### 4. Configure Dremio

Dremio allows you to query the data stored in your metastore.

1. **Access Dremio Interface:**
   - Open your browser and navigate to [http://localhost:9047/](http://localhost:9047/).
   - **Create your own credentials** during the first login.

2. **Configure Nessie Catalog:**
   - **Add New Data Source:**
     - Select **Nessie** as the data source type.
   - **Data Source Configuration:**
     - **Name:** `nessie`
     - **Nessie Endpoint:** `http://nessie:19120/api/v2`
     - **Nessie Authentication Type:** `None`
   - **Storage Configuration:**
     - Navigate to the **Storage** tab.
     - **Storage Provider:** `AWS`
     - **S3 Storage Path:** `datalake-gold/warehouse`
     - **S3 Authentication:** `AWS Access key`
       - **AWS Access Key:** `admin`
       - **AWS Secret Access Key:** `password`
     - **Additional Properties:**
       - `fs.s3a.path.style.access`: `true`
       - `fs.s3a.endpoint`: `minio:9000`
       - `dremio.s3.compat`: `true`
     - **Encryption:** Uncheck the **Encrypt Connection** option.

3. **Access Tables:**
   - After configuration, you can access the following namespaces:
     - **Silver:** Raw tables stored using Iceberg (`nessie.silver.tab_brewery`)
     - **Gold:** Aggregate views using Iceberg (`nessie.gold.tab_brewery_summary`)




## Pipeline Overview

The data pipeline consists of multiple layers, each responsible for different stages of data processing:

- **Bronze Layer:** Ingests raw data from APIs and stores it in MinIO.
- **Silver Layer:** Processes raw data using Spark and stores it in the Nessie Metastore.
- **Gold Layer:** Creates aggregate views from the processed data.

### DAGs Explanation

1. **`0_datalake_pipeline_breweries`**
   - **Responsibility:** Controls the flow of the entire pipeline by triggering other DAGs.
   - **Schedule:** Runs daily at 00:20.

2. **`1_datalake_bronze_fetch_raw_data_api_dag`**
   - **Responsibility:** Fetches data from APIs and stores the raw JSON data in the Bronze layer.

3. **`2_datalake_silver_spark_dag`**
   - **Responsibility:** Loads raw data from the Bronze layer, processes it using Spark, and stores it in the Silver layer.

4. **`3_datalake_gold_spark_dag`**
   - **Responsibility:** Reads data from the Silver layer and creates aggregate views in the Gold layer.


## Access Details

- **Airflow:** [http://localhost:8080/](http://localhost:8080/)
  - **Username:** `airflow`
  - **Password:** `airflow`

- **MinIO:** [http://localhost:9001/login](http://localhost:9001/login)
  - **Username:** `admin`
  - **Password:** `password`

- **Dremio:** [http://localhost:9047/](http://localhost:9047/)
  - **Set up your own credentials upon first access.


## Troubleshooting

- **Docker Issues:**
  - Ensure Docker and Docker Compose are installed and running.
  - Check if the Docker network is created successfully.

- **Service Unavailability:**
  - Verify that all Docker containers are up and running using `docker ps`.
  - Check logs for any container-specific errors using `docker logs <container_name>`.

- **Access Issues:**
  - Ensure that the correct ports are open and not blocked by firewalls.
  - Verify credentials if unable to log in to Airflow or MinIO.

## License

This project is licensed under the [MIT License](LICENSE).

---

*Happy Data Engineering!*
