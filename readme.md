# Data Engineering Lab

A complete lab environment for **Data Engineering** experiments and projects.  
It includes orchestration services, distributed processing, data storage, and graphical interfaces for managing workflows and data pipelines.  

---

## Media Folder  

The **`docs`** folder contains visual documentation materials for the project, such as images, diagrams, and screenshots.  
These files are used to enrich the explanations in the `README.md`.  
 
- [docs/images](https://github.com/joaopmts/lab/tree/main/docs/images) → screenshots from running applications shown in the `README.md`  

> Note: Files in this folder are strictly for documentation purposes.  
> Do **not** store datasets or execution results here.  

## Prerequisites

Before setting up the environment, install:  

- **Git** → [Install Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)  
- **Docker**  
  - [Docker Desktop (Windows)](https://hub.docker.com/editions/community/docker-ce-desktop-windows)  
  - [Docker Engine (Linux)](https://docs.docker.com/install/linux/docker-ce/ubuntu/) 

## Clean Installation

```powershell
net start com.docker.
```
```powershell
docker system prune -a --volumes
```

**WARNING**: This will erase **all Docker images and volumes** from your machine.  

---

## Postgres Setup

```powershell
docker compose up postgres -d
```
```powershell
docker exec -it postgres psql -U admin -d postgres -c "CREATE DATABASE airflow_db;"
docker exec -it postgres psql -U admin -d postgres -c "CREATE DATABASE metastore_db;"
docker exec -it postgres psql -U admin -d postgres -c "CREATE USER hive WITH PASSWORD 'password';"
docker exec -it postgres psql -U admin -d postgres -c "GRANT ALL PRIVILEGES ON DATABASE metastore_db TO hive;"
```

---

## Run All Services 
**WARNING: This command will start all the services on docker-compose.yml**
```powershell
docker compose up -d --build 
```

---

## Credentials and Informations

- **Airflow**  
  - User: `airflow`  
  - Password: `airflow`  

- **MinIO**  
  - User: `admin`  
  - Password: `minioadmin`  

- **Nifi**  
  - User: `nifi`       
  - Password: `nifiadmin1234!@`  

---

<h2>Services Tree (with Auth)</h2>

<pre>
Data Engineering Lab
├── Airflow
│   ├── apiserver (8084) → <a href="http://localhost:8084">http://localhost:8084</a>
│   ├── scheduler
│   ├── dag-processor
│   ├── worker
│   ├── triggerer
│   ├── cli
│   └── flower
│
├── Storage
│   ├── MinIO (9000, 9001) → <a href="http://localhost:9001">http://localhost:9001</a>
│   ├── Postgres (5442) 
│   └── Redis (6379) 
│
├── Hive
│   └── Metastore (9083)
│
├── Spark
│   ├── Master
│   │   ├── Cluster (7077) → spark://spark-master:7077
│   │   └── Jupyter Notebook (8891) → <a href="http://localhost:8891">http://localhost:8891</a>
│   └── Worker (8881) → <a href="http://localhost:8881">http://localhost:8881</a>
│
├── Kafka (9092)
│   ├─ Broker (19092) → broker:19092
│   └─ UI (3042) → <a href="http://localhost:3042">http://localhost:3042</a>
│
└── Nifi (8443) → <a href="https://localhost:8443">https://localhost:8443</a>
</pre>

---

## Stack

| Service | Version | Role |
|---|---|---|
| **Apache Kafka** | `4.1.0` (KRaft, no Zookeeper) | Streaming broker |
| **Kafka UI** | `provectuslabs/kafka-ui` | Kafka management UI |
| **Apache NiFi** | `2.5.0` | Data ingestion and routing |
| **MinIO** | custom image | S3-compatible object storage |
| **Apache Spark** | `3.5.4` | Distributed processing + JupyterLab |
| **Apache Airflow** | `3.0.4` (CeleryExecutor) | Pipeline orchestration |
| **Apache Hive Metastore** | `4.0.0` | Table catalog |
| **PostgreSQL** | `13` | Airflow metadata + Hive Metastore DB |
| **Redis** | `7.2` | Celery message broker |

---

## Spark — Libraries and Table Formats

**Table formats:** Delta Lake 3.2.0, Apache Iceberg 1.6.1

**Kafka connector:** `spark-sql-kafka-0-10` (Spark Structured Streaming)

**Python / JupyterLab:** PySpark, pandas, scikit-learn, delta-spark, plotly, seaborn, spotipy, nltk, wordcloud

**Default catalogs:**
- `spark_catalog` → Delta Lake
- `local` → Iceberg (warehouse at `s3a://bronze/warehouse`)

---

## Airflow — Providers and Plugins

**Installed providers:**

| Provider | Use |
|---|---|
| `apache-airflow-providers-apache-spark` | Submit Spark jobs |
| `apache-airflow-providers-databricks` | Databricks integration |
| `confluent-kafka` | Kafka producers/consumers in DAGs |
| `praw` | Reddit API (via custom hook/operator) |
| `spotipy` | Spotify API (via custom hook) |

**Custom plugins:**

```
airflow/plugins/
├── hook/
│   ├── reddit_hook.py
│   ├── spark_hook.py
│   └── spotify_hook.py
└── operators/
    └── reddit_operator.py
```

---

## Project File Structure

```
lab/
├── docker-compose.yml
├── .env                          # Airflow UID + S3/MinIO credentials
├── airflow/
│   ├── conf/
│   │   ├── Dockerfile            # Airflow 3.0.4 + Java 17 + Spark 3.5.4 + AWS JARs
│   │   └── requirements.txt
│   ├── config/airflow.cfg
│   ├── dags/                     # DAG definitions
│   └── plugins/                  # Custom hooks and operators
├── spark/
│   └── conf/
│       ├── Dockerfile            # Bitnami Spark + JupyterLab + Delta + Iceberg + Kafka JARs
│       ├── core-site.xml         # S3A / MinIO configuration
│       └── custom/startup.py
├── kafka/
│   └── data/                     # Kafka persistent volumes
├── nifi/
│   └── conf/                     # NiFi configuration
├── minio/
│   └── data/                     # MinIO buckets data
├── postgres/                     # PostgreSQL data volume
├── assets/
│   └── postgresql-42.6.0.jar     # JDBC driver for Hive Metastore
└── docs/
    └── images/                   # DAG and results screenshots
```