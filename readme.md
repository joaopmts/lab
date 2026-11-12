# Data Engineering Lab

A complete lab environment for **Data Engineering** experiments and projects.
It includes orchestration services, distributed processing, data storage, a shared table catalog, and graphical interfaces for managing workflows and data pipelines.

---

## Media Folder

The **`docs`** folder contains visual documentation materials for the project, such as images, diagrams, and screenshots.

- [docs/images](https://github.com/joaopmts/lab/tree/main/docs/images) → screenshots from running applications

> Note: Files in this folder are strictly for documentation purposes.
> Do **not** store datasets or execution results here.
> Some screenshots (HDFS / Hue) are from an earlier version of the lab, before those services were removed.

## Prerequisites

- **Git** → [Install Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
- **Docker**
  - [Docker Desktop (Windows)](https://docs.docker.com/desktop/setup/install/windows-install/)
  - [Docker Engine (Linux)](https://docs.docker.com/engine/install/)
- At least **8 GB of RAM** assigned to Docker (all services together)

---

## Quick Start

```bash
git clone https://github.com/joaopmts/lab.git
cd lab
docker compose up -d --build
```

That's it. No `.env` is needed: every variable has a lab default (see [Credentials](#credentials)). The first build takes a while because it downloads Spark and the jars.

On the first start, everything is set up by itself:

| What | Who does it |
|---|---|
| `airflow_db`, `metastore_db` and the `hive` user | [postgres/init/01-create-databases.sh](postgres/init/01-create-databases.sh) |
| Buckets `bronze`, `silver`, `temp`, `warehouse`, `teste` + a sample file | `minio-init` |
| Hive Metastore schema, warehouse on `s3a://warehouse/` | `metastore` |
| Airflow DB, admin user, `spark_default` and `trino_default` connections | `airflow-init` + compose |
| NiFi flow `lab_example` imported and configured (processors stopped) | `nifi-init` ([nifi/init/import_flows.py](nifi/init/import_flows.py)) |

**Try the integration example:** open Airflow at http://localhost:8084 (`airflow` / `airflow`), unpause and trigger `lab_end_to_end`, then open the notebook `Exemplos/lab_integracao.ipynb` in JupyterLab at http://localhost:8891. More details are in [Integration Example](#integration-example-every-service-talking).

> The defaults are **public**. That is fine on your own machine, but do not expose these ports to the internet. To change passwords and keys, run `copy .env.example .env` (Linux/macOS: `cp`) and edit the file before the first start.

### Useful commands

```powershell
docker compose ps                     # status
docker compose logs -f nifi-init      # e.g. watch the NiFi flow import
docker compose stop                   # stop (keeps data)
docker compose down -v                # remove containers AND volumes (Kafka, NiFi)
```

Data that stays on your disk (not versioned): `postgres/postgres-db-volume/`, `minio/data/`, `airflow/logs/`, `airflow/data/`. To start from zero, run `docker compose down -v` and delete those folders.

**WARNING:** `docker system prune -a --volumes` erases **all** Docker images and volumes on your machine, not only the lab's.

<details>
<summary>Upgrading an installation created before the init scripts</summary>

The databases already exist, so nothing needs to be done. If you ever need to create them by hand:

```powershell
docker compose up postgres -d
docker exec -it postgres psql -U admin -d postgres -c "CREATE DATABASE airflow_db;"
docker exec -it postgres psql -U admin -d postgres -c "CREATE USER hive WITH PASSWORD 'password';"
docker exec -it postgres psql -U admin -d postgres -c "CREATE DATABASE metastore_db OWNER hive;"
```

If the Hive `default` database was created when the warehouse was still local (`/opt/hive/data/warehouse`), point it to MinIO once:

```powershell
docker exec -it spark-master spark-sql -e "ALTER DATABASE default SET LOCATION 's3a://warehouse/'"
```
</details>

<details>
<summary>Start only part of the lab (less RAM)</summary>

`docker compose up` with service names also starts their dependencies:

```powershell
docker compose up -d trino spark-master spark-worker     # lakehouse: + metastore, postgres, minio
docker compose up -d nifi kafka-ui                       # streaming: + broker
docker compose up -d airflow-apiserver airflow-scheduler airflow-dag-processor airflow-worker airflow-triggerer
docker compose stop nifi kafka-ui                        # stop what you are not using
```
</details>

### Airflow connections

`spark_default` and `trino_default` are defined by `docker-compose.yml`. The example DAGs need nothing else. Create the other connections (`reddit_default`, `spotify_default`, `databricks_default`) in **Admin → Connections**, following the template in [airflow/conf/Airflow Connections.txt](airflow/conf/Airflow%20Connections.txt).

### NiFi flows

NiFi keeps its configuration (`nifi.properties`, users, certificates, current canvas) in the named volume `nifi-conf`. On first use, Docker fills it with the image defaults and NiFi generates its own self-signed certificate. Nothing in it is versioned.

- **Version a flow:** right-click the process group → *Download flow definition* → save it to `nifi/flows/`.
- **Auto-import:** `nifi-init` imports the flows listed in `NIFI_IMPORT_FLOWS` (compose) on the first start. It is idempotent.
- **Restore a flow by hand:** drag a *Process Group* onto the canvas → *Upload* (browse icon) → choose the JSON file from `nifi/flows/`.
- Sensitive properties (passwords, tokens) are **not** included in exported flows. Fill them in again after importing.
- `NIFI_SENSITIVE_PROPS_KEY` encrypts those values inside the volume. Do not change it on an existing installation, or the flow can no longer be decrypted.

---

## Credentials

Lab defaults (used when there is no `.env`). Change them by creating a `.env` from `.env.example`.

| Service | User | Password |
|---|---|---|
| Airflow | `_AIRFLOW_WWW_USER_USERNAME` (airflow) | `_AIRFLOW_WWW_USER_PASSWORD` (airflow) |
| MinIO | `MINIO_ROOT_USER` (admin) | `MINIO_ROOT_PASSWORD` (minioadmin) |
| NiFi | `NIFI_USERNAME` (nifi) | `NIFI_PASSWORD` |
| Kafka UI | `KAFKA_UI_USERNAME` (admin) | `KAFKA_UI_PASSWORD` |
| Postgres | `POSTGRES_USER` (admin) | `POSTGRES_PASSWORD` (admin) |
| JupyterLab | — | no token (bound to `127.0.0.1` only) |

---

<h2>Services Tree</h2>

<pre>
Data Engineering Lab
├── Airflow 3 (CeleryExecutor)
│   ├── apiserver (8084) → <a href="http://localhost:8084">http://localhost:8084</a>
│   ├── scheduler / dag-processor / triggerer / worker
│   ├── cli     (profile: debug)
│   └── flower  (5555, profile: flower) → <a href="http://localhost:5555">http://localhost:5555</a>
│
├── Storage
│   ├── MinIO (9000 API, 9001 console) → <a href="http://localhost:9001">http://localhost:9001</a>
│   ├── minio-init (creates the buckets and exits)
│   ├── Postgres (5442 → 5432)
│   └── Redis (6379, internal)
│
├── Catalog / SQL
│   ├── Hive Metastore (9083) → thrift://metastore:9083
│   └── Trino (8085) → <a href="http://localhost:8085">http://localhost:8085</a>
│
├── Spark 3.5.4
│   ├── Master
│   │   ├── Cluster (7077) → spark://spark-master:7077
│   │   ├── Web UI (8180) → <a href="http://localhost:8180">http://localhost:8180</a>
│   │   └── JupyterLab (8891) → <a href="http://localhost:8891">http://localhost:8891</a>
│   └── Worker UI (8881) → <a href="http://localhost:8881">http://localhost:8881</a>
│
├── Kafka (9092 from the host, broker:19092 inside the network)
│   └── Kafka UI (3042) → <a href="http://localhost:3042">http://localhost:3042</a>
│
└── NiFi (8443) → <a href="https://localhost:8443/nifi">https://localhost:8443/nifi</a>
</pre>

---

## Stack

| Service | Version | Role |
|---|---|---|
| **Apache Kafka** | `4.1.0` (KRaft, no Zookeeper) | Streaming broker |
| **Kafka UI** | `kafbat/kafka-ui:v1.3.0` | Kafka management UI |
| **Apache NiFi** | `2.5.0` | Data ingestion and routing |
| **MinIO** | `joaopmts99/minio` | S3-compatible object storage |
| **Apache Spark** | `3.5.4` | Distributed processing + JupyterLab |
| **Apache Airflow** | `3.0.4` (CeleryExecutor) | Pipeline orchestration |
| **Trino** | `469` | Interactive SQL query engine |
| **Apache Hive Metastore** | `4.0.0` | Shared table catalog (Spark + Trino) |
| **PostgreSQL** | `13` | Airflow metadata + Hive Metastore DB |
| **Redis** | `7.2` | Celery message broker |

---

## Spark: Libraries and Catalogs

**Table formats:** Delta Lake 3.2.0, Apache Iceberg 1.6.1

**Kafka connector:** `spark-sql-kafka-0-10` (Structured Streaming)

**Python / JupyterLab:** PySpark 3.5.4, delta-spark 3.2.0, pandas, scikit-learn, plotly, seaborn, spotipy, nltk, wordcloud

The catalogs are defined in [spark/conf/spark-defaults.conf](spark/conf/spark-defaults.conf). The same file is used by the cluster and by the Spark driver inside Airflow.

| Spark catalog | Format | Metastore | Visible in Trino as |
|---|---|---|---|
| `spark_catalog` (**default**) | Delta Lake / Parquet | Hive Metastore, warehouse `s3a://warehouse/` | `delta`, `hive` |
| `iceberg` | Iceberg | Hive Metastore, warehouse `s3a://warehouse/iceberg` | `iceberg` |
| `local` (legacy) | Iceberg (hadoop catalog) | none, files only at `s3a://bronze/warehouse` | — |

```python
spark.sql("CREATE DATABASE IF NOT EXISTS lab")
df.write.format("delta").saveAsTable("lab.my_delta_table")     # → Trino: delta.lab.my_delta_table
df.writeTo("iceberg.lab.my_iceberg_table").createOrReplace()    # → Trino: iceberg.lab.my_iceberg_table
```

The S3A credentials come from the `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` environment variables. They are filled from `S3A_ACCESS_KEY` / `S3A_SECRET_KEY` in `.env`, so there are no keys in the code or the notebooks.

---

## Airflow: Providers, Plugins and DAGs

| Package | Use |
|---|---|
| `apache-airflow-providers-apache-spark` | Submit Spark jobs |
| `apache-airflow-providers-databricks` | Databricks integration |
| `apache-airflow-providers-trino` | SQL on Trino (`trino_default`, defined in the compose file) |
| `confluent-kafka` | Kafka producers/consumers in DAGs |
| `praw` | Reddit API (via custom hook/operator) |
| `spotipy` | Spotify API (via custom hook) |

The Airflow image ships the Spark 3.5.4 client together with `hadoop-aws`, Delta, Iceberg and the Kafka jars in `$SPARK_HOME/jars`. It also includes the same `core-site.xml` and `spark-defaults.conf` as the cluster. Because of this, `SparkSubmitOperator` does not need `conf`, `jars` or `driver_class_path`.

**DAGs**

| DAG | Schedule | What it does |
|---|---|---|
| `reddit_bronze_s3a_spark_sso` | hourly | Reddit search → `s3a://bronze/reddit/day/json/<YYYYMMDDHH>` (spark-submit). Publishes an Asset |
| `reddit_silver_s3a_sso` | Asset (after bronze) | Bronze → `s3a://silver/reddit/json/{users,content}`, deduplicated by `id` |
| `reddit_bronze_s3a` / `reddit_silver_s3a` | hourly / Asset | Same pipeline using a PythonOperator + RedditOperator (paths `reddit_pyop`) |
| `reddit_one_file` | hourly | Reddit search → local CSV in `airflow/data/reddit_project` |
| `spotify_api_dag` | manual | Kaggle Spotify dataset → cleaning → release-year enrichment via the API → silver |
| `climate_data` | weekly | Open-Meteo → CSVs in `airflow/data/climate_data_project` |
| `coingeko-airflow-databricks-azure` | daily | Runs Databricks jobs (**paid**) |
| `lab_end_to_end` | manual | **Integration example** across every service (see below) |
| `test_kafka_dag`, `s3a_consult_test` | manual | Connectivity tests |

Secrets (Reddit/Spotify) are read from the connections **at run time** through Jinja and handed to the Spark driver as `env_vars`. They never appear in `application_args`, logs, or the Spark UI.

**DAG check**: catches import errors, missing tags, `catchup=True`, and templates that use `data_interval_*` / `ds` / `logical_date` directly. Those values are undefined in manual runs on Airflow 3.

```powershell
docker compose exec airflow-scheduler python /opt/airflow/tests/check_dags.py
```

**Code structure**

```
airflow/
├── dags/
│   ├── *.py                     # DAG definitions
│   └── scripts/
│       ├── lab_utils.py         # Reddit schema, generic read/write, dedup
│       ├── reddit_*.py          # Spark jobs / callables for the Reddit pipeline
│       └── spotify_*.py         # Spark jobs for the Spotify pipeline
└── plugins/
    ├── hook/{reddit,spark,spotify}_hook.py
    └── operators/reddit_operator.py
```

---

## Integration Example (every service talking)

Shows each service of the lab exchanging data with the others:

```
NiFi (flow lab_example) ─┬─> MinIO  s3a://bronze/nifi/events/   (PutS3Object)
                         └─> Kafka  lab_events                  (PublishKafka)
Airflow (produce_events) ──> Kafka  lab_events
Airflow → Spark (spark_kafka_to_lakehouse):
     Kafka lab_events ─> Iceberg iceberg.lab.events ─> Delta lab.events_summary ─> Kafka lab_events_summary
                          (Hive Metastore + MinIO)
Airflow → Trino: checks the tables, creates iceberg.lab.events_by_source (CTAS), reads kafka.default.lab_events_summary
Airflow ← Kafka: consume_summary reads the summary back
Jupyter → Spark: notebooks/Exemplos/lab_integracao.ipynb reads all of the above, including the table created by Trino
```

**How to run it**

1. *(Optional)* NiFi (https://localhost:8443/nifi): start the `lab_example` process group. It generates one event every 5 s.
   `nifi-init` already imported and configured it; it ships **stopped** so it does not fill MinIO/Kafka.
2. Airflow (http://localhost:8084): unpause and trigger `lab_end_to_end`. It works even with NiFi stopped, because it produces its own events.
3. JupyterLab (http://localhost:8891): open `Exemplos/lab_integracao.ipynb`.
4. Kafka UI (http://localhost:3042): watch the `lab_events` and `lab_events_summary` topics.

---

## Trino: Catalogs

| Catalog | Connector | Source |
|---|---|---|
| `hive` | hive | Hive Metastore + MinIO |
| `iceberg` | iceberg | Hive Metastore + MinIO (same as the Spark `iceberg` catalog) |
| `delta` | delta_lake | Hive Metastore + MinIO (same as the Spark `spark_catalog`) |
| `postgresql` | postgresql | `postgres:5432/airflow_db` (Airflow metadata) |
| `kafka` | kafka | Kafka broker `broker:19092`. Only the topics listed in `kafka.table-names` (e.g. `kafka.default.vendas`) |

```bash
docker exec -it trino trino
```

```sql
SHOW CATALOGS;
SHOW SCHEMAS FROM iceberg;
SELECT dag_id, is_paused FROM postgresql.public.dag LIMIT 10;
```

---

## Project File Structure

```
lab/
├── docker-compose.yml
├── .env.example                  # Template of the .env (credentials and secrets)
├── airflow/
│   ├── conf/
│   │   ├── Dockerfile            # Airflow 3.0.4 + Java 17 + Spark 3.5.4 client + jars
│   │   ├── requirements.txt
│   │   └── Airflow Connections.txt   # Connections template (no secrets)
│   ├── config/airflow.cfg        # Minimal; the compose file is the source of truth
│   ├── dags/
│   ├── plugins/
│   └── tests/check_dags.py       # DAG check (see above)
├── spark/
│   ├── conf/
│   │   ├── Dockerfile            # Bitnami Spark + JupyterLab + Delta + Iceberg + Kafka
│   │   ├── core-site.xml         # S3A / MinIO (shared with Airflow and Hive)
│   │   ├── spark-defaults.conf   # Catalogs (shared with Airflow)
│   │   └── custom/startup.py
│   └── jupyter_home/notebooks/   # Mounted in JupyterLab
├── hive/conf/hive-site.xml       # Metastore warehouse on MinIO
├── trino/conf/                   # Single-node coordinator + catalogs
├── postgres/init/                # Init script (databases + hive user)
├── nifi/
│   ├── flows/                    # Exported flow definitions (lab_example.json, Teste.json)
│   └── init/import_flows.py      # Imports the flows on the first start (nifi-init)
├── assets/postgresql-42.6.0.jar  # JDBC driver for the Hive Metastore
└── docs/images/

# Generated at runtime and not versioned:
# postgres/postgres-db-volume/, minio/, airflow/logs/, airflow/data/
# Docker volumes: kafka-data, nifi-conf (+ NiFi repositories)
```
