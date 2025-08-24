# Data Engineering Lab

A complete lab environment for **Data Engineering** experiments and projects.  
It includes orchestration services, distributed processing, data storage, and graphical interfaces for managing workflows and data pipelines.  

---

## `docs` Folder  

The **`docs`** folder contains visual documentation materials for the project, such as images, diagrams, and screenshots.  
These files are used to enrich the explanations in the `README.md`.  

### Suggested structure  
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
net start com.docker.service
docker system prune -a --volumes
```

**WARNING**: This will erase **all Docker images and volumes** from your machine.  

---

## Postgres Setup

```bash
docker compose up postgres
docker exec -it postgres psql -U admin -d postgres -c "CREATE DATABASE airflow_db;"
docker exec -it postgres psql -U admin -d postgres -c "CREATE DATABASE metastore_db;"
docker exec -it postgres psql -U admin -d postgres -c "CREATE DATABASE hue_db;"
docker exec -it postgres psql -U admin -d postgres -c "CREATE USER hive WITH PASSWORD 'password';"
docker exec -it postgres psql -U admin -d postgres -c "GRANT ALL PRIVILEGES ON DATABASE metastore_db TO hive;"
```

---

## Run All Services

```bash
docker compose up -d --build
```

---

## HDFS Setup

```bash
docker exec -it namenode bash
hdfs dfs -mkdir -p /user/admin
hdfs dfs -chown admin:admin /user/admin
hdfs dfs -ls /user
```

---

## Credentials

- **Airflow**  
  - User: `airflow`  
  - Password: `airflow`  

- **MinIO**  
  - User: `admin`  
  - Password: `minioadmin`  
  - Access Key: `eDFFCzvdNdoeLlJkZhXI`                      * not setup in clean installation
  - Secret Key: `gU3PQYFs6K60DvP5ut52zMQJAU3M3rW4XtCY9lqb`  * not setup in clean installation

- **Hue**  
  - User: `admin`       
  - Password: `admin`    

---

## Services Tree (with Auth)

```
Data Engineering Lab
├── Airflow
│   ├── apiserver (8084) → [http://localhost:8084](http://localhost:8084)
│   ├── scheduler
│   ├── dag-processor
│   ├── worker
│   ├── triggerer
│   ├── cli
│   └── flower
│
├── Storage
│   ├── MinIO (9000, 9001) → [http://localhost:9001](http://localhost:9001)
│   ├── Postgres (5442) 
│   │   ├── airflow_db
│   │   ├── metastore_db
│   │   └── hue_db
│   └── Redis (6379) 
│
├── Hadoop
│   ├── NameNode (9870) → [http://localhost:9870](http://localhost:9870)
│   └── DataNode (9864) → [http://localhost:9864](http://localhost:9864)
│
├── Hive
│   ├── Metastore (9083)
│   └── HiveServer2 (10000, 10002) → [http://localhost:10002](http://localhost:10002)
│
├── Spark
│   ├── Master
│   │   ├── Spark UI (8180) → [http://localhost:8180](http://localhost:8180)
│   │   ├── Cluster (7077) → spark://localhost:7077
│   │   └── Jupyter Notebook (8890 / 8888) → [http://localhost:8890](http://localhost:8890) ou [http://localhost:8888](http://localhost:8888)
│   └── Worker (8881) → [http://localhost:8881](http://localhost:8881)
│
└── Hue (8888) → [http://localhost:8888](http://localhost:8888)

```

---

## Project File Structure

```
Project Root
├── airflow
│   ├── dags 
│   │   ├── scripts
│   │   └── __pycache__
│   ├── data
│   │   ├── climate_data_project (Climate Project)
│   │   └── reddit_project (Reddit Project)
│   ├── plugins
│   │   ├── hook
│   │   └── operators
│   └── conf
│       └── airflowconnections.txt   (Airflow Connections list)
│   
│
│
└── spark
    └── notebooks
        └── Data-Engineering (MBA Files)
            └── Distributed-Data-Processing-and-Storage
```

## Notes

- All services are connected to the `spark-net` network.  
- Check the `docker-compose.yml` file for additional volumes and configuration customization.  
