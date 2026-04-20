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
  - [Databases](https://drive.google.com/drive/folders/1irErR_dXx2XzpMRyNKWbqAF7NNMKDAkh?usp=drive_link)
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

## Project File Structure