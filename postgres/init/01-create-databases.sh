#!/bin/bash
# Executado automaticamente pelo container postgres apenas na PRIMEIRA inicialização
# (quando postgres/postgres-db-volume está vazio). Em instalações existentes não roda.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE airflow_db;
    CREATE USER hive WITH PASSWORD '${HIVE_DB_PASSWORD:-password}';
    CREATE DATABASE metastore_db OWNER hive;
    GRANT ALL PRIVILEGES ON DATABASE metastore_db TO hive;
EOSQL
