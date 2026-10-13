#!/usr/bin/env bash
# Runs once, on first start of an empty pg-data volume.
set -euo pipefail
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<SQL
CREATE USER hive      PASSWORD '${HIVE_DB_PASSWORD}';
CREATE USER catalogue PASSWORD '${CATALOGUE_DB_PASSWORD}';
CREATE USER airflow   PASSWORD '${AIRFLOW_DB_PASSWORD}';
CREATE DATABASE metastore OWNER hive;
CREATE DATABASE catalogue OWNER catalogue;
CREATE DATABASE airflow   OWNER airflow;
SQL
