#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE salesdb;
    GRANT ALL PRIVILEGES ON DATABASE salesdb TO $POSTGRES_USER;
EOSQL
