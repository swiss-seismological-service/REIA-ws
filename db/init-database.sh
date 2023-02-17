#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --user "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER $APP_USER with encrypted password '$APP_PASS';
    CREATE DATABASE $APP_DB;
    GRANT ALL PRIVILEGES ON DATABASE $APP_DB TO $APP_USER;
    \c erm_db;
    GRANT ALL ON SCHEMA public TO $APP_USER;
    CREATE EXTENSION IF NOT EXISTS POSTGIS;
EOSQL

psql -d $APP_DB -U $APP_USER -f /etc/postgresql/municipalities.sql