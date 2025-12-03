#!/bin/bash

# Fail fast on any error
set -e

# Database name for the kids disappearance dataset
DB_NAME="sports_store"

echo "📌 Creating database and schemas for $DB_NAME ..."

# Check if database exists; if not, create it
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    -tc "SELECT 1 FROM pg_database WHERE datname = '${DB_NAME}'" | grep -q 1 \
    || psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" \
        -c "CREATE DATABASE ${DB_NAME};"

# Connect to the new database and set everything up
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$DB_NAME" <<-EOSQL

    -- =====================================
    -- SCHEMAS
    -- =====================================
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE SCHEMA IF NOT EXISTS processed;

    -- =====================================
    -- ROLE: data_analyst
    -- =====================================
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'data_analyst') THEN
            CREATE ROLE data_analyst LOGIN PASSWORD 'analyst123';
        END IF;
    END \$\$;

    -- =====================================
    -- GRANTS FOR data_analyst
    -- =====================================

    -- Allow the analyst to connect to the database
    GRANT CONNECT ON DATABASE sports_store TO data_analyst;

    -- Allow the analyst to access the schema
    GRANT USAGE ON SCHEMA processed TO data_analyst;

    -- Allow SELECT on all current tables in processed
    GRANT SELECT ON ALL TABLES IN SCHEMA processed TO data_analyst;

    -- Ensure SELECT permissions apply to future tables in processed
    ALTER DEFAULT PRIVILEGES IN SCHEMA processed
        GRANT SELECT ON TABLES TO data_analyst;

EOSQL

echo "Database setup completed for ${DB_NAME}"
