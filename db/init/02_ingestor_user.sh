#!/bin/bash
# App user for the worker: insert/select on ticks and nothing else.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE ROLE ingestor WITH LOGIN PASSWORD '${POSTGRES_PASSWORD}';
    GRANT CONNECT ON DATABASE ${POSTGRES_DB} TO ingestor;
    GRANT USAGE ON SCHEMA public TO ingestor;
    GRANT SELECT, INSERT ON ticks TO ingestor;
    GRANT SELECT ON ticks_summary TO ingestor;
EOSQL
