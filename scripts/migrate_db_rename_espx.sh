#!/usr/bin/env bash
# Blue/green rename of Postgres database ad_event_processor -> espx (M7.17).
set -euo pipefail

OLD_DB="${OLD_DB:-ad_event_processor}"
NEW_DB="${NEW_DB:-espx}"
DUMP_PATH="${DUMP_PATH:-/tmp/${OLD_DB}_$(date +%Y%m%d_%H%M%S).dump}"
PGHOST="${PGHOST:-127.0.0.1}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-postgres}"

echo "1. Pause writers (management, payment, billing, tracker processor)."
echo "2. Verify replication lag = 0."
echo "3. pg_dump -Fc -h $PGHOST -p $PGPORT -U $PGUSER -d $OLD_DB -f $DUMP_PATH"
pg_dump -Fc -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$OLD_DB" -f "$DUMP_PATH"
echo "4. createdb -h $PGHOST -p $PGPORT -U $PGUSER $NEW_DB"
createdb -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" "$NEW_DB" || true
echo "5. pg_restore -h $PGHOST -p $PGPORT -U $PGUSER -d $NEW_DB --no-owner $DUMP_PATH"
pg_restore -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$NEW_DB" --no-owner "$DUMP_PATH"
echo "6. Update DB_DSN / PAYMENT_DB_DSN to database=$NEW_DB and restart services."
echo "7. Run smoke tests; drop $OLD_DB only after 24h stable operation."
