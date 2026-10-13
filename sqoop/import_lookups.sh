#!/usr/bin/env bash
# Sqoop FULL imports of the small lookup tables straight into Hive (gh.known_bots, gh.event_types).
# --hive-import writes to a temp HDFS dir, then runs the hive CLI to create the table and LOAD DATA.
set -euo pipefail
. "$(dirname "$0")/common.sh"

for t in known_bots event_types; do
  sqoop import \
    --connect "$CATALOGUE_JDBC" --username catalogue --password-file "$PASSWORD_FILE" \
    --table "$t" -m 1 \
    --target-dir "/tmp/sqoop/$t" --delete-target-dir \
    --hive-import --hive-overwrite --hive-database gh --hive-table "$t" \
    --fields-terminated-by '\t' --null-string '\\N' --null-non-string '\\N'
done
