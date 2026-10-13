#!/usr/bin/env bash
# Sqoop EXPORT of curated results back to Postgres (PLAN.md §5, F2): trending_daily, language_trends.
#
# 1. Hive writes the rows as TSV under /gh/curated/export/<table> (Sqoop can't export Parquet without Kite).
# 2. `sqoop eval` empties the target table. Sqoop 1.4.7's Postgres connector can't upsert, and
#    --update-key can't be combined with a staging table, so "clear then reload" keeps re-runs idempotent.
# 3. `sqoop export --staging-table` loads into <table>_stage first and moves rows into <table> in one
#    transaction, so readers never see a half-written table.
set -euo pipefail
. "$(dirname "$0")/common.sh"

export_one() {
  local table=$1 select=$2 dir=/gh/curated/export/$1
  beeline -u jdbc:hive2://hiveserver2:10000 -n root --silent=true -e \
    "INSERT OVERWRITE DIRECTORY '$dir' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' $select" 2>&1 \
    | { grep -E 'Error|FAILED' || true; }
  sqoop eval --connect "$CATALOGUE_JDBC" --username catalogue --password-file "$PASSWORD_FILE" \
    --query "TRUNCATE $table" >/dev/null 2>&1
  sqoop export --connect "$CATALOGUE_JDBC" --username catalogue --password-file "$PASSWORD_FILE" \
    --table "$table" --staging-table "${table}_stage" --clear-staging-table \
    --export-dir "$dir" -m 1 \
    --input-fields-terminated-by '\t' --input-null-string '\\N' --input-null-non-string '\\N' 2>&1 \
    | { grep -E 'Exported [0-9]+ records|ERROR' || true; }
}

export_one trending_daily \
  "SELECT dt, event_type, rnk, repo_id, repo_name, events, prev_day_events, growth_pct FROM gh.trending_daily"
export_one language_trends \
  "SELECT dt, language, events, stars, active_repos FROM gh.language_trends"
