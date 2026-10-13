#!/usr/bin/env bash
# Sqoop INCREMENTAL import of the repo catalogue (PLAN.md §5, F2).
#
# A saved Sqoop job remembers the last value of updated_at, so each run imports only rows inserted or
# changed since the previous run, and --merge-key repo_id folds updates into the existing files
# (Sqoop runs a merge MapReduce job). Hive reads the result through the external table gh.catalogue_repos.
# Sqoop 1.4.7 can't combine --incremental lastmodified with --hive-import, hence HDFS dir + external table.
# The --input-* options let the merge job parse the \N nulls written by earlier runs.
set -euo pipefail
. "$(dirname "$0")/common.sh"

jobs=$(sqoop job --list 2>/dev/null || true)          # capture: avoids grep -q + pipefail
if ! grep -q '^ *repos_incr$' <<<"$jobs"; then
  sqoop job --create repos_incr -- import \
    --connect "$CATALOGUE_JDBC" --username catalogue --password-file "$PASSWORD_FILE" \
    --table repos -m 1 \
    --incremental lastmodified --check-column updated_at --merge-key repo_id \
    --target-dir /gh/raw/catalogue/repos \
    --fields-terminated-by '\t' --null-string '\\N' --null-non-string '\\N' \
    --input-fields-terminated-by '\t' --input-null-string '\\N' --input-null-non-string '\\N'
fi
sqoop job --exec repos_incr
echo "last-value now: $(sqoop job --show repos_incr 2>/dev/null | grep 'incremental.last.value')"
