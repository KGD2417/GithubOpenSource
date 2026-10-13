"""Daily batch: one run per GH Archive day (catchup), raw -> clean -> curated with DQ gates.

download -> sqoop lookups -> Pig ETL -> DQ(clean) -> MapReduce top-N -> Hive curated -> DQ(curated)
Each task overwrites its own day/partition, so any run can be retried or re-run safely.
Trigger the window with:  airflow dags backfill gh_pulse_daily -s 2025-09-07 -e 2025-09-08
(or set catchup over the DAG's start/end dates). params.hours narrows the download (sample: 12-14).
"""
from datetime import datetime

from airflow.sdk import DAG, Param

from pulse_common import DEFAULT_ARGS, pulse, stage

with DAG(
    dag_id="gh_pulse_daily",
    schedule="@daily",
    start_date=datetime(2025, 9, 1),
    end_date=datetime(2025, 9, 8),
    catchup=False,                       # backfill explicitly for the chosen dataset (sample/demo/large)
    max_active_runs=1,                   # one day at a time: the laptop has one cluster's worth of RAM
    default_args=DEFAULT_ARGS,
    params={"hours": Param("0-23", type="string", description="hour range to load, e.g. 12-14")},
    tags=["pulse", "batch"],
) as dag:
    ds = "{{ ds }}"
    warehouse = stage("warehouse")
    download = pulse("download_load", f"download --start {ds} --end {ds} --hours {{{{ params.hours }}}}", retries=5)
    lookups = pulse("sqoop_import_lookups", "sqoop-import-lookups")
    pig = pulse("pig_etl", f"pig-etl {ds}")
    dq_clean = pulse("dq_clean", f"dq clean {ds}", retries=0)          # a failed DQ gate should stop, not retry
    mr = pulse("mapreduce_top_repos", f"mr-top {ds}")
    curated = pulse("hive_curated", f"hive-curated {ds}")
    dq_curated = pulse("dq_curated", f"dq curated {ds}", retries=0)

    warehouse >> download >> lookups >> pig >> dq_clean >> mr >> curated >> dq_curated
