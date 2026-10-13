"""Window-level analytics, ML and serving, run once after the daily DAG has loaded the window.

catalogue (GitHub API) -> Sqoop incremental import -> Hive window tables
  -> Spark features -> { Spark viral model | Mahout item CF -> evaluation vs ALS | Mahout k-means }
  -> HBase serving load -> Sqoop export to Postgres
Stage switches keep each group inside the laptop's memory budget.
"""
from datetime import datetime

from airflow.sdk import DAG, Param

from pulse_common import DEFAULT_ARGS, pulse, stage

with DAG(
    dag_id="gh_pulse_models",
    schedule=None,
    start_date=datetime(2025, 9, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "top_n": Param(500, type="integer", description="repos to fetch into the catalogue"),
        "feature_hours": Param(24, type="integer"), "label_hours": Param(120, type="integer"),
        "min_user_stars": Param(3, type="integer"), "min_repo_stars": Param(5, type="integer"),
    },
    tags=["pulse", "ml"],
) as dag:
    p = "{{ params.%s }}"
    warehouse = stage("warehouse")
    seed = pulse("seed_catalogue", f"seed-catalogue {p % 'top_n'}")
    sqoop_in = pulse("sqoop_import_repos_incremental", "sqoop-import-repos")
    window = pulse("hive_window_tables", "hive-window")
    export = pulse("sqoop_export", "sqoop-export")

    ml = stage("ml")
    features = pulse("spark_features",
                     f"spark-features --feature-hours {p % 'feature_hours'} --label-hours {p % 'label_hours'}")
    viral = pulse("spark_viral_model", "spark-viral")
    prep = pulse("recs_prep", f"recs-prep --min-user-stars {p % 'min_user_stars'} --min-repo-stars {p % 'min_repo_stars'}")
    itemcf = pulse("mahout_itemcf", "mahout-itemcf")
    evaluate = pulse("recs_eval_vs_als", "recs-eval")
    kmeans = pulse("mahout_kmeans", "mahout-kmeans 6")
    hbase_prep = pulse("hbase_prep", "hbase-prep")

    serving = stage("serving")
    hbase_init = pulse("hbase_init", "hbase-init")
    hbase_load = pulse("hbase_load", "hbase-load")
    back = stage("warehouse", task_id="stage_warehouse_again")

    warehouse >> seed >> sqoop_in >> window >> ml >> features
    features >> viral
    features >> prep >> itemcf >> evaluate
    features >> kmeans
    [viral, evaluate, kmeans] >> hbase_prep >> serving >> hbase_init >> hbase_load >> back >> export
