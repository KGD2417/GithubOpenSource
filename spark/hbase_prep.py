"""Shape serving data into HBase row keys as TSV for ImportTsv bulk loads (PLAN.md §4.4).   pulse hbase-prep

Row-key salt = first 2 hex chars of MD5(id). Repo ids grow over time and hour suffixes are monotonic,
so unsalted keys would send every new write to the last region (a hotspot). The salt spreads rows
evenly over the 16 pre-split regions, and because it's derived from the id, a reader recomputes it
and still does a single Get / prefix Scan per repo.

  repo_hourly   <salt>|<repo_id:012d>|<yyyyMMddHH>  c:Watch c:Fork c:Push c:PR c:Issues c:Comment c:Create c:Release c:other c:bot c:total
  repo_similar  <salt>|<repo_id:012d>               m:name, s:01..s:20 = "similar_id|name|llr"
  user_recs     <salt>|<login>                      r:01..r:10 = "repo_name|score"   (Mahout)
  repo_index    <salt>|<lower(full_name)>           i:id  (name -> id, so lookups work by repo name)
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

OUT = "/gh/serving/tsv"
TYPES = {"WatchEvent": "watch", "ForkEvent": "fork", "PushEvent": "push", "PullRequestEvent": "pr",
         "IssuesEvent": "issues", "IssueCommentEvent": "comment", "CreateEvent": "create",
         "ReleaseEvent": "release"}


def salt(col):
    return F.substring(F.md5(col.cast("string")), 1, 2)


def rid(col):
    return F.lpad(col.cast("string"), 12, "0")


def write(df, name):
    df.coalesce(2).write.mode("overwrite").option("sep", "\t").csv(f"{OUT}/{name}")
    print(f"{name}: {df.count()} rows")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-events", type=int, default=10, help="serve hourly series for repos this active")
    a = ap.parse_args()
    spark = SparkSession.builder.appName("pulse-hbase-prep").enableHiveSupport().getOrCreate()

    ev = spark.table("gh.events_clean")
    active = ev.groupBy("repo_id").count().where(F.col("count") >= a.min_events).select("repo_id")
    t = F.coalesce(*[F.when(F.col("event_type") == k, F.lit(v)) for k, v in TYPES.items()], F.lit("other"))
    hourly = (ev.join(active, "repo_id")
                .withColumn("hour", F.date_format("created_at", "yyyyMMddHH")).withColumn("t", t)
                .groupBy("repo_id", "hour")
                .agg(*[F.sum((F.col("t") == v).cast("int")).alias(v) for v in list(TYPES.values()) + ["other"]],
                     F.sum(F.col("is_bot").cast("int")).alias("bot"), F.count("*").alias("total")))
    key = F.concat_ws("|", salt(F.col("repo_id")), rid(F.col("repo_id")), F.col("hour"))
    write(hourly.select(key.alias("k"), *list(TYPES.values()), "other", "bot", "total"), "repo_hourly")

    sim = spark.table("gh.similar_repos").where("rnk <= 20")
    cells = (sim.groupBy("repo_id", "repo_name")
                .pivot("rnk", list(range(1, 21)))
                .agg(F.first(F.concat_ws("|", "similar_repo_id", F.coalesce("similar_repo_name", F.lit("?")),
                                         F.format_number("similarity", 4)))))
    write(cells.select(F.concat_ws("|", salt(F.col("repo_id")), rid(F.col("repo_id"))).alias("k"),
                       F.coalesce("repo_name", F.lit("?")).alias("name"),
                       *[F.coalesce(F.col(str(i)), F.lit("")) for i in range(1, 21)]), "repo_similar")

    recs = (spark.table("gh.user_recs").where("source = 'mahout' AND actor_login IS NOT NULL AND rnk <= 10")
                 .groupBy("actor_login").pivot("rnk", list(range(1, 11)))
                 .agg(F.first(F.concat_ws("|", F.coalesce("repo_name", F.col("repo_id").cast("string")),
                                          F.format_number("score", 3)))))
    write(recs.select(F.concat_ws("|", salt(F.col("actor_login")), F.col("actor_login")).alias("k"),
                      *[F.coalesce(F.col(str(i)), F.lit("")) for i in range(1, 11)]), "user_recs")

    names = spark.table("gh.repo_type_daily").groupBy("repo_id").agg(F.max("repo_name").alias("n"))
    write(names.select(F.concat_ws("|", salt(F.lower("n")), F.lower("n")).alias("k"), "repo_id"), "repo_index")
    spark.stop()


if __name__ == "__main__":
    main()
