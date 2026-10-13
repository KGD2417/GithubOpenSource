"""Spark feature engineering (PLAN.md §7.2, Q6, Q8).   pulse spark-features [--feature-hours 24 --label-hours 120]

1. gh.repo_profile   per repo: fork/star ratio, contributor count, top-contributor share and Gini of
                     contribution events -> solo / small-team / community.
2. gh.repo_features  per NEW repo (CreateEvent ref_type=repository): activity in its first F hours and
                     the star count in the following L hours (the viral label input). Only repos whose
                     whole label window lies inside the loaded data are kept, so labels are never truncated.
"""
import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

CONTRIB_TYPES = ["PushEvent", "PullRequestEvent", "IssuesEvent", "IssueCommentEvent",
                 "PullRequestReviewEvent", "PullRequestReviewCommentEvent", "CreateEvent", "ReleaseEvent"]


def repo_profile(spark, min_events):
    ev = spark.table("gh.events_clean")
    contrib = (ev.where(F.col("event_type").isin(CONTRIB_TYPES) & ~F.col("is_bot"))
                 .groupBy("repo_id", "actor_id").agg(F.count("*").alias("n")))

    # Gini over contributors' event counts: G = 2*sum(i*x_i)/(n*sum x) - (n+1)/n, x sorted ascending
    w = Window.partitionBy("repo_id").orderBy("n")
    gini = (contrib.withColumn("i", F.row_number().over(w))
                   .groupBy("repo_id")
                   .agg(F.count("*").alias("contributors"), F.sum("n").alias("contribution_events"),
                        F.max("n").alias("top1"), F.sum(F.col("i") * F.col("n")).alias("isum"))
                   .withColumn("gini", F.when(F.col("contributors") > 1,
                               2 * F.col("isum") / (F.col("contributors") * F.col("contribution_events"))
                               - (F.col("contributors") + 1) / F.col("contributors")).otherwise(F.lit(0.0)))
                   .withColumn("top1_share", F.col("top1") / F.col("contribution_events")))

    totals = ev.groupBy("repo_id").agg(
        F.max("repo_name").alias("repo_name"),
        F.sum(F.when(F.col("event_type") == "WatchEvent", 1).otherwise(0)).alias("stars"),
        F.sum(F.when(F.col("event_type") == "ForkEvent", 1).otherwise(0)).alias("forks"),
        F.countDistinct("dt").alias("active_days"),
        F.avg(F.col("is_bot").cast("int")).alias("bot_share"),
        F.count("*").alias("events"))

    prof = (totals.where(F.col("events") >= min_events).join(gini, "repo_id", "left")
            .withColumn("fork_star_ratio", F.when(F.col("stars") > 0, F.col("forks") / F.col("stars")))
            .withColumn("profile",
                        F.when(F.col("contributors").isNull(), "no-contributions")
                         .when((F.col("contributors") <= 2) | (F.col("top1_share") >= 0.8), "solo")
                         .when((F.col("contributors") >= 10) & (F.col("top1_share") < 0.5), "community")
                         .otherwise("small-team")))
    cols = ["repo_id", "repo_name", "stars", "forks", "fork_star_ratio", "contributors", "contribution_events",
            "top1_share", "gini", "active_days", "bot_share", "profile"]
    prof.select(*cols).write.insertInto("gh.repo_profile", overwrite=True)
    return prof


def repo_features(spark, feature_hours, label_hours):
    ev = spark.table("gh.events_clean").select(
        "repo_id", "repo_name", "event_type", "created_ts", "created_at", "actor_id", "is_bot",
        "ref_type", "org_login", "event_id")
    data_end = ev.agg(F.max("created_ts")).first()[0]
    fs, ls = feature_hours * 3600, label_hours * 3600

    births = (ev.where((F.col("event_type") == "CreateEvent") & (F.col("ref_type") == "repository"))
                .groupBy("repo_id")
                .agg(F.min("created_ts").alias("t0"), F.min("created_at").alias("created_at"),
                     F.max("repo_name").alias("repo_name"),
                     F.max(F.col("org_login").isNotNull().cast("int")).alias("is_org"))
                .where(F.col("t0") + fs + ls <= F.lit(data_end)))          # full label window observed

    # has_description comes from the CreateEvent payload, which the clean zone doesn't keep. Read the raw
    # JSON text directly (a substring pre-filter means only repository-creation lines get parsed).
    desc = (spark.read.text("/gh/raw/events/dt=*/")
                 .where(F.col("value").contains('"type":"CreateEvent"') & F.col("value").contains('"ref_type":"repository"'))
                 .select(F.get_json_object("value", "$.repo.id").cast("long").alias("repo_id"),
                         F.get_json_object("value", "$.payload.description").alias("d"))
                 .groupBy("repo_id").agg(F.max(F.col("d").isNotNull().cast("int")).alias("has_description")))

    e = ev.join(births.select("repo_id", "t0"), "repo_id")
    rel = (F.col("created_ts") - F.col("t0"))
    first = e.where((rel >= 0) & (rel < fs))
    star = F.col("event_type") == "WatchEvent"

    hourly_stars = (first.where(star).groupBy("repo_id", F.floor(rel / 3600).alias("h"))
                         .agg(F.count("*").alias("n")).groupBy("repo_id").agg(F.max("n").alias("f_max_stars_hour")))
    last_slice = max(fs // 4, 3600)
    feats = first.groupBy("repo_id").agg(
        F.sum(star.cast("int")).alias("f_stars"),
        F.sum((F.col("event_type") == "ForkEvent").cast("int")).alias("f_forks"),
        F.countDistinct(F.when(star, F.col("actor_id"))).alias("f_stargazers"),
        F.sum((star & (rel >= fs - last_slice)).cast("int")).alias("f_star_velocity_last"),
        F.sum((F.col("event_type") == "PushEvent").cast("int")).alias("f_pushes"),
        F.sum((F.col("event_type") == "PullRequestEvent").cast("int")).alias("f_prs"),
        F.sum((F.col("event_type") == "IssuesEvent").cast("int")).alias("f_issues"),
        F.sum((F.col("event_type") == "ReleaseEvent").cast("int")).alias("f_releases"),
        F.countDistinct("actor_id").alias("f_actors"),
        F.avg(F.col("is_bot").cast("int")).alias("f_bot_share"))
    label = (e.where(star & ~F.col("is_bot") & (rel >= fs) & (rel < fs + ls))
              .groupBy("repo_id").agg(F.count("*").alias("label_stars")))

    out = (births.join(feats, "repo_id", "left").join(hourly_stars, "repo_id", "left")
                 .join(label, "repo_id", "left").join(desc, "repo_id", "left")
                 .na.fill(0)
                 .withColumn("name_len", F.length(F.element_at(F.split("repo_name", "/"), -1)))
                 .withColumn("create_hour", F.hour("created_at"))
                 .withColumn("create_weekday", F.dayofweek("created_at"))
                 .withColumn("label", F.lit(None).cast("int")))        # threshold applied in viral_model.py
    cols = ["repo_id", "repo_name", "created_at", "is_org", "has_description", "name_len", "create_hour",
            "create_weekday", "f_stars", "f_forks", "f_stargazers", "f_star_velocity_last", "f_max_stars_hour",
            "f_pushes", "f_prs", "f_issues", "f_releases", "f_actors", "f_bot_share", "label_stars", "label"]
    out.select(*cols).write.insertInto("gh.repo_features", overwrite=True)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--feature-hours", type=int, default=24)
    ap.add_argument("--label-hours", type=int, default=120)
    ap.add_argument("--min-events", type=int, default=20, help="repo_profile only for repos this active")
    a = ap.parse_args()
    spark = SparkSession.builder.appName("pulse-features").enableHiveSupport().getOrCreate()

    prof = repo_profile(spark, a.min_events)
    print("repo_profile:", {r["profile"]: r["count"] for r in prof.groupBy("profile").count().collect()})
    feats = repo_features(spark, a.feature_hours, a.label_hours)
    s = feats.agg(F.count("*").alias("new_repos"), F.sum("label_stars").alias("label_stars"),
                  F.max("label_stars").alias("max_label_stars")).first()
    print(f"repo_features: {s['new_repos']} new repos (features {a.feature_hours}h, label {a.label_hours}h), "
          f"label stars total={s['label_stars']} max={s['max_label_stars']}")
    spark.stop()


if __name__ == "__main__":
    main()
