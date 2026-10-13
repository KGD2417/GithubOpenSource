"""Spark side of the Mahout k-means bonus (PLAN.md §7.1).
  kmeans_io.py prep [--min-events 20]   gh.repo_daily_activity -> /gh/ml/mahout/kmeans/input.tsv
  kmeans_io.py load                     Mahout assignments -> gh.repo_clusters (+ auto cluster labels)

Each repo becomes an "activity mix" vector: the share of its events that are stars, forks, pushes, PRs,
issues, comments, reviews, releases, plus bot share and log10(volume)/6. Cosine distance on shares
groups repos by HOW they are used, not by how big they are.
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE = "/gh/ml/mahout/kmeans"
MIX = ["stars", "forks", "pushes", "prs_opened", "issues_opened", "comments", "reviews", "releases"]
LABEL = {"stars": "star magnet", "forks": "fork magnet", "pushes": "push-only / solo dev",
         "prs_opened": "PR-driven", "issues_opened": "issue tracker", "comments": "discussion-heavy",
         "reviews": "review-heavy", "releases": "release-driven"}


def mix(spark, min_events):
    a = spark.table("gh.repo_daily_activity").groupBy("repo_id").agg(
        F.max("repo_name").alias("repo_name"), F.sum("total_events").alias("total_events"),
        F.sum("bot_events").alias("bot_events"), *[F.sum(c).alias(c) for c in MIX])
    a = a.where(F.col("total_events") >= min_events)
    for c in MIX:
        a = a.withColumn(f"{c}_s", F.col(c) / F.col("total_events"))
    return (a.withColumn("bot_s", F.col("bot_events") / F.col("total_events"))
             .withColumn("vol_s", F.log10("total_events") / 6))


def main():
    spark = SparkSession.builder.appName("pulse-kmeans-io").enableHiveSupport().getOrCreate()
    min_events = int(sys.argv[sys.argv.index("--min-events") + 1]) if "--min-events" in sys.argv else 20
    m = mix(spark, min_events)
    cols = [f"{c}_s" for c in MIX] + ["bot_s", "vol_s"]
    if sys.argv[1] == "prep":
        m.select(F.concat_ws("\t", "repo_name", *[F.format_number(c, 6) for c in cols]).alias("v")) \
         .coalesce(1).write.mode("overwrite").text(f"{BASE}/input_tsv")
        print("kmeans vectors:", m.count())
    else:
        asg = (spark.read.option("sep", "\t").csv(f"{BASE}/assignments.tsv")
                    .toDF("repo_name", "cluster_id", "distance")
                    .select("repo_name", F.col("cluster_id").cast("int"), F.col("distance").cast("double")))
        j = asg.join(m, "repo_name")
        cent = j.groupBy("cluster_id").agg(F.count("*").alias("repos"), F.avg("bot_s").alias("bot"),
                                           *[F.avg(f"{c}_s").alias(c) for c in MIX])
        labels = {}
        for r in cent.collect():
            first, second = sorted(MIX, key=lambda c: -r[c])[:2]
            what = LABEL[first] if r[first] >= 0.5 else f"mixed: {first.split('_')[0]} + {second.split('_')[0]}"
            labels[r["cluster_id"]] = ("bot-driven, " if r["bot"] > 0.5 else "") + what
            print(f"cluster {r['cluster_id']}: {r['repos']} repos -> {labels[r['cluster_id']]}  "
                  + " ".join(f"{c}={r[c]:.2f}" for c in MIX) + f" bot={r['bot']:.2f}")
        lab = spark.createDataFrame(list(labels.items()), "cluster_id int, cluster_label string")
        (j.join(lab, "cluster_id")
          .select("repo_name", "cluster_id", "cluster_label", "distance", "total_events",
                  F.col("stars_s").alias("star_share"), F.col("pushes_s").alias("push_share"),
                  F.col("prs_opened_s").alias("pr_share"), F.col("issues_opened_s").alias("issue_share"),
                  F.col("comments_s").alias("comment_share"), F.col("bot_s").alias("bot_share"))
          .write.insertInto("gh.repo_clusters", overwrite=True))
    spark.stop()


if __name__ == "__main__":
    main()
