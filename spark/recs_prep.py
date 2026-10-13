"""Prepare the implicit star matrix for Mahout item-based CF (PLAN.md §7.1).   pulse recs-prep

gh.stars (human WatchEvents) -> distinct (user, repo) pairs -> noise filters -> time-based holdout.
  * users with fewer than --min-user-stars give no co-occurrence signal; more than --max-user-stars look
    like star-farming/automation: both dropped
  * repos with fewer than --min-repo-stars stargazers can't be recommended reliably: dropped
  * every user with >= 5 stars keeps their LATEST 20% of stars (by time) aside as the test set
Writes Mahout's input format (userID,itemID) to HDFS:
  /gh/ml/mahout/train   training pairs      /gh/ml/mahout/test   held-out pairs
  /gh/ml/mahout/users   users to recommend for (those with held-out stars)
"""
import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

BASE = "/gh/ml/mahout"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-user-stars", type=int, default=3)
    ap.add_argument("--max-user-stars", type=int, default=300)
    ap.add_argument("--min-repo-stars", type=int, default=5)
    a = ap.parse_args()
    spark = SparkSession.builder.appName("pulse-recs-prep").enableHiveSupport().getOrCreate()

    stars = (spark.table("gh.stars").groupBy("actor_id", "repo_id")
                  .agg(F.min("starred_at").alias("ts")))
    raw_pairs = stars.count()
    # One pass of each filter: repo popularity first, then user activity on what's left
    repos = stars.groupBy("repo_id").count().where(F.col("count") >= a.min_repo_stars).select("repo_id")
    s = stars.join(repos, "repo_id")
    users = (s.groupBy("actor_id").count()
              .where((F.col("count") >= a.min_user_stars) & (F.col("count") <= a.max_user_stars)).select("actor_id"))
    s = s.join(users, "actor_id").cache()

    w = Window.partitionBy("actor_id").orderBy(F.col("ts").desc(), F.col("repo_id"))
    n = F.count("*").over(Window.partitionBy("actor_id"))
    ranked = s.withColumn("r", F.row_number().over(w)).withColumn("n", n)
    is_test = (F.col("n") >= 5) & (F.col("r") <= F.greatest(F.lit(1), F.floor(F.col("n") * 0.2)))
    ranked = ranked.withColumn("test", is_test)

    train = ranked.where(~F.col("test")).select("actor_id", "repo_id")
    test = ranked.where(F.col("test")).select("actor_id", "repo_id")
    for name, df in (("train", train), ("test", test)):
        df.select(F.concat_ws(",", "actor_id", "repo_id")).coalesce(1).write.mode("overwrite").text(f"{BASE}/{name}")
    test.select(F.col("actor_id").cast("string")).distinct().coalesce(1).write.mode("overwrite").text(f"{BASE}/users")

    print(f"stars pairs raw={raw_pairs} after filters={s.count()} users={s.select('actor_id').distinct().count()} "
          f"repos={s.select('repo_id').distinct().count()} train={train.count()} test={test.count()} "
          f"test_users={test.select('actor_id').distinct().count()}")
    spark.stop()


if __name__ == "__main__":
    main()
