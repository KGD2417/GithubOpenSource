"""Load Mahout recommendations into Hive and evaluate them against Spark ALS and popularity (PLAN.md §7.1).
   pulse recs-eval

Inputs (HDFS, written by recs_prep.py and mahout/itemcf.sh):
  /gh/ml/mahout/train, /test          "user,repo" pairs (test = each user's latest 20% of stars)
  /gh/ml/mahout/recs                  "user \t [repo:score,...]"       (recommenditembased)
  /gh/ml/mahout/similarity            "repoA \t repoB \t llr-sim"      (itemsimilarity)
Outputs: gh.similar_repos, gh.user_recs (source = mahout | als), metrics in gh.model_metrics (task = recs)

Metrics per method, over users with held-out stars (k = 10):
  precision@k = mean(hits / k)   recall@k = mean(hits / |held-out|)   hit_rate = share of users with a hit
  coverage    = distinct recommended repos / repos in the training matrix
"""
import datetime as dt

from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

BASE = "/gh/ml/mahout"
K = 10


def pairs(spark, path):
    return (spark.read.text(path).select(F.split("value", ",").alias("p"))
                 .select(F.col("p")[0].cast("long").alias("actor_id"), F.col("p")[1].cast("long").alias("repo_id")))


def score(recs, test, n_items, name):
    """recs: actor_id, repo_id, rnk (1..K). Returns metric rows."""
    held = test.groupBy("actor_id").agg(F.count("*").alias("n_test"))
    hits = recs.join(test, ["actor_id", "repo_id"]).groupBy("actor_id").agg(F.count("*").alias("hits"))
    per_user = held.join(hits, "actor_id", "left").na.fill(0, ["hits"])
    m = per_user.agg(F.avg(F.col("hits") / K).alias("p"), F.avg(F.col("hits") / F.col("n_test")).alias("r"),
                     F.avg((F.col("hits") > 0).cast("double")).alias("hr"), F.count("*").alias("users")).first()
    cov = recs.select("repo_id").distinct().count() / max(n_items, 1)
    out = {f"precision_at_{K}": m["p"], f"recall_at_{K}": m["r"], "hit_rate": m["hr"], "coverage": cov,
           "eval_users": float(m["users"])}
    print(name, {k: round(v, 4) for k, v in out.items()})
    return [(name, k, float(v)) for k, v in out.items()]


def main():
    spark = SparkSession.builder.appName("pulse-recs-eval").enableHiveSupport().getOrCreate()
    train, test = pairs(spark, f"{BASE}/train").cache(), pairs(spark, f"{BASE}/test").cache()
    n_items = train.select("repo_id").distinct().count()
    names = spark.table("gh.repo_type_daily").groupBy("repo_id").agg(F.max("repo_name").alias("repo_name"))
    logins = spark.table("gh.stars").groupBy("actor_id").agg(F.max("actor_login").alias("actor_login"))

    # --- Mahout user recommendations: "user \t [repo:score,repo:score,...]"
    raw = spark.read.text(f"{BASE}/recs").select(F.split("value", "\t").alias("p"))
    mahout = (raw.select(F.col("p")[0].cast("long").alias("actor_id"),
                         F.posexplode(F.split(F.regexp_replace(F.col("p")[1], r"[\[\]]", ""), ",")).alias("pos", "rs"))
                 .select("actor_id", (F.col("pos") + 1).alias("rnk"),
                         F.split("rs", ":")[0].cast("long").alias("repo_id"),
                         F.split("rs", ":")[1].cast("double").alias("score")))

    # --- Mahout item similarity, made symmetric, top 20 per repo
    sim = (spark.read.text(f"{BASE}/similarity").select(F.split("value", "\t").alias("p"))
                .select(F.col("p")[0].cast("long").alias("a"), F.col("p")[1].cast("long").alias("b"),
                        F.col("p")[2].cast("double").alias("similarity")))
    sym = sim.unionByName(sim.select(F.col("b").alias("a"), F.col("a").alias("b"), "similarity"))
    w = Window.partitionBy("a").orderBy(F.desc("similarity"), "b")
    similar = (sym.withColumn("rnk", F.row_number().over(w)).where("rnk <= 20")
                  .join(names.withColumnRenamed("repo_id", "a"), "a", "left")
                  .join(names.select(F.col("repo_id").alias("b"), F.col("repo_name").alias("similar_repo_name")), "b", "left")
                  .select(F.col("a").alias("repo_id"), "repo_name", "rnk", F.col("b").alias("similar_repo_id"),
                          "similar_repo_name", "similarity"))
    similar.write.insertInto("gh.similar_repos", overwrite=True)

    # --- Spark ALS on the same training pairs (implicit feedback), same users, already-starred repos removed
    als = ALS(userCol="actor_id", itemCol="repo_id", ratingCol="r", implicitPrefs=True, rank=16, maxIter=10,
              regParam=0.1, alpha=10.0, coldStartStrategy="drop", seed=7)
    model = als.fit(train.withColumn("r", F.lit(1.0)))
    users = test.select("actor_id").distinct()
    cand = (model.recommendForUserSubset(users, K + 50)
                 .select("actor_id", F.posexplode("recommendations").alias("pos", "rec"))
                 .select("actor_id", F.col("rec.repo_id").alias("repo_id"), F.col("rec.rating").alias("score"), "pos")
                 .join(train, ["actor_id", "repo_id"], "left_anti"))
    als_recs = (cand.withColumn("rnk", F.row_number().over(Window.partitionBy("actor_id").orderBy("pos")))
                    .where(f"rnk <= {K}").select("actor_id", "rnk", "repo_id", "score"))

    # --- Popularity baseline: most-starred training repos the user hasn't starred
    pop = train.groupBy("repo_id").count().orderBy(F.desc("count"), "repo_id").limit(K + 50)
    pop_cand = users.crossJoin(pop).join(train, ["actor_id", "repo_id"], "left_anti")
    pop_recs = (pop_cand.withColumn("rnk", F.row_number().over(
                    Window.partitionBy("actor_id").orderBy(F.desc("count"), "repo_id")))
                        .where(f"rnk <= {K}").select("actor_id", "rnk", "repo_id"))

    rows = score(mahout, test, n_items, "mahout_itemcf_llr") + score(als_recs, test, n_items, "spark_als") \
        + score(pop_recs, test, n_items, "popularity")
    run_ts = dt.datetime.utcnow().isoformat(timespec="seconds")
    note = f"train_pairs={train.count()} test_pairs={test.count()} items={n_items}"
    spark.createDataFrame([(run_ts, "recs", m, k, v, note) for m, k, v in rows],
                          "run_ts string, task string, model string, metric string, value double, notes string") \
         .write.insertInto("gh.model_metrics", overwrite=False)

    user_recs = (mahout.withColumn("source", F.lit("mahout"))
                       .unionByName(als_recs.withColumn("source", F.lit("als")))
                       .join(logins, "actor_id", "left").join(names, "repo_id", "left")
                       .select("actor_id", "actor_login", "rnk", "repo_id", "repo_name", "score", "source"))
    user_recs.write.insertInto("gh.user_recs", overwrite=True)
    print(f"similar_repos rows={similar.count()} user_recs rows={user_recs.count()} ({note})")
    spark.stop()


if __name__ == "__main__":
    main()
