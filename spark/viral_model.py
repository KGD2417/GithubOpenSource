"""Viral-repo prediction with Spark MLlib (PLAN.md §7.2).   pulse spark-viral [--threshold N]

Reads gh.repo_features (built by features.py). Label = 1 when label_stars >= threshold; by default the
threshold is the ~97th percentile of label_stars (at least 1), so roughly the top 3% of new repos are
"viral". Split is BY TIME: repos created in the earlier part of the window train the model, later ones
test it, so the model never sees the future. Two models: LogisticRegression (baseline) and
GBTClassifier, both with class weights for the imbalance. Metrics: AUC-PR (primary), AUC-ROC,
precision@k, recall at 0.5. Results -> gh.model_metrics, gh.viral_predictions.
"""
import argparse
import datetime as dt

from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

FEATURES = ["is_org", "has_description", "name_len", "create_hour", "create_weekday", "f_stars", "f_forks",
            "f_stargazers", "f_star_velocity_last", "f_max_stars_hour", "f_pushes", "f_prs", "f_issues",
            "f_releases", "f_actors", "f_bot_share"]


def precision_at_k(pred, k):
    top = pred.orderBy(F.desc("probability_1")).limit(k)
    return top.agg(F.avg("label")).first()[0] or 0.0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--threshold", type=int, default=0, help="stars in label window; 0 = auto (p97, min 1)")
    ap.add_argument("--train-fraction", type=float, default=0.5, help="earliest share of creation times")
    ap.add_argument("--k", type=int, default=50)
    a = ap.parse_args()
    spark = SparkSession.builder.appName("pulse-viral").enableHiveSupport().getOrCreate()

    df = spark.table("gh.repo_features").drop("label")
    n = df.count()
    if n < 20:
        raise SystemExit(f"only {n} new repos in gh.repo_features; load more data (demo/large)")
    thr = a.threshold or max(1, int(df.approxQuantile("label_stars", [0.97], 0.001)[0]))
    df = df.withColumn("label", (F.col("label_stars") >= thr).cast("int"))

    ts = df.withColumn("t", F.col("created_at").cast("long"))
    cut = ts.approxQuantile("t", [a.train_fraction], 0.001)[0]
    train = ts.where(F.col("t") <= cut).withColumn("split", F.lit("train"))
    test = ts.where(F.col("t") > cut).withColumn("split", F.lit("test"))
    pos_rate = train.agg(F.avg("label")).first()[0] or 0.0
    w = (1 - pos_rate) / pos_rate if pos_rate > 0 else 1.0
    train = train.withColumn("w", F.when(F.col("label") == 1, w).otherwise(1.0))
    test = test.withColumn("w", F.lit(1.0))

    assembler = VectorAssembler(inputCols=FEATURES, outputCol="raw")
    models = {
        "logreg": Pipeline(stages=[assembler, StandardScaler(inputCol="raw", outputCol="features"),
                                   LogisticRegression(labelCol="label", weightCol="w", maxIter=50, regParam=0.01)]),
        "gbt": Pipeline(stages=[VectorAssembler(inputCols=FEATURES, outputCol="features"),
                                GBTClassifier(labelCol="label", weightCol="w", maxDepth=4, maxIter=50, seed=7)]),
    }
    run_ts = dt.datetime.utcnow().isoformat(timespec="seconds")
    rows, preds = [], []
    note = f"threshold={thr} n={n} train={train.count()} test={test.count()} pos_rate_train={pos_rate:.4f}"
    print(note)
    for name, pipe in models.items():
        model = pipe.fit(train)
        pred = model.transform(test).withColumn("probability_1", vector_to_array("probability")[1])
        has_both = pred.select("label").distinct().count() == 2
        roc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC").evaluate(pred) if has_both else None
        pr = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderPR").evaluate(pred) if has_both else None
        tp = pred.where((F.col("prediction") == 1) & (F.col("label") == 1)).count()
        positives = pred.where(F.col("label") == 1).count()
        metrics = {"auc_pr": pr, "auc_roc": roc, f"precision_at_{a.k}": precision_at_k(pred, a.k),
                   "recall_at_0.5": tp / positives if positives else None,
                   "test_positives": float(positives), "baseline_auc_pr": float(positives) / max(pred.count(), 1)}
        print(name, {k: (round(v, 4) if v is not None else None) for k, v in metrics.items()})
        rows += [(run_ts, "viral", name, k, float(v) if v is not None else None, note) for k, v in metrics.items()]
        if name == "gbt":
            imp = model.stages[-1].featureImportances.toArray()
            top = sorted(zip(FEATURES, imp), key=lambda x: -x[1])[:8]
            print("gbt feature importance:", [(f, round(float(v), 3)) for f, v in top])
            rows += [(run_ts, "viral", "gbt", f"importance_{f}", float(v), note) for f, v in top]
        preds.append(pred.select("repo_id", "repo_name", "created_at", "split", F.lit(name).alias("model"),
                                 "probability_1", "label", "label_stars"))

    spark.createDataFrame(rows, "run_ts string, task string, model string, metric string, value double, notes string") \
         .write.insertInto("gh.model_metrics", overwrite=False)
    out = preds[0]
    for p in preds[1:]:
        out = out.unionByName(p)
    out.withColumnRenamed("probability_1", "probability").write.insertInto("gh.viral_predictions", overwrite=True)
    spark.stop()


if __name__ == "__main__":
    main()
