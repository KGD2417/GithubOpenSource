#!/usr/bin/env bash
# Mahout 0.13 k-means over repo activity-mix vectors (bonus, PLAN.md §7.1).   pulse mahout-kmeans [k]
set -euo pipefail
B=/gh/ml/mahout/kmeans K=${1:-6}
JAR=/opt/pulse/java/hadoop-jobs/target/pulse-hadoop.jar
# Mahout's vector classes for our helper only; exporting this globally would shadow the
# self-contained mahout job jar that the `mahout` command runs.
MCP="/opt/mahout/mahout-math-0.13.0.jar:/opt/mahout/mahout-hdfs-0.13.0.jar:/opt/mahout/mahout-mr-0.13.0.jar"
hdfs dfs -rm -r -f -skipTrash $B/vectors $B/initial $B/out >/dev/null

hdfs dfs -cat "$B/input_tsv/part-*" | hdfs dfs -put -f - $B/input.tsv
HADOOP_CLASSPATH=$MCP hadoop jar $JAR pulse.mahout.KMeansIO vectors $B/input.tsv $B/vectors/part-00000
# -k with an empty -c dir: Mahout picks k random vectors as initial centroids. -cl assigns every point.
mahout kmeans -i $B/vectors -c $B/initial -o $B/out -k "$K" -x 20 -cd 0.01 -ow -cl \
  -dm org.apache.mahout.common.distance.CosineDistanceMeasure
HADOOP_CLASSPATH=$MCP hadoop jar $JAR pulse.mahout.KMeansIO assignments $B/out/clusteredPoints $B/assignments.tsv
