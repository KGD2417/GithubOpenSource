#!/usr/bin/env bash
# Mahout 0.13 item-based collaborative filtering on implicit stars (PLAN.md §7.1).   pulse mahout-itemcf
#
# Input: /gh/ml/mahout/train  "userID,repoID" pairs (spark/recs_prep.py). Stars carry no rating, so
# --booleanData treats every pair as "1". Log-likelihood ratio similarity is built for this case:
# it asks whether two repos are co-starred more often than chance, without over-rewarding rare repos
# the way cosine or Jaccard do. Both jobs are chains of MapReduce jobs on YARN.
set -euo pipefail
B=/gh/ml/mahout
hdfs dfs -rm -r -f -skipTrash $B/similarity $B/recs $B/tmp >/dev/null

# Item-item similarity: "repoA \t repoB \t similarity", top 20 neighbours per repo -> "similar repos"
mahout itemsimilarity -i $B/train -o $B/similarity --booleanData true \
  --similarityClassname SIMILARITY_LOGLIKELIHOOD --maxSimilaritiesPerItem 20 --minPrefsPerUser 1 \
  --tempDir $B/tmp/sim

# Per-user top 10 "repos you may like" for the users that have held-out stars (evaluation set).
# --usersFile must be a single file, not Spark's output directory.
hdfs dfs -cat "$B/users/part-*" | hdfs dfs -put -f - $B/users.txt
mahout recommenditembased -i $B/train -o $B/recs --booleanData true \
  --similarityClassname SIMILARITY_LOGLIKELIHOOD --numRecommendations 10 --maxPrefsPerUser 300 \
  --maxSimilaritiesPerItem 50 --usersFile $B/users.txt --tempDir $B/tmp/rec

echo "similar pairs: $(hdfs dfs -cat $B/similarity/part-* | wc -l)   users with recs: $(hdfs dfs -cat $B/recs/part-* | wc -l)"
