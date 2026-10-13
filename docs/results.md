# Results (measured on the dev Mac, LITE mode)

**Hardware:** Apple Silicon, 8 GB RAM, Docker capped at 4 GB. The cluster ran with 1 NodeManager (2 for the worker experiment).

**Data loaded:** 15 hours of GH Archive, **2,397,993 events**:

| Dataset | Hours | Note |
|---|---|---|
| sample | 2025-09-01, 12:00–14:00 UTC | a Monday |
| medium | 2025-09-07 and 2025-09-08, 10:00–15:00 UTC each | Sunday + Monday |

The planned demo (2 full days) and large (8 days) sizes need the 16 GB / ≥ 60 GB laptop. The commands are the same: `make download MODE=demo|large` and the DAGs.

## Data quality (`bench/charts/dq_funnel.png`)

| Stage | Records |
|---|---|
| raw lines downloaded | 2,397,993 |
| parsed by the Pig UDF | 2,397,993 (0 malformed) |
| clean, deduplicated | 2,397,992 (1 duplicate event id on 2025-09-08) |
| clean, human only | 1,685,763 |
| curated repo-days | 814,035 |
| curated human stars | 76,864 |

Every curated table sums back to the clean count for its day (`pulse dq curated`).

The DQ gate caught two real problems during the build:
- An hourly file with no trailing newline: `wc -l` was off by one against Pig's record count.
- 140 events on 2025-09-08 already in GitHub's later trimmed payload format.

## Answers to the analytical questions

- **Q1 trending:** human stars, 2025-09-01 12–15 UTC: dockur/windows (168), pewdiepie-archdaemon/dionysus (93), JetBrains/koog (56). Raw push volume is led by automated repos (farhan7reza7/hosters: 4,138 pushes in 3 h). That's why the catalogue is seeded by distinct human actors.
- **Q3 activity clock:** Sunday's 10–15 UTC hours carry 157k events/hour against 162k on the weekday, only about 3% less.
- **Q4 bots:** 28–32% of all events are from bots. Bots dominate commit comments (60%), issue comments (52%) and pull requests (49%).
- **Q5 maintenance** (human-authored PRs only): for example, NixOS/nixpkgs merges 93% of closed PRs with a median of about 38 h open.
- **Q6 solo vs community:** of 11,754 repos with ≥ 20 events, 6,083 are solo, 1,877 small-team and 407 community (top contributor < 50%, ≥ 10 contributors).
- **Q7 recommendations** (205 users, hold out each user's latest 20% of stars):

| Method | Hit rate | Recall@10 | Precision@10 | Coverage |
|---|---|---|---|---|
| Mahout item CF (LLR) | **0.263** | **0.251** | **0.028** | **0.53** |
| Spark ALS (implicit, rank 16) | 0.259 | 0.237 | 0.026 | 0.24 |
| Popularity baseline | 0.044 | 0.041 | 0.004 | 0.01 |

- **Q8 viral model** (1 h features, 3 h label, time split, 67,585 new repos, 40 test positives):
  - Logistic regression: AUC-ROC 0.73, AUC-PR 0.0033 against a 0.0012 base rate (2.7× lift).
  - GBT: AUC-ROC 0.55.
  - This is a weak but real signal. The planned 24 h / 5-day setting on the large dataset is what the model is designed for.
- **Q9 live:** one GitHub hour (166–167k events) replayed at 60× in about 65 s through Kafka → Storm with 0 failed tuples. That produced 60 per-minute rows, 60 rolling top-10 windows and spike alerts. For example, ursacomputing/crossbow went from nothing to 104 events in a minute; hotspotlab/hourly hit 18 against a mean of 0.41 (z = 12.8).
- **Q10 clusters (Mahout k-means, k = 6):** push-only/solo, bot-driven push, PR + push collaboration, discussion-heavy, star + issue magnets.

## Engine benchmark (`bench/results/timings.csv`, `bench/charts/`)

Median seconds; the same 3 queries on the same Parquet tables.

| Size (clean Parquet) | Workers | MapReduce | Pig | Hive (MR) | Spark SQL | Impala |
|---|---|---|---|---|---|---|
| sample (0.028 GB) | 1 | 62–71 | 27–43 | 20–39 | 37–38 | 1.2–1.3 |
| sample (0.028 GB) | 2 | 53–59 | 27–38 | 15–28 | 20–37 | – |
| medium (0.130 GB) | 1 | 162–175 | 40–48 | 23–39 | 38–39 | 1.4–1.5 |

What the numbers show:
- **Start-up dominates at these sizes.** Hive, Pig and Spark barely move from sample to medium. Impala (long-running C++ daemons) answers in about 1.3 s; the JVM/YARN engines need 20–40 s before reading a byte.
- **MapReduce scales worst here: the small-files problem.** It launches one map per Parquet file, and the date/event-type partitions create many small files. Pig's combined splits avoid this (Q1 on medium: Pig 40 s vs MapReduce 162 s).
- **2 workers help modestly** (1.0–1.9×; Spark gains most). Both NodeManagers share the same laptop cores, so this is a ceiling of the hardware, not of the engines.
- **Hive vs Impala on the six dashboard queries:** 20–69 s against 1.2–1.6 s, with identical results.
