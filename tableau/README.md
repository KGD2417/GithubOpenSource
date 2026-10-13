# Tableau dashboard: GitHub Open Source Pulse

Tableau Desktop (free Tableau for Students licence) reads the same curated tables as Hive and Impala.
There are two ways to connect. Option A (ODBC) is live; option B (CSV extracts) needs no drivers.

## Option A: live ODBC to Impala (fast) or Hive

1. Download the drivers from **tableau.com/support/drivers**. You'll need a free Cloudera account.
   - **Cloudera ODBC Driver for Impala**: macOS universal `.dmg` (Apple Silicon and Intel)
   - **Cloudera ODBC Driver for Apache Hive**: macOS universal `.dmg`
   - Install both, then restart Tableau.
2. Start the stage you want to query:
   - Impala: `make stage STAGE=impala`, then `docker compose exec edge beeline -u "jdbc:hive2://impalad:21050/;auth=noSasl" -f impala/setup.sql` (refreshes metadata and stats)
   - Hive: `make stage STAGE=warehouse`
3. In Tableau, choose **Connect → To a Server**:

   | Engine | Connector | Server | Port | Type / Auth |
   |---|---|---|---|---|
   | Impala | **Impala** | `localhost` | `21050` | Authentication: *No Authentication* |
   | Hive | **Cloudera Hadoop** | `localhost` | `10000` | Type: *HiveServer2* · Auth: *Username* (`root`) · Transport: *Binary* |

4. Select schema `gh` and drag in the tables listed below. Use **Extract** mode for the published workbook, so it still opens when the cluster is off.

Impala is the better live source: dashboard queries come back in about 1 s, against 20–70 s on Hive-on-MapReduce (`bench/charts/hive_vs_impala.png`).

## Option B: CSV extracts (no drivers)

```
docker compose exec edge pulse export-extracts     # warehouse stage -> tableau/extracts/*.csv
docker compose exec edge pulse export-live         # streaming stage -> tableau/extracts/live_*.csv
```

In Tableau, choose **Connect → Text file** and pick `tableau/extracts/<table>.csv`. The column names match the Hive tables, so views built on CSV keep working when you swap the data source to ODBC later (*Data → Replace Data Source*).

## The views (`pulse.twb`)

| # | View | Table(s) | Build |
|---|---|---|---|
| 1 | Trending now | `trending_daily` | Bars: `repo_name` × `events`. Filters: `dt`, `event_type`; `rnk` ≤ 10 |
| 2 | Rising repos (window) | `trending_window` | Slope or bars of `growth_pct`; tooltip `first_day_events` → `last_day_events` |
| 3 | Languages & topics | `language_trends`, `topic_trends` | Lines of `events` by `dt` per `language`; treemap of `topic` sized by `stars` |
| 4 | Activity clock UTC/IST | `hourly_activity` | Heatmap of `hour_utc` × `weekday_utc` (`SUM(events)`). A parameter *Timezone* swaps in `hour_ist`/`weekday_ist`; weekend = weekday ≥ 6 |
| 5 | Bots vs humans | `hourly_activity`, `top_bots` | 100% stacked bars of `event_type` by `is_bot`; table of top bot accounts |
| 6 | Maintenance health | `repo_health`, `pr_lifecycle` | Box plot of `hours_open` (merged human PRs); scatter of `issue_close_ratio` vs `prs_closed` |
| 7 | Solo vs community | `repo_profile`, `repo_clusters` | Scatter of `top1_share` × `fork_star_ratio`, colour = `profile` (or k-means `cluster_label`) |
| 8 | Recommendations & viral watchlist | `similar_repos`, `user_recs`, `viral_predictions` | Parameter *Repo* (type-in) filters `similar_repos.repo_name`, plus a table of top `probability` new repos |
| (9) | Live pulse | `live_minute.csv`, `live_topk.csv` | Line of events/min per type; refresh the extract while the replay runs |

Tableau can't read HBase directly. The live view uses the CSV that `pulse export-live` writes from HBase. Re-run it (or `watch -n 30 ...`) and press *Refresh* in Tableau.
