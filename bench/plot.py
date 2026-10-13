#!/usr/bin/env python3
"""Benchmark + data-quality charts for the report (PLAN.md §10).   pulse bench-chart

Reads bench/results/timings.csv (bench/run_bench.sh), analytics_timings.csv (bench/time_query.sh) and
dq_funnel.csv (pulse dq-export); writes PNGs to bench/charts/. Charts whose input is missing are skipped.
  1 runtime_by_engine_<size>.png  median seconds per engine for Q1-Q3 (log axis: engines differ ~50x)
  2 scaling.png                   median seconds vs input GB, one small multiple per query
  3 speedup_workers.png           1 -> 2 NodeManagers speed-up per engine and query
  4 hive_vs_impala.png            the six dashboard queries, Hive vs Impala
  5 dq_funnel.png                 records surviving each stage, raw -> clean -> curated
Colours: validated categorical order (dataviz palette, light surface); every bar carries its value label
because three hues sit below 3:1 contrast on white. The CSVs are the table view.
"""
import csv
import statistics
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

ROOT = Path(__file__).resolve().parent
RES, OUT = ROOT / "results", ROOT / "charts"
SURFACE, INK, INK2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"
ENGINES = ["mr", "pig", "hive", "spark", "impala"]                     # fixed order = fixed colour
LABEL = {"mr": "MapReduce", "pig": "Pig", "hive": "Hive (MR)", "spark": "Spark SQL", "impala": "Impala"}
COLOR = dict(zip(ENGINES, ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"]))
QUERIES = {"q1": "Q1 scan + group", "q2": "Q2 top-10 stars", "q3": "Q3 join language"}


def style(ax, title, ylabel):
    ax.set_facecolor(SURFACE)
    ax.set_title(title, loc="left", color=INK, fontsize=12, fontweight="bold")
    ax.set_ylabel(ylabel, color=INK2)
    ax.tick_params(colors=INK2, length=0)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for s in ax.spines.values():
        s.set_visible(False)
    ax.spines["bottom"].set_visible(True)
    ax.spines["bottom"].set_color(INK2)


def log_seconds(ax):
    """Log axis labelled in plain seconds (1, 2, 5, 10, ...) instead of 10^n with unlabelled minor ticks."""
    from matplotlib.ticker import FixedLocator, FuncFormatter, NullLocator
    lo, hi = ax.get_ylim()
    ticks = [t for t in (0.5, 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000) if lo <= t <= hi * 1.2]
    ax.yaxis.set_major_locator(FixedLocator(ticks))
    ax.yaxis.set_minor_locator(NullLocator())
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))


def save(fig, name):
    OUT.mkdir(exist_ok=True)
    fig.patch.set_facecolor(SURFACE)
    if not fig.legends:                 # figure-level legends manage their own spacing
        fig.tight_layout()
    fig.savefig(OUT / name, dpi=160)
    plt.close(fig)
    print("wrote", OUT / name)


def read(name):
    p = RES / name
    return list(csv.DictReader(open(p))) if p.exists() else []


def medians(rows, key):
    groups = defaultdict(list)
    for r in rows:
        if float(r["seconds"]) > 0 and r.get("rows_out", "0") != "-1":
            groups[key(r)].append(float(r["seconds"]))
    return {k: statistics.median(v) for k, v in groups.items()}


def seconds(y):
    return f"{y:.0f}s" if y >= 10 else f"{y:.1f}s"


def grouped_bars(ax, cats, series, values, log=False, fmt=seconds):
    """values[(series, cat)] -> bar; 2px surface gaps between bars, value labels on top."""
    n = len(series)
    width = 0.8 / n
    for i, s in enumerate(series):
        xs, ys = [], []
        for j, c in enumerate(cats):
            if (s, c) in values:
                xs.append(j - 0.4 + width * (i + 0.5))
                ys.append(values[(s, c)])
        bars = ax.bar(xs, ys, width, color=COLOR.get(s, "#2a78d6"), label=LABEL.get(s, s),
                      edgecolor=SURFACE, linewidth=2)
        for b, y in zip(bars, ys):
            ax.annotate(fmt(y), (b.get_x() + b.get_width() / 2, y),
                        ha="center", va="bottom", fontsize=7, color=INK2, xytext=(0, 2), textcoords="offset points")
    ax.set_xticks(range(len(cats)))
    ax.set_xticklabels([QUERIES.get(c, c) for c in cats], color=INK)
    if log:
        ax.set_yscale("log")
        log_seconds(ax)
    ax.legend(frameon=False, ncol=n, loc="upper left", bbox_to_anchor=(0, -0.08), fontsize=8, labelcolor=INK)


def main():
    t = read("timings.csv")
    if t:
        for size in sorted({r["size"] for r in t}):
            rows = [r for r in t if r["size"] == size]
            best_workers = {}                                        # use the largest worker count per engine
            for r in rows:
                best_workers[r["engine"]] = max(best_workers.get(r["engine"], r["workers"]), r["workers"])
            m = medians([r for r in rows if r["workers"] == best_workers[r["engine"]]], lambda r: (r["engine"], r["query"]))
            engines = [e for e in ENGINES if any(k[0] == e for k in m)]
            gb = rows[0]["input_gb"]
            w = max((v for e, v in best_workers.items() if e != "impala"), default="?")
            fig, ax = plt.subplots(figsize=(9, 4.6))
            grouped_bars(ax, list(QUERIES), engines, m, log=True)
            style(ax, f"Runtime by engine — {size} ({gb} GB Parquet), {w} YARN workers / 1 impalad",
                  "median seconds (log scale)")
            save(fig, f"runtime_by_engine_{size}.png")

        # like-for-like: 1 YARN worker at every size (Impala has no worker axis)
        m = medians([r for r in t if r["workers"] in ("1", "NA")], lambda r: (r["engine"], r["query"], float(r["input_gb"])))
        if len({k[2] for k in m}) > 1:
            fig, axes = plt.subplots(1, 3, figsize=(12, 4), sharey=True)
            for ax, q in zip(axes, QUERIES):
                for e in ENGINES:
                    pts = sorted((gb, s) for (ee, qq, gb), s in m.items() if ee == e and qq == q)
                    if pts:
                        ax.plot([p[0] for p in pts], [p[1] for p in pts], color=COLOR[e], linewidth=2,
                                marker="o", markersize=8, markeredgecolor=SURFACE, markeredgewidth=2, label=LABEL[e])
                ax.set_yscale("log")
                log_seconds(ax)
                style(ax, QUERIES[q] + (" — 1 YARN worker" if q == "q1" else ""), "median seconds (log)" if q == "q1" else "")
                ax.set_xlabel("input GB (clean Parquet)", color=INK2)
            handles, labels = axes[0].get_legend_handles_labels()
            fig.legend(handles, labels, frameon=False, fontsize=9, labelcolor=INK, ncol=len(labels),
                       loc="lower center", bbox_to_anchor=(0.5, -0.02))
            fig.subplots_adjust(bottom=0.24)
            save(fig, "scaling.png")

        m = medians([r for r in t if r["workers"] in ("1", "2")], lambda r: (r["engine"], r["query"], r["size"], r["workers"]))
        speed = {}
        for (e, q, size, w), s in m.items():
            if w == "2" and (e, q, size, "1") in m:
                speed[(e, q)] = max(speed.get((e, q), 0), m[(e, q, size, "1")] / s)
        if speed:
            fig, ax = plt.subplots(figsize=(9, 4.2))
            engines = [e for e in ENGINES if any(k[0] == e for k in speed)]
            grouped_bars(ax, list(QUERIES), engines, speed, fmt=lambda y: f"{y:.2f}x")
            ax.axhline(1, color=INK2, linewidth=1, linestyle="--")
            style(ax, "Speed-up from 1 to 2 NodeManagers (largest size measured)", "speed-up (x)")
            save(fig, "speedup_workers.png")

    a = read("analytics_timings.csv")
    if a:
        m = medians(a, lambda r: (r["engine"], r["query"]))
        qs = sorted({k[1] for k in m})
        fig, ax = plt.subplots(figsize=(10, 4.4))
        n = 2
        for i, e in enumerate(["hive", "impala"]):
            xs = [j - 0.4 + 0.4 * (i + 0.5) for j, q in enumerate(qs) if (e, q) in m]
            ys = [m[(e, q)] for q in qs if (e, q) in m]
            bars = ax.bar(xs, ys, 0.4, color=COLOR[e], label=LABEL[e], edgecolor=SURFACE, linewidth=2)
            for b, y in zip(bars, ys):
                ax.annotate(f"{y:.1f}s", (b.get_x() + b.get_width() / 2, y), ha="center", va="bottom",
                            fontsize=7, color=INK2, xytext=(0, 2), textcoords="offset points")
        ax.set_xticks(range(len(qs)))
        ax.set_xticklabels([q.replace("_", " ") for q in qs], color=INK, fontsize=8)
        ax.set_yscale("log")
        log_seconds(ax)
        ax.legend(frameon=False, ncol=n, fontsize=8, labelcolor=INK)
        style(ax, "Dashboard queries on the same tables: Hive vs Impala", "median seconds (log scale)")
        save(fig, "hive_vs_impala.png")

    f = read("dq_funnel.csv")
    if f:
        stages = [r["stage"] for r in f]
        vals = [int(r["records"]) for r in f]
        fig, ax = plt.subplots(figsize=(8, 0.6 * len(f) + 1.2))
        bars = ax.barh(range(len(f)), vals, color="#2a78d6", edgecolor=SURFACE, linewidth=2, height=0.6)
        for b, v in zip(bars, vals):
            ax.annotate(f"{v:,}", (v, b.get_y() + b.get_height() / 2), va="center", xytext=(4, 0),
                        textcoords="offset points", fontsize=8, color=INK2)
        ax.set_yticks(range(len(f)))
        ax.set_yticklabels(stages, color=INK)
        ax.invert_yaxis()
        style(ax, "Records at each pipeline stage", "")
        from matplotlib.ticker import FuncFormatter
        ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v / 1e6:g}M" if v else "0"))
        ax.grid(axis="x", color=GRID)
        ax.grid(axis="y", visible=False)
        save(fig, "dq_funnel.png")


if __name__ == "__main__":
    main()
