#!/usr/bin/env python3
"""
Extract and plot Riak fan-out experiment metrics.

Expected directory layout (example):
experiments/riak/
  k_001/
    baseline/
      run_01/
        fanout_requests.jsonl
        locust_fanout_stats.csv
        ...
      run_02/...
      run_03/...
    latency_100ms_j50ms/
      run_01/...
    packetloss_1percent_25/
      run_01/...

Outputs:
  <out_dir>/run_metrics.csv
  <out_dir>/agg_metrics.csv
  <out_dir>/outcome_breakdown.csv
  <out_dir>/fig_*.png and fig_*.pdf
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# -----------------------------
# Condition labels for plots/legends
# -----------------------------

CONDITION_LABELS = {
    "baseline": "Baseline",
    "latency_100ms_j50ms": "Latency 100ms, jitter 30ms",
    "packetloss_1percent_25": "Packet loss 1% / 25",
}


# -----------------------------
# Helpers: directory parsing
# -----------------------------

K_DIR_RE = re.compile(r"^k_(\d+)$", re.IGNORECASE)
RUN_DIR_RE = re.compile(r"^run_(\d+)$", re.IGNORECASE)

def parse_k_from_dirname(name: str) -> Optional[int]:
    m = K_DIR_RE.match(name)
    if not m:
        return None
    return int(m.group(1))

def is_run_dir(name: str) -> bool:
    return RUN_DIR_RE.match(name) is not None

def condition_label_from_dirname(name: str) -> str:
    return CONDITION_LABELS.get(name, name)

def safe_float(x) -> Optional[float]:
    try:
        if pd.isna(x):
            return None
        return float(x)
    except Exception:
        return None


# -----------------------------
# Online stats for correlation
# -----------------------------

@dataclass
class OnlineCov:
    n: int = 0
    mean_x: float = 0.0
    mean_y: float = 0.0
    C: float = 0.0   # covariance accumulator
    M2x: float = 0.0 # variance accumulator for x
    M2y: float = 0.0 # variance accumulator for y

    def add(self, x: float, y: float) -> None:
        self.n += 1
        dx = x - self.mean_x
        dy = y - self.mean_y
        self.mean_x += dx / self.n
        self.mean_y += dy / self.n
        # Update covariance and variances (Welford-style)
        self.C += dx * (y - self.mean_y)
        self.M2x += dx * (x - self.mean_x)
        self.M2y += dy * (y - self.mean_y)

    def corr(self) -> Optional[float]:
        if self.n < 2:
            return None
        denom = math.sqrt(self.M2x * self.M2y)
        if denom == 0:
            return None
        # Sample covariance uses (n-1); the factor cancels in correlation
        return self.C / denom


# -----------------------------
# Extractors
# -----------------------------

@dataclass
class RunMetrics:
    k: int
    condition: str
    run: str
    path: str

    # Availability from jsonl
    total_requests: int
    ok_count: int
    availability: float

    # Locust stats from CSV
    request_count_locust: Optional[int]
    failure_count_locust: Optional[int]
    rps: Optional[float]
    fps: Optional[float]
    p50_ms: Optional[float]
    p95_ms: Optional[float]
    p99_ms: Optional[float]
    avg_ms: Optional[float]

    # Mechanism
    mean_latency_ms_jsonl: Optional[float]
    mean_max_subread_ms: Optional[float]
    mean_mean_subread_ms: Optional[float]
    corr_latency_vs_maxsubread: Optional[float]

def read_locust_stats(stats_csv: Path) -> Dict[str, Optional[float]]:
    """
    Locust stats CSV typically has 2 rows: one per request name (e.g., FANOUT K25),
    and an Aggregated row. We prefer Type == 'FANOUT' if present, else the non-aggregated row.
    """
    df = pd.read_csv(stats_csv)
    if df.empty:
        return {}

    # Prefer row(s) where Type == 'FANOUT'
    if "Type" in df.columns:
        fanout_rows = df[df["Type"].astype(str).str.upper() == "FANOUT"]
    else:
        fanout_rows = pd.DataFrame()

    chosen = None
    if not fanout_rows.empty:
        chosen = fanout_rows.iloc[0]
    else:
        # otherwise pick any row that isn't Aggregated
        if "Name" in df.columns:
            non_agg = df[df["Name"].astype(str).str.lower() != "aggregated"]
            chosen = non_agg.iloc[0] if not non_agg.empty else df.iloc[0]
        else:
            chosen = df.iloc[0]

    def col(name: str) -> Optional[float]:
        return safe_float(chosen.get(name))

    out = {
        "request_count_locust": safe_float(chosen.get("Request Count")),
        "failure_count_locust": safe_float(chosen.get("Failure Count")),
        "rps": col("Requests/s"),
        "fps": col("Failures/s"),
        "p50_ms": col("50%") if "50%" in df.columns else col("Median Response Time"),
        "p95_ms": col("95%"),
        "p99_ms": col("99%"),
        "avg_ms": col("Average Response Time"),
    }
    # Cast counts to int if present
    if out["request_count_locust"] is not None:
        out["request_count_locust"] = int(out["request_count_locust"])
    if out["failure_count_locust"] is not None:
        out["failure_count_locust"] = int(out["failure_count_locust"])
    return out

def stream_fanout_jsonl(jsonl_path: Path) -> Tuple[Dict[str, int], Dict[str, Optional[float]]]:
    """
    Stream fanout_requests.jsonl and compute:
      - total_requests, ok_count
      - means: latency_ms, max_subread_ms, mean_subread_ms
      - correlation: latency_ms vs max_subread_ms
      - outcome breakdown counts
    """
    total = 0
    ok = 0

    sum_latency = 0.0
    sum_maxsub = 0.0
    sum_meansub = 0.0
    n_latency = 0
    n_maxsub = 0
    n_meansub = 0

    cov = OnlineCov()
    outcome_counts: Dict[str, int] = {}

    with jsonl_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            total += 1

            outcome = obj.get("outcome", "unknown")
            outcome_counts[outcome] = outcome_counts.get(outcome, 0) + 1
            if outcome == "ok":
                ok += 1

            lat = obj.get("latency_ms")
            mx = obj.get("max_subread_ms")
            mn = obj.get("mean_subread_ms")

            if isinstance(lat, (int, float)) and math.isfinite(lat):
                sum_latency += float(lat)
                n_latency += 1
            if isinstance(mx, (int, float)) and math.isfinite(mx):
                sum_maxsub += float(mx)
                n_maxsub += 1
            if isinstance(mn, (int, float)) and math.isfinite(mn):
                sum_meansub += float(mn)
                n_meansub += 1

            if (
                isinstance(lat, (int, float)) and math.isfinite(lat) and
                isinstance(mx, (int, float)) and math.isfinite(mx)
            ):
                cov.add(float(lat), float(mx))

    availability = (ok / total) if total > 0 else float("nan")

    means = {
        "mean_latency_ms_jsonl": (sum_latency / n_latency) if n_latency else None,
        "mean_max_subread_ms": (sum_maxsub / n_maxsub) if n_maxsub else None,
        "mean_mean_subread_ms": (sum_meansub / n_meansub) if n_meansub else None,
        "corr_latency_vs_maxsubread": cov.corr(),
    }
    return outcome_counts, means | {"total_requests": total, "ok_count": ok, "availability": availability}


# -----------------------------
# Main walk + aggregation
# -----------------------------

def find_runs(root: Path) -> List[Tuple[int, str, Path]]:
    """
    Returns list of (k, condition_label, run_dir_path).
    """
    runs: List[Tuple[int, str, Path]] = []
    for k_dir in sorted(root.iterdir()):
        if not k_dir.is_dir():
            continue
        k = parse_k_from_dirname(k_dir.name)
        if k is None:
            continue

        for cond_dir in sorted(k_dir.iterdir()):
            if not cond_dir.is_dir():
                continue
            cond_label = condition_label_from_dirname(cond_dir.name)

            # Condition folder may have run_XX subfolders
            for run_dir in sorted(cond_dir.iterdir()):
                if not run_dir.is_dir():
                    continue
                if not is_run_dir(run_dir.name):
                    continue
                runs.append((k, cond_label, run_dir))

    return runs

def extract_run_metrics(k: int, condition: str, run_dir: Path) -> Tuple[RunMetrics, Dict[str, int]]:
    jsonl = run_dir / "fanout_requests.jsonl"
    stats_csv = run_dir / "locust_fanout_stats.csv"

    if not jsonl.exists():
        raise FileNotFoundError(f"Missing {jsonl}")
    if not stats_csv.exists():
        raise FileNotFoundError(f"Missing {stats_csv}")

    outcome_counts, jsonl_metrics = stream_fanout_jsonl(jsonl)
    locust = read_locust_stats(stats_csv)

    rm = RunMetrics(
        k=k,
        condition=condition,
        run=run_dir.name,
        path=str(run_dir),

        total_requests=int(jsonl_metrics["total_requests"]),
        ok_count=int(jsonl_metrics["ok_count"]),
        availability=float(jsonl_metrics["availability"]),

        request_count_locust=locust.get("request_count_locust"),
        failure_count_locust=locust.get("failure_count_locust"),
        rps=locust.get("rps"),
        fps=locust.get("fps"),
        p50_ms=locust.get("p50_ms"),
        p95_ms=locust.get("p95_ms"),
        p99_ms=locust.get("p99_ms"),
        avg_ms=locust.get("avg_ms"),

        mean_latency_ms_jsonl=jsonl_metrics.get("mean_latency_ms_jsonl"),
        mean_max_subread_ms=jsonl_metrics.get("mean_max_subread_ms"),
        mean_mean_subread_ms=jsonl_metrics.get("mean_mean_subread_ms"),
        corr_latency_vs_maxsubread=jsonl_metrics.get("corr_latency_vs_maxsubread"),
    )
    return rm, outcome_counts

def aggregate_runs(run_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate across runs per (k, condition): mean and std.
    """
    metric_cols = [
        "availability",
        "rps",
        "fps",
        "p50_ms",
        "p95_ms",
        "p99_ms",
        "avg_ms",
        "mean_latency_ms_jsonl",
        "mean_max_subread_ms",
        "mean_mean_subread_ms",
        "corr_latency_vs_maxsubread",
    ]

    grouped = run_df.groupby(["condition", "k"], as_index=False)

    mean_df = grouped[metric_cols].mean(numeric_only=True).rename(
        columns={c: f"{c}_mean" for c in metric_cols}
    )
    std_df = grouped[metric_cols].std(numeric_only=True, ddof=1).rename(
        columns={c: f"{c}_std" for c in metric_cols}
    )

    out = pd.merge(mean_df, std_df, on=["condition", "k"], how="left")
    return out.sort_values(["condition", "k"])


# -----------------------------
# Plotting
# -----------------------------

def save_fig(fig, out_dir: Path, stem: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(out_dir / f"{stem}.png", dpi=300)
    fig.savefig(out_dir / f"{stem}.pdf")
    plt.close(fig)

def plot_lines_by_condition(
    agg: pd.DataFrame,
    y_col: str,
    ylabel: str,
    title: str,
    out_dir: Path,
    stem: str,
    yscale: Optional[str] = None,
) -> None:
    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)

    for cond, sub in agg.groupby("condition"):
        sub = sub.sort_values("k")
        ax.plot(sub["k"], sub[y_col], marker="o", label=cond)

    ax.set_xlabel("Fan-out K")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    if yscale:
        ax.set_yscale(yscale)
    ax.grid(True, which="both", linestyle=":", linewidth=0.7)
    ax.legend()
    save_fig(fig, out_dir, stem)

def plot_p50_p99(agg: pd.DataFrame, out_dir: Path) -> None:
    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)

    for cond, sub in agg.groupby("condition"):
        sub = sub.sort_values("k")
        ax.plot(sub["k"], sub["p50_ms_mean"], marker="o", label=f"{cond} p50")
        ax.plot(sub["k"], sub["p99_ms_mean"], marker="o", linestyle="--", label=f"{cond} p99")

    ax.set_xlabel("Fan-out K")
    ax.set_ylabel("Latency (ms)")
    ax.set_title("Median (p50) vs Tail (p99) Logical Latency")
    ax.grid(True, which="both", linestyle=":", linewidth=0.7)
    ax.legend()
    save_fig(fig, out_dir, "fig_p50_vs_p99_latency_vs_k")

def plot_mechanism_scatter(run_df: pd.DataFrame, out_dir: Path) -> None:
    """
    Uses per-run means: mean logical latency vs mean max subread latency.
    """
    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)

    for cond, sub in run_df.groupby("condition"):
        ax.scatter(sub["mean_max_subread_ms"], sub["mean_latency_ms_jsonl"], label=cond)

    ax.set_xlabel("Mean max_subread_ms (ms) [per run]")
    ax.set_ylabel("Mean logical latency_ms (ms) [per run]")
    ax.set_title("Mechanism Evidence: Logical Latency Tracks Slowest Subread")
    ax.grid(True, which="both", linestyle=":", linewidth=0.7)
    ax.legend()
    save_fig(fig, out_dir, "fig_mechanism_latency_vs_maxsubread")

def plot_corr_vs_k(agg: pd.DataFrame, out_dir: Path) -> None:
    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)

    for cond, sub in agg.groupby("condition"):
        sub = sub.sort_values("k")
        ax.plot(sub["k"], sub["corr_latency_vs_maxsubread_mean"], marker="o", label=cond)

    ax.set_xlabel("Fan-out K")
    ax.set_ylabel("Corr(latency_ms, max_subread_ms)")
    ax.set_title("Correlation Between Logical Latency and Slowest Subread vs K")
    ax.set_ylim(-1.05, 1.05)
    ax.grid(True, which="both", linestyle=":", linewidth=0.7)
    ax.legend()
    save_fig(fig, out_dir, "fig_corr_latency_vs_maxsubread_vs_k")


# -----------------------------
# Entry point
# -----------------------------

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True, help="Root directory containing k_*/ condition / run_* folders")
    ap.add_argument("--out", required=True, help="Output directory for CSVs and figures")
    args = ap.parse_args()

    root = Path(args.root).expanduser().resolve()
    out_dir = Path(args.out).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    runs = find_runs(root)
    if not runs:
        raise SystemExit(f"No runs found under {root}. Expected k_*/<condition>/run_*/")

    run_rows: List[Dict] = []
    outcome_rows: List[Dict] = []

    for k, condition, run_dir in runs:
        rm, outcome_counts = extract_run_metrics(k, condition, run_dir)
        run_rows.append(asdict(rm))

        for outcome, cnt in outcome_counts.items():
            outcome_rows.append({
                "k": k,
                "condition": condition,
                "run": run_dir.name,
                "outcome": outcome,
                "count": cnt,
                "path": str(run_dir),
            })

    run_df = pd.DataFrame(run_rows).sort_values(["condition", "k", "run"])
    outcome_df = pd.DataFrame(outcome_rows).sort_values(["condition", "k", "run", "outcome"])
    agg_df = aggregate_runs(run_df)

    run_df.to_csv(out_dir / "run_metrics.csv", index=False)
    agg_df.to_csv(out_dir / "agg_metrics.csv", index=False)
    outcome_df.to_csv(out_dir / "outcome_breakdown.csv", index=False)

    # Core figures
    plot_lines_by_condition(
        agg_df, "availability_mean",
        ylabel="Effective availability (ok / total)",
        title="Effective Availability vs Fan-out K",
        out_dir=out_dir,
        stem="fig_availability_vs_k",
    )

    plot_lines_by_condition(
        agg_df, "p99_ms_mean",
        ylabel="p99 logical latency (ms)",
        title="Tail Latency (p99) vs Fan-out K",
        out_dir=out_dir,
        stem="fig_p99_latency_vs_k",
        yscale=None,  # set to "log" if you want log scaling
    )

    plot_p50_p99(agg_df, out_dir)

    plot_lines_by_condition(
        agg_df, "rps_mean",
        ylabel="Throughput (requests/s)",
        title="Throughput vs Fan-out K",
        out_dir=out_dir,
        stem="fig_throughput_vs_k",
    )

    plot_mechanism_scatter(run_df, out_dir)

    plot_corr_vs_k(agg_df, out_dir)

    print(f"Wrote:\n  {out_dir / 'run_metrics.csv'}\n  {out_dir / 'agg_metrics.csv'}\n  {out_dir / 'outcome_breakdown.csv'}")
    print(f"Figures saved under: {out_dir}")

if __name__ == "__main__":
    main()