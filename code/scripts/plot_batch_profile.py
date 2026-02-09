#!/usr/bin/env python3
"""Generate charts from run_batch_updates profiler CSV output."""

import argparse
import csv
import os
import re
import sys
from collections import defaultdict

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except Exception as exc:
    print(
        "Failed to import matplotlib. Install it first, for example:\n"
        "  python3 -m pip install --user matplotlib",
        file=sys.stderr,
    )
    raise SystemExit(1) from exc


def parse_optional_float(value):
    if value is None:
        return None
    s = value.strip()
    if not s or s.upper() == "NA":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def parse_optional_int(value):
    if value is None:
        return None
    s = value.strip()
    if not s:
        return None
    try:
        return int(s)
    except ValueError:
        return None


def infer_op(row):
    op = (row.get("Op") or "").strip()
    if op:
        return op
    phase = (row.get("Phase") or "").strip()
    if phase.startswith("Insert"):
        return "Insert"
    if phase.startswith("Delete"):
        return "Delete"
    return "Unknown"


def infer_batch_index(row):
    idx = parse_optional_int(row.get("BatchIndex"))
    if idx is not None:
        return idx
    phase = (row.get("Phase") or "").strip()
    match = re.search(r"(\d+)$", phase)
    if match:
        return int(match.group(1))
    return None


def load_records(csv_path):
    records = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError("CSV appears empty or malformed (missing header).")
        for row in reader:
            records.append(
                {
                    "phase": row.get("Phase", ""),
                    "op": infer_op(row),
                    "batch_index": infer_batch_index(row),
                    "update_size": parse_optional_int(row.get("UpdateSize")),
                    "size_index": parse_optional_int(row.get("SizeIndex")),
                    "trial": parse_optional_int(row.get("Trial")),
                    "time": parse_optional_float(row.get("Time")),
                    "cycles": parse_optional_float(row.get("Cycles")),
                    "instructions": parse_optional_float(row.get("Instructions")),
                    "l1_misses": parse_optional_float(row.get("L1_Misses")),
                    "l2_misses": parse_optional_float(row.get("L2_Misses")),
                    "minor_faults": parse_optional_float(row.get("MinorFaults")),
                    "major_faults": parse_optional_float(row.get("MajorFaults")),
                    "rss_kb_delta": parse_optional_float(row.get("RSS_KB_Delta")),
                }
            )
    return records


def mean(values):
    if not values:
        return None
    return sum(values) / len(values)


def build_summary_by_size(records):
    grouped = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for rec in records:
        op = rec["op"]
        update_size = rec["update_size"]
        if update_size is None:
            continue

        for metric in (
            "time",
            "cycles",
            "instructions",
            "l1_misses",
            "l2_misses",
            "minor_faults",
            "major_faults",
            "rss_kb_delta",
        ):
            value = rec[metric]
            if value is not None:
                grouped[op][update_size][metric].append(value)

        if (
            rec["cycles"] is not None
            and rec["instructions"] is not None
            and rec["instructions"] > 0.0
        ):
            grouped[op][update_size]["cpi"].append(
                rec["cycles"] / rec["instructions"]
            )

    summary = defaultdict(dict)
    for op, by_size in grouped.items():
        for update_size, metrics in by_size.items():
            summary[op][update_size] = {}
            for metric, values in metrics.items():
                summary[op][update_size][metric] = mean(values)
    return summary


def write_summary_csv(summary, out_path):
    fields = [
        "Op",
        "UpdateSize",
        "AvgTime",
        "AvgCycles",
        "AvgInstructions",
        "AvgL1Misses",
        "AvgL2Misses",
        "AvgMinorFaults",
        "AvgMajorFaults",
        "AvgRSSDeltaKB",
        "AvgCPI",
    ]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for op in ("Insert", "Delete", "Unknown"):
            sizes = sorted(summary.get(op, {}).keys())
            for update_size in sizes:
                row = summary[op][update_size]
                writer.writerow(
                    {
                        "Op": op,
                        "UpdateSize": update_size,
                        "AvgTime": row.get("time", "NA"),
                        "AvgCycles": row.get("cycles", "NA"),
                        "AvgInstructions": row.get("instructions", "NA"),
                        "AvgL1Misses": row.get("l1_misses", "NA"),
                        "AvgL2Misses": row.get("l2_misses", "NA"),
                        "AvgMinorFaults": row.get("minor_faults", "NA"),
                        "AvgMajorFaults": row.get("major_faults", "NA"),
                        "AvgRSSDeltaKB": row.get("rss_kb_delta", "NA"),
                        "AvgCPI": row.get("cpi", "NA"),
                    }
                )


def plot_time_by_batch_index(records, out_path):
    colors = {"Insert": "#1f77b4", "Delete": "#d62728", "Unknown": "#7f7f7f"}
    fig, ax = plt.subplots(figsize=(11, 5))
    plotted = False

    for op in ("Insert", "Delete", "Unknown"):
        subset = [r for r in records if r["op"] == op and r["time"] is not None]
        if not subset:
            continue
        subset.sort(
            key=lambda r: (
                r["batch_index"] if r["batch_index"] is not None else sys.maxsize
            )
        )

        x = []
        y = []
        for idx, rec in enumerate(subset):
            x.append(rec["batch_index"] if rec["batch_index"] is not None else idx)
            y.append(rec["time"])
        ax.plot(
            x,
            y,
            label=op,
            color=colors[op],
            marker="o",
            markersize=2.5,
            linewidth=1.2,
        )
        plotted = True

    ax.set_title("Batch Latency by Batch Index")
    ax.set_xlabel("Batch Index")
    ax.set_ylabel("Time (s)")
    ax.grid(True, linestyle="--", alpha=0.3)

    if plotted:
        ax.legend()
    else:
        ax.text(0.5, 0.5, "No time data found", ha="center", va="center")

    fig.tight_layout()
    fig.savefig(out_path, dpi=180)
    plt.close(fig)


def plot_metrics_by_update_size(summary, out_path):
    colors = {"Insert": "#1f77b4", "Delete": "#d62728", "Unknown": "#7f7f7f"}
    metrics = [
        ("time", "Time (s)"),
        ("cycles", "Cycles"),
        ("instructions", "Instructions"),
        ("l1_misses", "L1 Misses"),
        ("l2_misses", "L2 / LLC Misses"),
        ("minor_faults", "Minor Faults"),
        ("major_faults", "Major Faults"),
        ("rss_kb_delta", "RSS Delta (KB)"),
        ("cpi", "CPI"),
    ]

    fig, axes = plt.subplots(3, 3, figsize=(16, 12), sharex=True)
    any_update_size_data = False
    legend_handles = None
    legend_labels = None

    for ax, (metric_key, title) in zip(axes.flatten(), metrics):
        metric_plotted = False
        for op in ("Insert", "Delete", "Unknown"):
            sizes = sorted(summary.get(op, {}).keys())
            xs = []
            ys = []
            for update_size in sizes:
                value = summary[op][update_size].get(metric_key)
                if value is None:
                    continue
                xs.append(update_size)
                ys.append(value)

            if not xs:
                continue
            any_update_size_data = True
            metric_plotted = True
            ax.plot(
                xs,
                ys,
                label=op,
                color=colors[op],
                marker="o",
                markersize=3,
                linewidth=1.2,
            )

        ax.set_title(title)
        ax.grid(True, linestyle="--", alpha=0.3)
        if metric_plotted:
            ax.set_xscale("log")
            if legend_handles is None:
                legend_handles, legend_labels = ax.get_legend_handles_labels()
        else:
            ax.text(
                0.5,
                0.5,
                "No data",
                ha="center",
                va="center",
                transform=ax.transAxes,
            )

    for ax in axes[-1, :]:
        ax.set_xlabel("Update Size (log scale)")

    if legend_handles:
        fig.legend(legend_handles, legend_labels, loc="upper center", ncol=3)

    if any_update_size_data:
        fig.suptitle("Mean Metrics by Update Size")
    else:
        fig.suptitle("Mean Metrics by Update Size (No UpdateSize column found)")

    fig.tight_layout(rect=(0, 0, 1, 0.96))
    fig.savefig(out_path, dpi=180)
    plt.close(fig)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Plot run_batch_updates profiling CSV output."
    )
    parser.add_argument(
        "--input",
        default="data/batch_updates/batch_profile.csv",
        help="Path to input CSV generated from run_batch_updates output.",
    )
    parser.add_argument(
        "--outdir",
        default="data/batch_updates/plots",
        help="Directory where plots and summary CSV will be written.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    os.makedirs(args.outdir, exist_ok=True)

    records = load_records(args.input)
    if not records:
        raise ValueError("No records were found in the input CSV.")

    summary = build_summary_by_size(records)
    time_plot = os.path.join(args.outdir, "time_by_batch_index.png")
    dashboard_plot = os.path.join(args.outdir, "metrics_by_update_size.png")
    summary_csv = os.path.join(args.outdir, "summary_by_update_size.csv")

    plot_time_by_batch_index(records, time_plot)
    plot_metrics_by_update_size(summary, dashboard_plot)
    write_summary_csv(summary, summary_csv)

    print(f"Wrote: {time_plot}")
    print(f"Wrote: {dashboard_plot}")
    print(f"Wrote: {summary_csv}")


if __name__ == "__main__":
    main()
