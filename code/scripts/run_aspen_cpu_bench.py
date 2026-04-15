#!/usr/bin/env python3

import argparse
import csv
import json
import os
from pathlib import Path
import platform
import re
import shutil
import statistics
import subprocess
import sys
from datetime import datetime, timezone

from config_loader import coerce_bool, coerce_int, coerce_int_list, load_named_json_config


RAW_COLUMNS = [
    "run_id",
    "timestamp",
    "dataset",
    "num_vertices",
    "num_edges_initial",
    "batch_id",
    "batch_size",
    "operation_type",
    "update_ms",
    "bfs_ms",
    "pr_ms",
    "update_cycles",
    "update_instructions",
    "update_cache_refs",
    "update_cache_misses",
    "update_branch_misses",
    "bfs_cycles",
    "bfs_instructions",
    "bfs_cache_refs",
    "bfs_cache_misses",
    "bfs_branch_misses",
    "update_energy_j",
    "bfs_energy_j",
    # update-phase breakdown (empty unless --breakdown passed to binary)
    "update_sort_ms",
    "update_preprocess_ms",
    "update_edge_struct_ms",
    "update_tree_merge_ms",
    # stall counters (empty unless --collect-stall passed and PMU supports them)
    "update_stall_frontend",
    "update_stall_backend",
    "bfs_stall_frontend",
    "bfs_stall_backend",
    "seed",
    "trial",
    "threads",
    "notes",
]

AGG_COLUMNS = [
    "dataset",
    "batch_size",
    "operation_type",
    "metric_name",
    "mean",
    "stddev",
    "min",
    "max",
    "trials",
]

METRICS = ["update_ms", "bfs_ms",
           "update_cycles", "update_instructions", "update_cache_refs", "update_cache_misses", "update_branch_misses",
           "bfs_cycles", "bfs_instructions", "bfs_cache_refs", "bfs_cache_misses", "bfs_branch_misses",
           "update_energy_j", "bfs_energy_j"]


def fail(message: str) -> None:
    print(f"run_aspen_cpu_bench.py: {message}", file=sys.stderr)
    raise SystemExit(1)


def run_command(cmd, cwd: Path, env=None, capture_output=True, check=True):
    return subprocess.run(
        cmd,
        cwd=str(cwd),
        env=env,
        text=True,
        capture_output=capture_output,
        check=check,
    )


def require_tool(name: str) -> None:
    if shutil.which(name) is None:
        fail(f"Required dependency '{name}' was not found in PATH.")


def read_perf_paranoid() -> int | None:
    paranoid_path = Path("/proc/sys/kernel/perf_event_paranoid")
    if not paranoid_path.exists():
        return None
    try:
        return int(paranoid_path.read_text().strip())
    except Exception:
        return None


def ensure_perf_access(enabled: bool) -> None:
    if not enabled:
        return
    paranoid = read_perf_paranoid()
    if paranoid is not None and paranoid > 1:
        fail(
            "Perf collection was requested, but /proc/sys/kernel/perf_event_paranoid="
            f"{paranoid}. Lower it or grant CAP_PERFMON/CAP_SYS_ADMIN before running the default profile."
        )


def repo_roots() -> tuple[Path, Path]:
    script_path = Path(__file__).resolve()
    code_root = script_path.parents[1]
    repo_root = code_root.parent
    return repo_root, code_root


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Aspen CPU-only dynamic update + BFS benchmarks.")
    parser.add_argument("--config", default=None, help="JSON config file loaded before CLI overrides.")
    parser.add_argument("--config-key", default=None, help="Optional top-level key inside --config.")
    parser.add_argument("--profile", choices=["default", "smoke", "custom"], default=None)
    parser.add_argument("--dataset", choices=["rmat", "snap", "adj"], default=None)
    parser.add_argument("--dataset-name", default=None)
    parser.add_argument("--graph-path", default=None, help="Use an existing .adj graph directly.")
    parser.add_argument("--snap-input", default=None, help="SNAP edge-list input to convert with snap_to_adj.")
    parser.add_argument("--rmat-scale", type=int, default=None)
    parser.add_argument("--rmat-edge-factor", type=int, default=None)
    parser.add_argument("--mmap", action="store_true", default=None, help="Pass -m to Aspen graph loading.")
    parser.add_argument("--compressed", action="store_true", default=None, help="Pass -c to Aspen graph loading.")
    parser.add_argument("--nparts", type=int, default=None, help="Pass -nparts to Aspen graph loading.")
    parser.add_argument("--batch-sizes", default=None, help="Comma-separated logical batch sizes.")
    parser.add_argument("--trials", type=int, default=None)
    parser.add_argument("--warmup-trials", type=int, default=None)
    parser.add_argument("--threads", type=int, default=None)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Override the run directory. Defaults to code/data/aspen_dynamic_bench/<run_id>.",
    )
    parser.add_argument(
        "--make-plots", action="store_true", default=None,
        help="Generate matplotlib plots after the run finishes."
    )
    parser.add_argument(
        "--plot-only",
        default=None,
        help="Generate plots from an existing benchmark run directory that contains aggregate.csv.",
    )
    parser.add_argument(
        "--plot-dir",
        default=None,
        help="Override the plot output directory. Defaults to <run_dir>/plots.",
    )
    parser.add_argument("--skip-perf", action="store_true", default=None, help="Disable Linux perf counters.")
    parser.add_argument("--sample-multiplier", type=int, default=None)
    return parser.parse_args()


def profile_defaults(profile: str) -> dict:
    if profile == "default":
        return {
            "dataset": "rmat",
            "dataset_name": "rmat_scale18_ef8",
            "rmat_scale": 18,
            "rmat_edge_factor": 8,
            "batch_sizes": [128, 1024, 8192],
            "trials": 5,
            "warmup_trials": 1,
            "threads": max(1, os.cpu_count() or 1),
        }
    if profile == "smoke":
        return {
            "dataset": "rmat",
            "dataset_name": "rmat_scale12_ef4_smoke",
            "rmat_scale": 12,
            "rmat_edge_factor": 4,
            "batch_sizes": [32],
            "trials": 1,
            "warmup_trials": 1,
            "threads": min(4, max(1, os.cpu_count() or 1)),
        }
    return {
        "dataset": "rmat",
        "dataset_name": "rmat_custom",
        "rmat_scale": 18,
        "rmat_edge_factor": 8,
        "batch_sizes": [1024],
        "trials": 3,
        "warmup_trials": 1,
        "threads": max(1, os.cpu_count() or 1),
    }


def resolve_config(args: argparse.Namespace, repo_root: Path) -> dict:
    file_cfg, config_path = load_named_json_config(args.config, args.config_key, repo_root, fail)

    profile = args.profile if args.profile is not None else file_cfg.get("profile")
    if profile is None:
        profile = "default"

    defaults = profile_defaults(profile)
    config = dict(defaults)
    config.update({
        "profile": profile,
        "seed": 1,
        "collect_perf": True,
        "sample_multiplier": 8,
        "graph_path": None,
        "snap_input": None,
        "mmap": False,
        "compressed": False,
        "nparts": 1,
        "output_dir": None,
        "make_plots": False,
        "plot_only": None,
        "plot_dir": None,
        "config_file": str(config_path) if config_path else None,
        "config_key": args.config_key,
    })

    for key in ("dataset", "dataset_name", "graph_path", "snap_input", "output_dir", "plot_only", "plot_dir"):
        if key in file_cfg and file_cfg[key] is not None:
            config[key] = file_cfg[key]

    for key in ("rmat_scale", "rmat_edge_factor", "trials", "warmup_trials",
                "threads", "seed", "nparts", "sample_multiplier"):
        if key in file_cfg and file_cfg[key] is not None:
            config[key] = coerce_int(file_cfg[key], key, fail)

    if "batch_sizes" in file_cfg and file_cfg["batch_sizes"] is not None:
        config["batch_sizes"] = coerce_int_list(file_cfg["batch_sizes"], "batch_sizes", fail)

    for key in ("mmap", "compressed", "make_plots"):
        if key in file_cfg and file_cfg[key] is not None:
            config[key] = coerce_bool(file_cfg[key], key, fail)

    if "collect_perf" in file_cfg and file_cfg["collect_perf"] is not None:
        config["collect_perf"] = coerce_bool(file_cfg["collect_perf"], "collect_perf", fail)
    elif "skip_perf" in file_cfg and file_cfg["skip_perf"] is not None:
        config["collect_perf"] = not coerce_bool(file_cfg["skip_perf"], "skip_perf", fail)

    if args.dataset is not None:
        config["dataset"] = args.dataset
    if args.dataset_name is not None:
        config["dataset_name"] = args.dataset_name
    if args.graph_path is not None:
        config["graph_path"] = args.graph_path
    if args.snap_input is not None:
        config["snap_input"] = args.snap_input
    if args.rmat_scale is not None:
        config["rmat_scale"] = args.rmat_scale
    if args.rmat_edge_factor is not None:
        config["rmat_edge_factor"] = args.rmat_edge_factor
    if args.batch_sizes is not None:
        config["batch_sizes"] = coerce_int_list(args.batch_sizes, "batch_sizes", fail)
    if args.trials is not None:
        config["trials"] = args.trials
    if args.warmup_trials is not None:
        config["warmup_trials"] = args.warmup_trials
    if args.threads is not None:
        config["threads"] = args.threads
    if args.seed is not None:
        config["seed"] = args.seed
    if args.output_dir is not None:
        config["output_dir"] = args.output_dir
    if args.make_plots is True:
        config["make_plots"] = True
    if args.plot_only is not None:
        config["plot_only"] = args.plot_only
    if args.plot_dir is not None:
        config["plot_dir"] = args.plot_dir
    if args.skip_perf is True:
        config["collect_perf"] = False
    if args.sample_multiplier is not None:
        config["sample_multiplier"] = args.sample_multiplier
    if args.mmap is True:
        config["mmap"] = True
    if args.compressed is True:
        config["compressed"] = True
    if args.nparts is not None:
        config["nparts"] = args.nparts

    config["sample_multiplier"] = max(2, config["sample_multiplier"])
    return config


def ensure_paths(repo_root: Path, code_root: Path, config: dict) -> tuple[str, Path]:
    run_id = datetime.now(timezone.utc).strftime("run_%Y%m%dT%H%M%SZ")
    output_dir_arg = config.get("output_dir")
    if output_dir_arg:
        output_dir = Path(output_dir_arg)
        if not output_dir.is_absolute():
            output_dir = repo_root / output_dir
    else:
        output_dir = code_root / "data" / "aspen_dynamic_bench" / run_id
    try:
        output_dir.mkdir(parents=True, exist_ok=False)
        (output_dir / "logs").mkdir(exist_ok=True)
    except OSError as exc:
        fail(
            f"Could not create output directory '{output_dir}': {exc}. "
            "Pass --output-dir to choose a writable location."
        )
    return run_id, output_dir


def build_binaries(code_root: Path) -> None:
    cmd = ["make", "snap_to_adj", "rmat_to_adj", "run_dynamic_cpu_benchmark"]
    result = run_command(cmd, cwd=code_root, capture_output=True, check=False)
    if result.returncode != 0:
        fail("Build failed.\n" + result.stdout + result.stderr)


def prepare_rmat_dataset(code_root: Path, config: dict) -> Path:
    dataset_name = config["dataset_name"]
    output_path = code_root / "inputs" / f"{dataset_name}.adj"
    if output_path.exists():
        return output_path
    cmd = [
        str(code_root / "rmat_to_adj"),
        "--scale",
        str(config["rmat_scale"]),
        "--edge-factor",
        str(config["rmat_edge_factor"]),
        "--seed",
        str(config["seed"]),
        "--output",
        str(output_path),
    ]
    result = run_command(cmd, cwd=code_root, capture_output=True, check=False)
    if result.returncode != 0:
        fail("RMAT dataset generation failed.\n" + result.stdout + result.stderr)
    return output_path


def prepare_snap_dataset(repo_root: Path, code_root: Path, config: dict) -> Path:
    snap_input = config.get("snap_input")
    if not snap_input:
        fail("--snap-input is required when --dataset snap is selected.")
    snap_path = Path(snap_input)
    if not snap_path.is_absolute():
        snap_path = repo_root / snap_path
    if not snap_path.exists():
        fail(f"SNAP input does not exist: {snap_path}")
    dataset_name = config["dataset_name"]
    output_path = code_root / "inputs" / f"{dataset_name}.adj"
    if output_path.exists():
        return output_path
    cmd = [str(code_root / "snap_to_adj"), str(snap_path), str(output_path)]
    result = run_command(cmd, cwd=code_root, capture_output=True, check=False)
    if result.returncode != 0:
        fail("SNAP conversion failed.\n" + result.stdout + result.stderr)
    return output_path


def prepare_adj_dataset(repo_root: Path, code_root: Path, config: dict) -> Path:
    graph_path = config.get("graph_path")
    if not graph_path:
        fail("--graph-path is required when --dataset adj is selected.")
    path = Path(graph_path)
    if not path.is_absolute():
        path = repo_root / path
    if not path.exists():
        fail(f"Adjacency graph does not exist: {path}")
    return path


def prepare_dataset(repo_root: Path, code_root: Path, config: dict) -> Path:
    dataset = config["dataset"]
    if dataset == "rmat":
        return prepare_rmat_dataset(code_root, config)
    if dataset == "snap":
        return prepare_snap_dataset(repo_root, code_root, config)
    if dataset == "adj":
        return prepare_adj_dataset(repo_root, code_root, config)
    fail(f"Unsupported dataset mode: {dataset}")


def collect_metadata(code_root: Path) -> dict:
    def safe_output(cmd):
        result = run_command(cmd, cwd=code_root, capture_output=True, check=False)
        if result.returncode != 0:
            return ""
        return result.stdout.strip()

    cpu_model = ""
    lscpu_output = safe_output(["lscpu"])
    for line in lscpu_output.splitlines():
        if line.startswith("Model name:"):
            cpu_model = line.split(":", 1)[1].strip()
            break

    compiler_version = safe_output(["g++", "--version"])
    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "platform": platform.platform(),
        "kernel": safe_output(["uname", "-srmo"]),
        "cpu_model": cpu_model,
        "core_count": os.cpu_count(),
        "compiler": compiler_version.splitlines()[0] if compiler_version else "",
        "python": sys.version.splitlines()[0],
        "perf_event_paranoid": read_perf_paranoid(),
    }


def write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def read_csv(path: Path) -> list[dict]:
    with path.open("r", newline="") as handle:
        return list(csv.DictReader(handle))


def slugify(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    return slug or "dataset"


def parse_runner_row(stdout: str) -> dict:
    lines = [line for line in stdout.splitlines() if line.strip()]
    if len(lines) != 1:
        fail(f"Expected exactly one CSV row from run_dynamic_cpu_benchmark, got {len(lines)} lines.")
    reader = csv.DictReader(lines, fieldnames=RAW_COLUMNS)
    row = next(reader)
    return row


def numeric_value(value: str):
    if value == "":
        return None
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return float(value)


def sample_size_for(batch_size: int, config: dict) -> int:
    return max(batch_size * config["sample_multiplier"], batch_size + 1024)


def benchmark_invocation(code_root: Path, graph_path: Path, config: dict, run_id: str,
                         operation: str, batch_size: int, trial: int, warmup: bool,
                         log_dir: Path) -> dict | None:
    env = os.environ.copy()
    env["NUM_THREADS"] = str(config["threads"])
    cmd = [
        str(code_root / "run_dynamic_cpu_benchmark"),
        "-f",
        str(graph_path),
        "-s",
        "--dataset-name",
        config["dataset_name"],
        "--operation",
        operation,
        "--batch-size",
        str(batch_size),
        "--seed",
        str(config["seed"]),
        "--trial",
        str(trial),
        "--threads",
        str(config["threads"]),
        "--bfs-src",
        "auto",
        "--sample-size",
        str(sample_size_for(batch_size, config)),
        "--run-id",
        run_id,
    ]
    if config["mmap"]:
        cmd.append("-m")
    if config["compressed"]:
        cmd.append("-c")
    if config["nparts"] != 1:
        cmd.extend(["-nparts", str(config["nparts"])])
    if config["collect_perf"]:
        cmd.append("--collect-perf")

    label = f"{operation}_bs{batch_size}_trial{trial}{'_warmup' if warmup else ''}"
    result = run_command(cmd, cwd=code_root, env=env, capture_output=True, check=False)
    (log_dir / f"{label}.stdout.log").write_text(result.stdout)
    (log_dir / f"{label}.stderr.log").write_text(result.stderr)

    if result.returncode != 0:
        fail(
            f"Benchmark command failed for {label}.\n"
            f"Command: {' '.join(cmd)}\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )

    if warmup:
        return None
    return parse_runner_row(result.stdout)


def aggregate_rows(rows: list[dict]) -> list[dict]:
    groups: dict[tuple[str, str, str, str], list[float]] = {}
    for row in rows:
        for metric in METRICS:
            value = numeric_value(row[metric])
            if value is None:
                continue
            key = (row["dataset"], row["batch_size"], row["operation_type"], metric)
            groups.setdefault(key, []).append(float(value))

    aggregate_rows_out = []
    for (dataset, batch_size, operation_type, metric_name), values in sorted(groups.items()):
        stddev = statistics.stdev(values) if len(values) > 1 else 0.0
        aggregate_rows_out.append(
            {
                "dataset": dataset,
                "batch_size": batch_size,
                "operation_type": operation_type,
                "metric_name": metric_name,
                "mean": f"{statistics.mean(values):.6f}",
                "stddev": f"{stddev:.6f}",
                "min": f"{min(values):.6f}",
                "max": f"{max(values):.6f}",
                "trials": str(len(values)),
            }
        )
    return aggregate_rows_out


def write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _build_pivot(rows: list[dict]) -> dict:
    """pivot[(dataset, batch_size, operation_type)] = {metric_name: (mean, stddev)}"""
    pivot: dict = {}
    for row in rows:
        key = (row["dataset"], row["batch_size"], row["operation_type"])
        try:
            pivot.setdefault(key, {})[row["metric_name"]] = (
                float(row["mean"]),
                float(row["stddev"]),
            )
        except (ValueError, KeyError):
            pass
    return pivot


def _compute_derived(pivot: dict) -> dict:
    """
    Returns derived[(dataset, metric_name)] = list of (batch_size_int, operation_type, mean, stddev).

    Derived metrics:
      mem_bw_mbps  — approximate LLC-miss bandwidth = cache_misses * 64 bytes / update_time (MB/s)
      ipc          — Instructions Per Cycle = instructions / cycles  (proxy for compute efficiency)
    """
    derived: dict = {}
    for (dataset, batch_size, op), metrics in pivot.items():
        bs = int(batch_size)

        if "update_cache_misses" in metrics and "update_ms" in metrics:
            cm, cm_s = metrics["update_cache_misses"]
            um, um_s = metrics["update_ms"]
            if um > 0 and cm > 0:
                bw = cm * 64.0 / (um / 1000.0) / 1e9  # GB/s (update-phase scoped)
                rel = ((cm_s / cm) ** 2 + (um_s / um) ** 2) ** 0.5
                bw_err = bw * rel
                derived.setdefault((dataset, "mem_bw_gbps"), []).append((bs, op, bw, bw_err))

        if "update_cache_misses" in metrics and "update_cache_refs" in metrics:
            cm, cm_s = metrics["update_cache_misses"]
            cr, cr_s = metrics["update_cache_refs"]
            if cr > 0:
                rate = cm / cr * 100.0
                rate_err = rate * ((cm_s / max(cm, 1)) ** 2 + (cr_s / max(cr, 1)) ** 2) ** 0.5
                derived.setdefault((dataset, "llc_miss_rate_pct"), []).append((bs, op, rate, rate_err))

        if "update_instructions" in metrics and "update_cycles" in metrics:
            inst, inst_s = metrics["update_instructions"]
            cyc, cyc_s = metrics["update_cycles"]
            if cyc > 0 and inst > 0:
                ipc = inst / cyc
                ipc_err = ipc * ((inst_s / max(inst, 1)) ** 2 + (cyc_s / max(cyc, 1)) ** 2) ** 0.5
                derived.setdefault((dataset, "update_ipc"), []).append((bs, op, ipc, ipc_err))

        if "bfs_instructions" in metrics and "bfs_cycles" in metrics:
            inst, inst_s = metrics["bfs_instructions"]
            cyc, cyc_s = metrics["bfs_cycles"]
            if cyc > 0 and inst > 0:
                ipc = inst / cyc
                ipc_err = ipc * ((inst_s / max(inst, 1)) ** 2 + (cyc_s / max(cyc, 1)) ** 2) ** 0.5
                derived.setdefault((dataset, "bfs_ipc"), []).append((bs, op, ipc, ipc_err))

    return derived


def _make_subtitle(run_config: dict, metadata: dict) -> str:
    parts = []
    if run_config.get("threads"):
        parts.append(f"threads={run_config['threads']}")
    cpu = metadata.get("cpu_model", "").strip()
    if cpu:
        parts.append(cpu)
    return " | ".join(parts)


def _errorbar_chart(
    ax,
    data_by_op: dict,
    *,
    x_label: str,
    y_label: str,
    title: str,
    sci_y: bool = False,
) -> None:
    """Plot one axis with insert/delete error-bar lines."""
    operation_styles = {
        "insert": {"color": "#005f73", "marker": "o", "label": "Insert"},
        "delete": {"color": "#bb3e03", "marker": "s", "label": "Delete"},
    }
    has_multiple_x = len({bs for pts in data_by_op.values() for bs, *_ in pts}) > 1
    for op, points in data_by_op.items():
        if not points:
            continue
        points.sort(key=lambda p: p[0])
        x = [p[0] for p in points]
        y = [p[1] for p in points]
        err = [p[2] for p in points]
        style = operation_styles.get(op, {"color": "grey", "marker": "^", "label": op})
        ax.errorbar(x, y, yerr=err, color=style["color"], marker=style["marker"],
                    linewidth=2.0, markersize=7, capsize=5, label=style["label"])
    if has_multiple_x:
        ax.set_xscale("log", base=2)
    ax.set_title(title, fontsize=13, pad=8)
    ax.set_xlabel(x_label, fontsize=11)
    ax.set_ylabel(y_label, fontsize=11)
    ax.grid(True, linestyle="--", alpha=0.35)
    if sci_y:
        ax.ticklabel_format(axis="y", style="sci", scilimits=(0, 0))
    ax.legend(fontsize=10)


def generate_plots(run_dir: Path, plot_dir: Path | None = None) -> list[Path]:
    aggregate_csv = run_dir / "aggregate.csv"
    if not aggregate_csv.exists():
        fail(f"Could not find aggregate.csv in plot input directory: {run_dir}")

    if plot_dir is None:
        plot_dir = run_dir / "plots"
    plot_dir.mkdir(parents=True, exist_ok=True)

    metadata_path = run_dir / "metadata.json"
    run_config_path = run_dir / "run_config.json"
    metadata = json.loads(metadata_path.read_text()) if metadata_path.exists() else {}
    run_config = json.loads(run_config_path.read_text()) if run_config_path.exists() else {}

    mpl_config_dir = plot_dir / ".matplotlib"
    mpl_config_dir.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("MPLCONFIGDIR", str(mpl_config_dir))

    try:
        import matplotlib
    except ModuleNotFoundError:
        fail(
            "Plot generation requires matplotlib. Install it with: pip install matplotlib\n"
            f"Active Python: {sys.executable}"
        )

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    rows = read_csv(aggregate_csv)
    if not rows:
        fail(f"aggregate.csv is empty in {run_dir}")

    subtitle = _make_subtitle(run_config, metadata)
    pivot = _build_pivot(rows)
    derived = _compute_derived(pivot)

    datasets = sorted({row["dataset"] for row in rows})
    generated_paths: list[Path] = []

    for dataset in datasets:
        slug = slugify(dataset)
        dataset_rows = [row for row in rows if row["dataset"] == dataset]

        def get_metric_rows(metric: str) -> list[dict]:
            return [r for r in dataset_rows if r["metric_name"] == metric]

        # ── Chart 1: Update Latency ──────────────────────────────────────────
        update_rows = get_metric_rows("update_ms")
        if update_rows:
            fig, ax = plt.subplots(figsize=(8, 5))
            data_by_op: dict = {}
            for r in update_rows:
                data_by_op.setdefault(r["operation_type"], []).append(
                    (int(r["batch_size"]), float(r["mean"]), float(r["stddev"]))
                )
            _errorbar_chart(
                ax, data_by_op,
                x_label="Batch Size (logical undirected edges)",
                y_label="Latency (ms)",
                title=f"Update Latency — {dataset}",
            )
            fig.suptitle(subtitle, fontsize=9, color="#555555")
            fig.tight_layout(rect=(0, 0, 1, 0.96))
            out = plot_dir / f"{slug}_update_latency.png"
            fig.savefig(out, dpi=200, bbox_inches="tight")
            plt.close(fig)
            generated_paths.append(out)

        # ── Chart 2: Memory Bandwidth Utilization (approximate) ───────────────
        bw_points = derived.get((dataset, "mem_bw_mbps"), [])
        if bw_points:
            fig, ax = plt.subplots(figsize=(8, 5))
            data_by_op = {}
            for bs, op, mean, err in bw_points:
                data_by_op.setdefault(op, []).append((bs, mean, err))
            _errorbar_chart(
                ax, data_by_op,
                x_label="Batch Size (logical undirected edges)",
                y_label="Bandwidth (MB/s)",
                title=f"Memory Bandwidth Utilization — {dataset}",
            )
            ax.annotate(
                "Approximate: LLC cache misses × 64 B ÷ update time",
                xy=(0.02, 0.97), xycoords="axes fraction",
                fontsize=8, color="#888888", va="top",
            )
            fig.suptitle(subtitle, fontsize=9, color="#555555")
            fig.tight_layout(rect=(0, 0, 1, 0.96))
            out = plot_dir / f"{slug}_mem_bandwidth.png"
            fig.savefig(out, dpi=200, bbox_inches="tight")
            plt.close(fig)
            generated_paths.append(out)

        # ── Chart 3: CPU Efficiency (IPC) ─────────────────────────────────────
        ipc_points = derived.get((dataset, "ipc"), [])
        if ipc_points:
            fig, ax = plt.subplots(figsize=(8, 5))
            data_by_op = {}
            for bs, op, mean, err in ipc_points:
                data_by_op.setdefault(op, []).append((bs, mean, err))
            _errorbar_chart(
                ax, data_by_op,
                x_label="Batch Size (logical undirected edges)",
                y_label="IPC (Instructions Per Cycle)",
                title=f"CPU Efficiency (IPC) — {dataset}",
            )
            ax.annotate(
                "IPC = Instructions ÷ Cycles. Higher = more compute work per clock.",
                xy=(0.02, 0.97), xycoords="axes fraction",
                fontsize=8, color="#888888", va="top",
            )
            fig.suptitle(subtitle, fontsize=9, color="#555555")
            fig.tight_layout(rect=(0, 0, 1, 0.96))
            out = plot_dir / f"{slug}_cpu_efficiency_ipc.png"
            fig.savefig(out, dpi=200, bbox_inches="tight")
            plt.close(fig)
            generated_paths.append(out)

        # ── Chart 4: Raw perf counters (combined) ─────────────────────────────
        perf_metrics = [
            ("cycles", "CPU Cycles", True),
            ("instructions", "Instructions", True),
            ("cache_misses", "Cache Misses", True),
            ("branch_misses", "Branch Misses", True),
        ]
        available_perf = [(m, lbl, sci) for m, lbl, sci in perf_metrics if get_metric_rows(m)]
        if available_perf:
            ncols = 2
            nrows = (len(available_perf) + 1) // 2
            fig, axes = plt.subplots(nrows, ncols, figsize=(14, 5 * nrows), squeeze=False)
            axes_flat = axes.flatten()
            for idx, (metric, lbl, sci) in enumerate(available_perf):
                data_by_op = {}
                for r in get_metric_rows(metric):
                    data_by_op.setdefault(r["operation_type"], []).append(
                        (int(r["batch_size"]), float(r["mean"]), float(r["stddev"]))
                    )
                _errorbar_chart(
                    axes_flat[idx], data_by_op,
                    x_label="Batch Size",
                    y_label=lbl,
                    title=lbl,
                    sci_y=sci,
                )
            for idx in range(len(available_perf), len(axes_flat)):
                fig.delaxes(axes_flat[idx])
            fig.suptitle(f"Perf Counters — {dataset}\n{subtitle}", fontsize=12)
            fig.tight_layout(rect=(0, 0, 1, 0.95))
            out = plot_dir / f"{slug}_perf_counters.png"
            fig.savefig(out, dpi=200, bbox_inches="tight")
            plt.close(fig)
            generated_paths.append(out)

    if not generated_paths:
        fail(f"No plottable metrics were found in {aggregate_csv}")
    return generated_paths


def main() -> None:
    args = parse_args()
    repo_root, code_root = repo_roots()
    if sys.version_info < (3, 10):
        fail("Python 3.10 or newer is required.")

    config = resolve_config(args, repo_root)

    if config["plot_only"]:
        run_dir = Path(config["plot_only"])
        if not run_dir.is_absolute():
            run_dir = repo_root / run_dir
        plot_dir = None
        if config["plot_dir"]:
            plot_dir = Path(config["plot_dir"])
            if not plot_dir.is_absolute():
                plot_dir = repo_root / plot_dir
        generated_paths = generate_plots(run_dir, plot_dir)
        print(f"Plot input: {run_dir}")
        for path in generated_paths:
            print(f"Plot: {path}")
        return

    if config["threads"] <= 0:
        fail("--threads must be positive.")
    if config["trials"] <= 0:
        fail("--trials must be positive.")
    if config["warmup_trials"] < 0:
        fail("--warmup-trials must be non-negative.")
    if not config["batch_sizes"] or any(batch <= 0 for batch in config["batch_sizes"]):
        fail("--batch-sizes must contain one or more positive integers.")

    require_tool("make")
    require_tool("g++")
    ensure_perf_access(config["collect_perf"])

    run_id, output_dir = ensure_paths(repo_root, code_root, config)
    build_binaries(code_root)
    graph_path = prepare_dataset(repo_root, code_root, config)

    metadata = collect_metadata(code_root)
    run_config = {
        "profile": config["profile"],
        "dataset": config["dataset"],
        "dataset_name": config["dataset_name"],
        "graph_path": str(graph_path),
        "mmap": config["mmap"],
        "compressed": config["compressed"],
        "nparts": config["nparts"],
        "batch_sizes": config["batch_sizes"],
        "trials": config["trials"],
        "warmup_trials": config["warmup_trials"],
        "threads": config["threads"],
        "seed": config["seed"],
        "collect_perf": config["collect_perf"],
        "sample_multiplier": config["sample_multiplier"],
        "config_file": config["config_file"],
        "config_key": config["config_key"],
        "repo_root": str(repo_root),
        "code_root": str(code_root),
    }
    write_json(output_dir / "metadata.json", metadata)
    write_json(output_dir / "run_config.json", run_config)

    measured_rows: list[dict] = []
    log_dir = output_dir / "logs"
    for operation in ("insert", "delete"):
        for batch_size in config["batch_sizes"]:
            for warmup_trial in range(config["warmup_trials"]):
                benchmark_invocation(
                    code_root, graph_path, config, run_id, operation, batch_size, warmup_trial, True, log_dir
                )
            for trial in range(config["trials"]):
                row = benchmark_invocation(
                    code_root, graph_path, config, run_id, operation, batch_size, trial, False, log_dir
                )
                assert row is not None
                measured_rows.append(row)

    raw_csv_path = output_dir / "raw.csv"
    aggregate_csv_path = output_dir / "aggregate.csv"
    write_csv(raw_csv_path, RAW_COLUMNS, measured_rows)
    write_csv(aggregate_csv_path, AGG_COLUMNS, aggregate_rows(measured_rows))

    print(f"Run ID: {run_id}")
    print(f"Dataset: {config['dataset_name']}")
    print(f"Graph: {graph_path}")
    print(f"Raw CSV: {raw_csv_path}")
    print(f"Aggregate CSV: {aggregate_csv_path}")
    if config["make_plots"]:
        plot_dir = None
        if config["plot_dir"]:
            plot_dir = Path(config["plot_dir"])
            if not plot_dir.is_absolute():
                plot_dir = repo_root / plot_dir
        generated_paths = generate_plots(output_dir, plot_dir)
        for path in generated_paths:
            print(f"Plot: {path}")


if __name__ == "__main__":
    main()
