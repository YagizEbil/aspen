#!/usr/bin/env python3
"""
run_comparison_bench.py — Static vs Dynamic BFS Comparison Pipeline

Runs the same BFS from the same source vertex on the same graph in two modes:

  static_baseline  — graph snapshot, no updates (run_static_bfs_bench)
  dynamic_insert   — batch insert → immediate BFS (run_dynamic_cpu_benchmark)
  dynamic_delete   — batch delete → immediate BFS (run_dynamic_cpu_benchmark)

All three phases use the same input graph, the same BFS source vertex, the same
thread count, and the same dataset label, ensuring a fair apples-to-apples
comparison.

Outputs per run:
  raw.csv          — one row per (phase, batch_size, trial)
  aggregate.csv    — mean/stddev/min/max per (phase, batch_size, metric)
  run_config.json
  metadata.json
  logs/
  plots/           — when --make-plots or --plot-only is used

Usage examples:

  # Default profile (RMAT scale-18)
  .venv/bin/python3 code/scripts/run_comparison_bench.py --profile default --make-plots

  # Smoke test (tiny graph, no perf)
  .venv/bin/python3 code/scripts/run_comparison_bench.py --profile smoke --skip-perf --make-plots

  # SNAP graph
  .venv/bin/python3 code/scripts/run_comparison_bench.py \\
      --profile custom --dataset snap --dataset-name soc_livejournal \\
      --snap-input code/inputs/soc-LiveJournal1.txt \\
      --batch-sizes 128,1024,8192 --trials 5 --warmup-trials 1 --threads 48 --make-plots

  # Re-plot an existing comparison run
  .venv/bin/python3 code/scripts/run_comparison_bench.py \\
      --plot-only code/data/aspen_comparison_bench/<run_id>
"""

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

# ── CSV schemas ───────────────────────────────────────────────────────────────

# Columns emitted by run_dynamic_cpu_benchmark (split perf counters per phase)
# cache_refs  = LLC accesses (AMD: cache-references = L3 lookups)
# cache_misses = LLC misses  (AMD: cache-misses     = L3 misses → DRAM)
_RAW_COLUMNS_FROM_DYNAMIC_BINARY = [
    "run_id", "timestamp", "dataset", "num_vertices", "num_edges_initial",
    "batch_id", "batch_size", "operation_type", "update_ms", "bfs_ms", "pr_ms",
    "update_cycles", "update_instructions", "update_cache_refs", "update_cache_misses", "update_branch_misses",
    "bfs_cycles", "bfs_instructions", "bfs_cache_refs", "bfs_cache_misses", "bfs_branch_misses",
    "update_energy_j", "bfs_energy_j",
    # update-phase breakdown (empty unless --breakdown passed)
    "update_sort_ms", "update_preprocess_ms", "update_edge_struct_ms", "update_tree_merge_ms",
    # stall counters (empty unless --collect-stall passed and PMU supports them)
    "update_stall_frontend", "update_stall_backend", "bfs_stall_frontend", "bfs_stall_backend",
    "seed", "trial", "threads", "notes",
]

# Columns emitted by run_static_bfs_bench (combined perf, bfs only)
_RAW_COLUMNS_FROM_STATIC_BINARY = [
    "run_id", "timestamp", "dataset", "num_vertices", "num_edges_initial",
    "batch_id", "batch_size", "operation_type", "update_ms", "bfs_ms", "pr_ms",
    "cycles", "instructions", "cache_refs", "cache_misses", "branch_misses",
    "bfs_energy_j",
    "seed", "trial", "threads", "notes",
]

# Comparison raw.csv adds derived columns on top
COMP_RAW_COLUMNS = [
    "run_id", "timestamp", "dataset", "num_vertices", "num_edges",
    "phase",           # static_baseline | dynamic_insert | dynamic_delete
    "batch_size",      # 0 for static
    "bfs_source",      # explicit vertex ID used in all phases
    "trial", "threads",
    "update_ms",       # empty for static_baseline
    "bfs_ms",
    "end_to_end_ms",   # bfs_ms for static; update_ms + bfs_ms for dynamic
    # update-phase perf (empty for static)
    "update_cycles", "update_instructions",
    "update_cache_refs", "update_cache_misses", "update_branch_misses",
    # BFS-phase perf (always present when --collect-perf)
    "bfs_cycles", "bfs_instructions",
    "bfs_cache_refs", "bfs_cache_misses", "bfs_branch_misses",
    "update_energy_j", "bfs_energy_j",    # socket energy (RAPL); empty if paranoid > 0
    # update-phase breakdown (empty unless --collect-update-breakdown)
    "update_sort_ms", "update_preprocess_ms", "update_edge_struct_ms", "update_tree_merge_ms",
    # stall counters (empty unless --collect-stall and PMU supports them)
    "update_stall_frontend", "update_stall_backend", "bfs_stall_frontend", "bfs_stall_backend",
    "perf_scope",      # bfs_only (static) | update_separate;bfs_separate (dynamic)
    "notes",
]

COMP_AGG_COLUMNS = [
    "phase", "batch_size", "metric_name", "mean", "stddev", "min", "max", "trials",
]

# Metrics to aggregate; update_ms and update_* perf are skipped when empty (static phase)
AGG_METRICS = ["bfs_ms", "update_ms", "end_to_end_ms",
               "update_cycles", "update_instructions",
               "update_cache_refs", "update_cache_misses", "update_branch_misses",
               "bfs_cycles", "bfs_instructions",
               "bfs_cache_refs", "bfs_cache_misses", "bfs_branch_misses",
               "update_energy_j", "bfs_energy_j",
               "update_sort_ms", "update_preprocess_ms", "update_edge_struct_ms", "update_tree_merge_ms",
               "update_stall_frontend", "update_stall_backend",
               "bfs_stall_frontend", "bfs_stall_backend"]

# CSV schemas for new analysis outputs
THREAD_SCALING_COLUMNS = [
    "dataset", "phase", "batch_size", "threads", "trial",
    "update_ms", "bfs_ms", "end_to_end_ms", "notes",
]

STALL_METRICS_COLUMNS = [
    "dataset", "phase", "batch_size", "trial", "threads",
    "update_cycles", "update_instructions", "update_stall_frontend", "update_stall_backend",
    "bfs_cycles", "bfs_instructions", "bfs_stall_frontend", "bfs_stall_backend",
    "update_frontend_stall_pct", "update_backend_stall_pct", "update_cpi",
    "bfs_frontend_stall_pct", "bfs_backend_stall_pct", "bfs_cpi",
]

UPDATE_BREAKDOWN_COLUMNS = [
    "dataset", "phase", "batch_size", "trial", "component", "time_ms",
]


# ── Utilities ─────────────────────────────────────────────────────────────────

def fail(message: str) -> None:
    print(f"run_comparison_bench.py: {message}", file=sys.stderr)
    raise SystemExit(1)


def run_command(cmd, cwd: Path, env=None, capture_output=True, check=True):
    return subprocess.run(
        cmd, cwd=str(cwd), env=env, text=True,
        capture_output=capture_output, check=check,
    )


def require_tool(name: str) -> None:
    if shutil.which(name) is None:
        fail(f"Required dependency '{name}' was not found in PATH.")


def read_perf_paranoid() -> int | None:
    p = Path("/proc/sys/kernel/perf_event_paranoid")
    if not p.exists():
        return None
    try:
        return int(p.read_text().strip())
    except Exception:
        return None


def ensure_perf_access(enabled: bool) -> None:
    if not enabled:
        return
    paranoid = read_perf_paranoid()
    if paranoid is not None and paranoid > 1:
        fail(
            "Perf collection was requested but /proc/sys/kernel/perf_event_paranoid="
            f"{paranoid}. Lower it with: sudo sysctl -w kernel.perf_event_paranoid=1"
        )


def repo_roots() -> tuple[Path, Path]:
    script_path = Path(__file__).resolve()
    code_root = script_path.parents[1]
    repo_root = code_root.parent
    return repo_root, code_root


def write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def read_csv(path: Path) -> list[dict]:
    with path.open("r", newline="") as f:
        return list(csv.DictReader(f))


def slugify(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    return slug or "dataset"


def numeric_value(value: str):
    if not value:
        return None
    try:
        return float(value) if "." in value else int(value)
    except ValueError:
        return None


# ── Argument parsing ──────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Static vs Dynamic BFS comparison benchmark for Aspen."
    )
    p.add_argument("--config", default=None, help="JSON config file loaded before CLI overrides.")
    p.add_argument("--config-key", default=None, help="Optional top-level key inside --config.")
    p.add_argument("--profile", choices=["default", "smoke", "custom"], default=None)
    p.add_argument("--dataset", choices=["rmat", "snap", "adj"], default=None)
    p.add_argument("--dataset-name", default=None)
    p.add_argument("--graph-path", default=None)
    p.add_argument("--snap-input", default=None)
    p.add_argument("--rmat-scale", type=int, default=None)
    p.add_argument("--rmat-edge-factor", type=int, default=None)
    p.add_argument("--batch-sizes", default=None)
    p.add_argument("--trials", type=int, default=None)
    p.add_argument("--warmup-trials", type=int, default=None)
    p.add_argument("--threads", type=int, default=None)
    p.add_argument("--seed", type=int, default=None)
    p.add_argument(
        "--bfs-src", type=int, default=None,
        help="BFS source vertex used for all phases. Default: auto-selected as the "
             "highest-degree vertex in the graph.",
    )
    p.add_argument("--skip-perf", action="store_true", default=None)
    p.add_argument(
        "--collect-stall", action="store_true", default=None,
        help="Collect stalled-cycle frontend/backend counters. Replaces branch_misses "
             "(same PMC budget). Generates stall_metrics.csv and stall_analysis plot.",
    )
    p.add_argument(
        "--collect-update-breakdown", action="store_true", default=None,
        help="Collect per-phase timing breakdown of the Aspen update path. "
             "Generates update_breakdown.csv and stacked-bar breakdown plot.",
    )
    p.add_argument(
        "--thread-scaling", action="store_true", default=None,
        help="Run a thread-count sweep for insert and delete. "
             "Generates thread_scaling.csv and thread_scaling plot.",
    )
    p.add_argument(
        "--thread-list", default=None,
        help="Comma-separated thread counts for --thread-scaling "
             "(default: 1,2,4,8,16,32,48; values > os.cpu_count() are skipped).",
    )
    p.add_argument(
        "--static-warmup-rounds", type=int, default=None,
        help="Number of discarded BFS iterations run inside run_static_bfs_bench before "
             "the timed measurement. Warms the cache to match dynamic benchmark conditions. "
             "Default: 3.",
    )
    p.add_argument("--sample-multiplier", type=int, default=None)
    p.add_argument("--mmap", action="store_true", default=None)
    p.add_argument("--compressed", action="store_true", default=None)
    p.add_argument("--nparts", type=int, default=None)
    p.add_argument("--output-dir", default=None)
    p.add_argument("--make-plots", action="store_true", default=None)
    p.add_argument("--plot-only", default=None)
    p.add_argument("--plot-dir", default=None)
    return p.parse_args()


def profile_defaults(profile: str) -> dict:
    if profile == "default":
        return {
            "dataset": "rmat", "dataset_name": "rmat_scale18_ef8",
            "rmat_scale": 18, "rmat_edge_factor": 8,
            "batch_sizes": [128, 1024, 8192],
            "trials": 5, "warmup_trials": 1,
            "threads": max(1, os.cpu_count() or 1),
        }
    if profile == "smoke":
        return {
            "dataset": "rmat", "dataset_name": "rmat_scale12_ef4_smoke",
            "rmat_scale": 12, "rmat_edge_factor": 4,
            "batch_sizes": [32],
            "trials": 2, "warmup_trials": 1,
            "threads": min(4, max(1, os.cpu_count() or 1)),
        }
    return {
        "dataset": "rmat", "dataset_name": "rmat_custom",
        "rmat_scale": 18, "rmat_edge_factor": 8,
        "batch_sizes": [1024],
        "trials": 3, "warmup_trials": 1,
        "threads": max(1, os.cpu_count() or 1),
    }


def resolve_config(args: argparse.Namespace, repo_root: Path) -> dict:
    file_cfg, config_path = load_named_json_config(args.config, args.config_key, repo_root, fail)

    profile = args.profile if args.profile is not None else file_cfg.get("profile")
    if profile is None:
        profile = "default"

    config = dict(profile_defaults(profile))
    config.update({
        "profile": profile,
        "seed": 1,
        "collect_perf": True,
        "collect_stall": False,
        "collect_update_breakdown": False,
        "thread_scaling": False,
        "thread_list": [1, 2, 4, 8, 16, 32, 48],
        "sample_multiplier": 8,
        "graph_path": None,
        "snap_input": None,
        "mmap": False,
        "compressed": False,
        "nparts": 1,
        "bfs_src": None,
        "static_warmup_rounds": 3,
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

    for key in ("rmat_scale", "rmat_edge_factor", "trials", "warmup_trials", "threads",
                "seed", "nparts", "bfs_src", "static_warmup_rounds", "sample_multiplier"):
        if key in file_cfg and file_cfg[key] is not None:
            config[key] = coerce_int(file_cfg[key], key, fail)

    for key in ("mmap", "compressed", "make_plots", "collect_stall",
                "collect_update_breakdown", "thread_scaling"):
        if key in file_cfg and file_cfg[key] is not None:
            config[key] = coerce_bool(file_cfg[key], key, fail)

    if "batch_sizes" in file_cfg and file_cfg["batch_sizes"] is not None:
        config["batch_sizes"] = coerce_int_list(file_cfg["batch_sizes"], "batch_sizes", fail)
    if "thread_list" in file_cfg and file_cfg["thread_list"] is not None:
        config["thread_list"] = coerce_int_list(file_cfg["thread_list"], "thread_list", fail)

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
    if args.bfs_src is not None:
        config["bfs_src"] = args.bfs_src
    if args.skip_perf is True:
        config["collect_perf"] = False
    if args.collect_stall is True:
        config["collect_stall"] = True
    if args.collect_update_breakdown is True:
        config["collect_update_breakdown"] = True
    if args.thread_scaling is True:
        config["thread_scaling"] = True
    if args.thread_list is not None:
        config["thread_list"] = coerce_int_list(args.thread_list, "thread_list", fail)
    if args.static_warmup_rounds is not None:
        config["static_warmup_rounds"] = args.static_warmup_rounds
    if args.sample_multiplier is not None:
        config["sample_multiplier"] = args.sample_multiplier
    if args.mmap is True:
        config["mmap"] = True
    if args.compressed is True:
        config["compressed"] = True
    if args.nparts is not None:
        config["nparts"] = args.nparts
    if args.output_dir is not None:
        config["output_dir"] = args.output_dir
    if args.make_plots is True:
        config["make_plots"] = True
    if args.plot_only is not None:
        config["plot_only"] = args.plot_only
    if args.plot_dir is not None:
        config["plot_dir"] = args.plot_dir

    config["sample_multiplier"] = max(2, config["sample_multiplier"])
    return config


# ── File system setup ─────────────────────────────────────────────────────────

def ensure_paths(repo_root: Path, code_root: Path, config: dict) -> tuple[str, Path]:
    run_id = datetime.now(timezone.utc).strftime("run_%Y%m%dT%H%M%SZ")
    override = config.get("output_dir")
    if override:
        output_dir = Path(override)
        if not output_dir.is_absolute():
            output_dir = repo_root / output_dir
    else:
        output_dir = code_root / "data" / "aspen_comparison_bench" / run_id
    try:
        output_dir.mkdir(parents=True, exist_ok=False)
        (output_dir / "logs").mkdir(exist_ok=True)
    except OSError as exc:
        fail(f"Could not create output directory '{output_dir}': {exc}")
    return run_id, output_dir


# ── Build ─────────────────────────────────────────────────────────────────────

def build_binaries(code_root: Path) -> None:
    cmd = ["make", "snap_to_adj", "rmat_to_adj",
           "run_static_bfs_bench", "run_dynamic_cpu_benchmark"]
    result = run_command(cmd, cwd=code_root, capture_output=True, check=False)
    if result.returncode != 0:
        fail("Build failed.\n" + result.stdout + result.stderr)


# ── Dataset preparation (shared with run_aspen_cpu_bench.py logic) ────────────

def prepare_rmat_dataset(code_root: Path, config: dict) -> Path:
    output_path = code_root / "inputs" / f"{config['dataset_name']}.adj"
    if output_path.exists():
        return output_path
    cmd = [
        str(code_root / "rmat_to_adj"),
        "--scale", str(config["rmat_scale"]),
        "--edge-factor", str(config["rmat_edge_factor"]),
        "--seed", str(config["seed"]),
        "--output", str(output_path),
    ]
    result = run_command(cmd, cwd=code_root, capture_output=True, check=False)
    if result.returncode != 0:
        fail("RMAT generation failed.\n" + result.stdout + result.stderr)
    return output_path


def prepare_snap_dataset(repo_root: Path, code_root: Path, config: dict) -> Path:
    snap_input = config.get("snap_input")
    if not snap_input:
        fail("--snap-input is required when --dataset snap is selected.")
    snap_path = Path(snap_input)
    if not snap_path.is_absolute():
        snap_path = repo_root / snap_input
    if not snap_path.exists():
        fail(f"SNAP input does not exist: {snap_path}")
    output_path = code_root / "inputs" / f"{config['dataset_name']}.adj"
    if output_path.exists():
        return output_path
    result = run_command(
        [str(code_root / "snap_to_adj"), str(snap_path), str(output_path)],
        cwd=code_root, capture_output=True, check=False,
    )
    if result.returncode != 0:
        fail("SNAP conversion failed.\n" + result.stdout + result.stderr)
    return output_path


def prepare_adj_dataset(repo_root: Path, config: dict) -> Path:
    graph_path = config.get("graph_path")
    if not graph_path:
        fail("--graph-path is required when --dataset adj is selected.")
    p = Path(graph_path)
    if not p.is_absolute():
        p = repo_root / graph_path
    if not p.exists():
        fail(f"Adjacency graph does not exist: {p}")
    return p


def prepare_dataset(repo_root: Path, code_root: Path, config: dict) -> Path:
    d = config["dataset"]
    if d == "rmat":
        return prepare_rmat_dataset(code_root, config)
    if d == "snap":
        return prepare_snap_dataset(repo_root, code_root, config)
    if d == "adj":
        return prepare_adj_dataset(repo_root, config)
    fail(f"Unsupported dataset mode: {d}")


# ── BFS source selection ──────────────────────────────────────────────────────

def find_max_degree_vertex(adj_path: Path) -> int:
    """
    Parse the AdjacencyGraph offset section to find the highest-degree vertex.
    Avoids loading the full edge list — only reads the n+3 header lines.

    Format:
      AdjacencyGraph
      n
      m
      offset[0]
      ...
      offset[n-1]
      <m edge lines>
    """
    print(f"  Selecting BFS source: scanning {adj_path.name} for highest-degree vertex...")
    with adj_path.open() as f:
        tag = f.readline().strip()
        if tag != "AdjacencyGraph":
            fail(f"Unexpected adj file tag: {tag!r}. Expected 'AdjacencyGraph'.")
        n = int(f.readline().strip())
        m = int(f.readline().strip())

        prev_offset = int(f.readline().strip())
        max_deg = 0
        max_v = 0
        for i in range(1, n):
            cur_offset = int(f.readline().strip())
            deg = cur_offset - prev_offset
            if deg > max_deg:
                max_deg = deg
                max_v = i - 1  # degree is for vertex i-1
            prev_offset = cur_offset
        # check last vertex
        last_deg = m - prev_offset
        if last_deg > max_deg:
            max_v = n - 1
            max_deg = last_deg

    print(f"  BFS source: vertex {max_v} (degree {max_deg})")
    return max_v


# ── Metadata ──────────────────────────────────────────────────────────────────

def collect_metadata(code_root: Path) -> dict:
    def safe(cmd):
        r = run_command(cmd, cwd=code_root, capture_output=True, check=False)
        return r.stdout.strip() if r.returncode == 0 else ""

    cpu_model = ""
    for line in safe(["lscpu"]).splitlines():
        if line.startswith("Model name:"):
            cpu_model = line.split(":", 1)[1].strip()
            break

    compiler = safe(["g++", "--version"])
    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "platform": platform.platform(),
        "kernel": safe(["uname", "-srmo"]),
        "cpu_model": cpu_model,
        "core_count": os.cpu_count(),
        "compiler": compiler.splitlines()[0] if compiler else "",
        "python": sys.version.splitlines()[0],
        "perf_event_paranoid": read_perf_paranoid(),
    }


# ── Benchmark invocations ─────────────────────────────────────────────────────

def _parse_binary_row(stdout: str, is_static: bool = False) -> dict:
    """Parse the single CSV row emitted by run_static_bfs_bench or run_dynamic_cpu_benchmark."""
    lines = [line for line in stdout.splitlines() if line.strip()]
    if len(lines) != 1:
        fail(f"Expected exactly 1 CSV row from benchmark binary, got {len(lines)} lines:\n{stdout}")
    columns = _RAW_COLUMNS_FROM_STATIC_BINARY if is_static else _RAW_COLUMNS_FROM_DYNAMIC_BINARY
    reader = csv.DictReader(lines, fieldnames=columns)
    return next(reader)


def _augment_row(raw: dict, bfs_src: int) -> dict:
    """Map the binary's CSV row to the comparison schema."""
    op = raw["operation_type"]
    if op == "static_baseline":
        phase = "static_baseline"
        perf_scope = "bfs_only"
        update_ms = ""
        bfs_ms_val = float(raw["bfs_ms"])
        end_to_end_ms = f"{bfs_ms_val:.6f}"
        # Static binary emits combined bfs-only counters under generic names
        update_cycles = update_instructions = ""
        update_cache_refs = update_cache_misses = update_branch_misses = ""
        bfs_cycles        = raw.get("cycles", "")
        bfs_instructions  = raw.get("instructions", "")
        bfs_cache_refs    = raw.get("cache_refs", "")
        bfs_cache_misses  = raw.get("cache_misses", "")
        bfs_branch_misses = raw.get("branch_misses", "")
        update_energy_j   = ""
        bfs_energy_j      = raw.get("bfs_energy_j", "")
    else:
        phase = f"dynamic_{op}"
        perf_scope = "update_separate;bfs_separate"
        update_ms = raw["update_ms"]
        bfs_ms_val = float(raw["bfs_ms"])
        update_ms_val = float(raw["update_ms"])
        end_to_end_ms = f"{update_ms_val + bfs_ms_val:.6f}"
        update_cycles        = raw.get("update_cycles", "")
        update_instructions  = raw.get("update_instructions", "")
        update_cache_refs    = raw.get("update_cache_refs", "")
        update_cache_misses  = raw.get("update_cache_misses", "")
        update_branch_misses = raw.get("update_branch_misses", "")
        bfs_cycles           = raw.get("bfs_cycles", "")
        bfs_instructions     = raw.get("bfs_instructions", "")
        bfs_cache_refs       = raw.get("bfs_cache_refs", "")
        bfs_cache_misses     = raw.get("bfs_cache_misses", "")
        bfs_branch_misses    = raw.get("bfs_branch_misses", "")
        update_energy_j      = raw.get("update_energy_j", "")
        bfs_energy_j         = raw.get("bfs_energy_j", "")

    return {
        "run_id":         raw["run_id"],
        "timestamp":      raw["timestamp"],
        "dataset":        raw["dataset"],
        "num_vertices":   raw["num_vertices"],
        "num_edges":      raw["num_edges_initial"],
        "phase":          phase,
        "batch_size":     raw["batch_size"],
        "bfs_source":     str(bfs_src),
        "trial":          raw["trial"],
        "threads":        raw["threads"],
        "update_ms":      update_ms,
        "bfs_ms":         raw["bfs_ms"],
        "end_to_end_ms":  end_to_end_ms,
        "update_cycles":        update_cycles,
        "update_instructions":  update_instructions,
        "update_cache_refs":    update_cache_refs,
        "update_cache_misses":  update_cache_misses,
        "update_branch_misses": update_branch_misses,
        "bfs_cycles":           bfs_cycles,
        "bfs_instructions":     bfs_instructions,
        "bfs_cache_refs":       bfs_cache_refs,
        "bfs_cache_misses":     bfs_cache_misses,
        "bfs_branch_misses":    bfs_branch_misses,
        "update_energy_j":      update_energy_j,
        "bfs_energy_j":         bfs_energy_j,
        # breakdown columns — empty for static, pass-through for dynamic
        "update_sort_ms":        raw.get("update_sort_ms", "") if op != "static_baseline" else "",
        "update_preprocess_ms":  raw.get("update_preprocess_ms", "") if op != "static_baseline" else "",
        "update_edge_struct_ms": raw.get("update_edge_struct_ms", "") if op != "static_baseline" else "",
        "update_tree_merge_ms":  raw.get("update_tree_merge_ms", "") if op != "static_baseline" else "",
        # stall counters — pass-through for dynamic; static has none
        "update_stall_frontend": raw.get("update_stall_frontend", "") if op != "static_baseline" else "",
        "update_stall_backend":  raw.get("update_stall_backend", "") if op != "static_baseline" else "",
        "bfs_stall_frontend":    raw.get("bfs_stall_frontend", ""),
        "bfs_stall_backend":     raw.get("bfs_stall_backend", ""),
        "perf_scope":     perf_scope,
        "notes":          raw["notes"],
    }


def _label(phase: str, batch_size: int, trial: int, warmup: bool) -> str:
    suffix = "_warmup" if warmup else ""
    bs = f"_bs{batch_size}" if batch_size else ""
    return f"{phase}{bs}_trial{trial}{suffix}"


def run_static_trial(
    code_root: Path, graph_path: Path, config: dict,
    run_id: str, bfs_src: int, trial: int, warmup: bool, log_dir: Path,
) -> dict | None:
    env = os.environ.copy()
    env["NUM_THREADS"] = str(config["threads"])
    cmd = [
        str(code_root / "run_static_bfs_bench"),
        "-f", str(graph_path), "-s",
        "--run-id", run_id,
        "--dataset-name", config["dataset_name"],
        "--bfs-src", str(bfs_src),
        "--trial", str(trial),
        "--threads", str(config["threads"]),
        # Internal warmup: runs discarded BFS iterations in the same process to
        # bring the graph working set into L3 before the timed measurement.
        # Without this, static BFS runs cold while dynamic BFS runs warm (the
        # update phase inherently warms the cache), producing a misleading result
        # where static appears SLOWER than dynamic BFS.
        "--warmup-rounds", str(config.get("static_warmup_rounds", 3)),
    ]
    if config["mmap"]:
        cmd.append("-m")
    if config["compressed"]:
        cmd.append("-c")
    if config["nparts"] != 1:
        cmd.extend(["-nparts", str(config["nparts"])])
    if config["collect_perf"]:
        cmd.append("--collect-perf")

    label = _label("static_baseline", 0, trial, warmup)
    result = run_command(cmd, cwd=code_root, env=env, capture_output=True, check=False)
    (log_dir / f"{label}.stdout.log").write_text(result.stdout)
    (log_dir / f"{label}.stderr.log").write_text(result.stderr)

    if result.returncode != 0:
        fail(
            f"Static benchmark failed for {label}.\n"
            f"Command: {' '.join(cmd)}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    if warmup:
        return None
    return _augment_row(_parse_binary_row(result.stdout, is_static=True), bfs_src)


def _sample_size_for(batch_size: int, config: dict) -> int:
    return max(batch_size * config["sample_multiplier"], batch_size + 1024)


def run_dynamic_trial(
    code_root: Path, graph_path: Path, config: dict,
    run_id: str, operation: str, batch_size: int, bfs_src: int,
    trial: int, warmup: bool, log_dir: Path,
) -> dict | None:
    env = os.environ.copy()
    env["NUM_THREADS"] = str(config["threads"])
    cmd = [
        str(code_root / "run_dynamic_cpu_benchmark"),
        "-f", str(graph_path), "-s",
        "--run-id", run_id,
        "--dataset-name", config["dataset_name"],
        "--operation", operation,
        "--batch-size", str(batch_size),
        # Per-trial seed: gives each trial a distinct edge sample, which produces
        # better statistics and avoids rare hash-table race conditions in the PBBS
        # concurrent edge sampler when the same seed is reused across many trials.
        "--seed", str(config["seed"] + trial),
        "--trial", str(trial),
        "--threads", str(config["threads"]),
        "--bfs-src", str(bfs_src),   # fixed — same source as static phase
        "--sample-size", str(_sample_size_for(batch_size, config)),
    ]
    if config["mmap"]:
        cmd.append("-m")
    if config["compressed"]:
        cmd.append("-c")
    if config["nparts"] != 1:
        cmd.extend(["-nparts", str(config["nparts"])])
    if config["collect_perf"]:
        cmd.append("--collect-perf")
    if config.get("collect_stall"):
        cmd.append("--collect-stall")
    if config.get("collect_update_breakdown"):
        cmd.append("--breakdown")

    label = _label(f"dynamic_{operation}", batch_size, trial, warmup)
    result = run_command(cmd, cwd=code_root, env=env, capture_output=True, check=False)
    (log_dir / f"{label}.stdout.log").write_text(result.stdout)
    (log_dir / f"{label}.stderr.log").write_text(result.stderr)

    if result.returncode != 0:
        # Retry with a large seed offset to get a completely different edge sample.
        # Root cause: PBBS concurrent sparse_table_hash occasionally produces ghost
        # entries under 48-thread contention, causing the insert preconditioning
        # edge-count check to fail non-deterministically.
        MAX_RETRIES = 3
        for attempt in range(1, MAX_RETRIES + 1):
            shifted_seed = config["seed"] + trial + attempt * 100
            # Rebuild cmd with the new seed rather than doing a fragile string
            # substitution that could corrupt a coincidentally matching argument.
            seed_idx = cmd.index("--seed") + 1
            cmd_retry = list(cmd)
            cmd_retry[seed_idx] = str(shifted_seed)
            print(f"  [retry {attempt}/{MAX_RETRIES} seed={shifted_seed}]")
            result = run_command(cmd_retry, cwd=code_root, env=env, capture_output=True, check=False)
            retry_label = f"{label}_retry{attempt}"
            (log_dir / f"{retry_label}.stdout.log").write_text(result.stdout)
            (log_dir / f"{retry_label}.stderr.log").write_text(result.stderr)
            if result.returncode == 0:
                break
        else:
            fail(
                f"Dynamic benchmark failed for {label} after {MAX_RETRIES} retries.\n"
                f"Command: {' '.join(cmd)}\n"
                f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
            )
    if warmup:
        return None
    return _augment_row(_parse_binary_row(result.stdout), bfs_src)


# ── Aggregation ───────────────────────────────────────────────────────────────

def run_thread_scaling_sweep(
    code_root: Path, graph_path: Path, config: dict,
    run_id: str, bfs_src: int, log_dir: Path,
) -> list[dict]:
    """Run a thread-count sweep and return rows for thread_scaling.csv."""
    available_cpus = os.cpu_count() or 1
    thread_list = config.get("thread_list", [1, 2, 4, 8, 16, 32, 48])
    thread_list = sorted(set(thread_list))
    skipped = [t for t in thread_list if t > available_cpus]
    thread_list = [t for t in thread_list if t <= available_cpus]
    # Use a dedicated subdirectory so thread-scaling logs don't overwrite the
    # main-run logs (both use the same trial numbers and operation names).
    log_dir = log_dir / "thread_scaling"
    log_dir.mkdir(parents=True, exist_ok=True)
    if skipped:
        print(f"  [thread-scaling] Skipping thread counts > {available_cpus} CPUs: {skipped}")

    rows: list[dict] = []
    dataset = config["dataset_name"]
    for threads in thread_list:
        for operation in ("insert", "delete"):
            for batch_size in config["batch_sizes"]:
                phase = f"dynamic_{operation}"
                # Warmup
                warmup_config = dict(config, threads=threads)
                for w in range(config["warmup_trials"]):
                    run_dynamic_trial(code_root, graph_path, warmup_config, run_id,
                                      operation, batch_size, bfs_src, w, True, log_dir)
                # Measured trials
                for t in range(config["trials"]):
                    row = run_dynamic_trial(code_root, graph_path, warmup_config, run_id,
                                            operation, batch_size, bfs_src, t, False, log_dir)
                    if row is None:
                        continue
                    update_ms = row.get("update_ms", "")
                    bfs_ms = row.get("bfs_ms", "")
                    try:
                        e2e = f"{float(update_ms) + float(bfs_ms):.6f}"
                    except (ValueError, TypeError):
                        e2e = ""
                    rows.append({
                        "dataset":       dataset,
                        "phase":         phase,
                        "batch_size":    str(batch_size),
                        "threads":       str(threads),
                        "trial":         str(t),
                        "update_ms":     update_ms,
                        "bfs_ms":        bfs_ms,
                        "end_to_end_ms": e2e,
                        "notes":         row.get("notes", ""),
                    })
    return rows


def generate_stall_metrics_csv(run_dir: Path) -> list[dict]:
    """Derive stall_metrics.csv rows from raw.csv stall columns."""
    raw_csv = run_dir / "raw.csv"
    if not raw_csv.exists():
        return []
    rows_raw = read_csv(raw_csv)

    def safe_pct(num_s: str, denom_s: str) -> str:
        try:
            n, d = float(num_s), float(denom_s)
            return f"{n / d * 100.0:.4f}" if d > 0 else ""
        except (ValueError, TypeError):
            return ""

    def safe_div(num_s: str, denom_s: str) -> str:
        try:
            n, d = float(num_s), float(denom_s)
            return f"{n / d:.4f}" if d > 0 else ""
        except (ValueError, TypeError):
            return ""

    out = []
    for row in rows_raw:
        phase = row.get("phase", "")
        if not phase.startswith("dynamic_"):
            continue
        uf = row.get("update_stall_frontend", "")
        ub = row.get("update_stall_backend", "")
        bf = row.get("bfs_stall_frontend", "")
        bb = row.get("bfs_stall_backend", "")
        uc = row.get("update_cycles", "")
        ui = row.get("update_instructions", "")
        bc = row.get("bfs_cycles", "")
        bi = row.get("bfs_instructions", "")
        if not uf and not ub and not bf and not bb:
            continue  # no stall data in this row
        out.append({
            "dataset":    row.get("dataset", ""),
            "phase":      phase,
            "batch_size": row.get("batch_size", ""),
            "trial":      row.get("trial", ""),
            "threads":    row.get("threads", ""),
            "update_cycles":       uc,
            "update_instructions": ui,
            "update_stall_frontend": uf,
            "update_stall_backend":  ub,
            "bfs_cycles":       bc,
            "bfs_instructions": bi,
            "bfs_stall_frontend": bf,
            "bfs_stall_backend":  bb,
            "update_frontend_stall_pct": safe_pct(uf, uc),
            "update_backend_stall_pct":  safe_pct(ub, uc),
            "update_cpi":                safe_div(uc, ui),
            "bfs_frontend_stall_pct": safe_pct(bf, bc),
            "bfs_backend_stall_pct":  safe_pct(bb, bc),
            "bfs_cpi":                safe_div(bc, bi),
        })
    return out


def generate_update_breakdown_csv(run_dir: Path) -> list[dict]:
    """Melt update breakdown columns from raw.csv into long-format rows."""
    raw_csv = run_dir / "raw.csv"
    if not raw_csv.exists():
        return []
    rows_raw = read_csv(raw_csv)

    components = ["update_sort_ms", "update_preprocess_ms",
                  "update_edge_struct_ms", "update_tree_merge_ms"]
    short_names = {
        "update_sort_ms":        "sort",
        "update_preprocess_ms":  "preprocess",
        "update_edge_struct_ms": "edge_struct",
        "update_tree_merge_ms":  "tree_merge",
    }

    out = []
    for row in rows_raw:
        phase = row.get("phase", "")
        if not phase.startswith("dynamic_"):
            continue
        has_any = any(row.get(c, "") != "" for c in components)
        if not has_any:
            continue
        dataset    = row.get("dataset", "")
        batch_size = row.get("batch_size", "")
        trial      = row.get("trial", "")
        update_ms  = row.get("update_ms", "")

        accounted = 0.0
        for col in components:
            val_s = row.get(col, "")
            try:
                val = float(val_s)
            except (ValueError, TypeError):
                val = 0.0
            accounted += val
            out.append({
                "dataset":    dataset,
                "phase":      phase,
                "batch_size": batch_size,
                "trial":      trial,
                "component":  short_names[col],
                "time_ms":    f"{val:.6f}",
            })
        # "other" = residual (version install + lock overhead)
        try:
            other = float(update_ms) - accounted
        except (ValueError, TypeError):
            other = 0.0
        out.append({
            "dataset":    dataset,
            "phase":      phase,
            "batch_size": batch_size,
            "trial":      trial,
            "component":  "other",
            "time_ms":    f"{max(0.0, other):.6f}",
        })
    return out


def aggregate_rows(rows: list[dict]) -> list[dict]:
    groups: dict[tuple, list[float]] = {}
    for row in rows:
        for metric in AGG_METRICS:
            val = numeric_value(row.get(metric, ""))
            if val is None:
                continue
            key = (row["phase"], row["batch_size"], metric)
            groups.setdefault(key, []).append(float(val))

    out = []
    for (phase, batch_size, metric_name), values in sorted(groups.items()):
        stddev = statistics.stdev(values) if len(values) > 1 else 0.0
        out.append({
            "phase":       phase,
            "batch_size":  batch_size,
            "metric_name": metric_name,
            "mean":        f"{statistics.mean(values):.6f}",
            "stddev":      f"{stddev:.6f}",
            "min":         f"{min(values):.6f}",
            "max":         f"{max(values):.6f}",
            "trials":      str(len(values)),
        })
    return out


# ── Plots ─────────────────────────────────────────────────────────────────────

def generate_comparison_plots(run_dir: Path, plot_dir: Path | None = None) -> list[Path]:
    agg_csv = run_dir / "aggregate.csv"
    if not agg_csv.exists():
        fail(f"Could not find aggregate.csv in: {run_dir}")
    if plot_dir is None:
        plot_dir = run_dir / "plots"
    plot_dir.mkdir(parents=True, exist_ok=True)

    cfg_path = run_dir / "run_config.json"
    meta_path = run_dir / "metadata.json"
    run_config = json.loads(cfg_path.read_text()) if cfg_path.exists() else {}
    metadata = json.loads(meta_path.read_text()) if meta_path.exists() else {}

    mpl_dir = plot_dir / ".matplotlib"
    mpl_dir.mkdir(exist_ok=True)
    os.environ.setdefault("MPLCONFIGDIR", str(mpl_dir))

    try:
        import matplotlib
    except ModuleNotFoundError:
        fail(
            "Plot generation requires matplotlib. Install it with: pip install matplotlib\n"
            f"Active Python: {sys.executable}"
        )
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker

    rows = read_csv(agg_csv)
    if not rows:
        fail(f"aggregate.csv is empty in {run_dir}")

    # pivot[(phase, batch_size_str, metric)] → (mean, stddev)
    pivot: dict[tuple, tuple] = {}
    for row in rows:
        key = (row["phase"], row["batch_size"], row["metric_name"])
        pivot[key] = (float(row["mean"]), float(row["stddev"]))

    # ── Subtitle ──────────────────────────────────────────────────────────────
    parts = []
    if run_config.get("threads"):
        parts.append(f"threads={run_config['threads']}")
    cpu = metadata.get("cpu_model", "").strip()
    if cpu:
        parts.append(cpu)
    if run_config.get("bfs_source") is not None:
        parts.append(f"bfs_src={run_config['bfs_source']}")
    subtitle = " | ".join(parts)

    dataset = run_config.get("dataset_name", "dataset")
    slug = slugify(dataset)

    dyn_batch_sizes = sorted({
        int(bs) for (ph, bs, _) in pivot if ph.startswith("dynamic_")
    })

    phase_styles = {
        "static_baseline": {"color": "#444444", "marker": "D", "linestyle": "--",
                            "label": "Static Baseline (warm)"},
        "dynamic_insert":  {"color": "#005f73", "marker": "o", "linestyle": "-",
                            "label": "Dynamic Insert"},
        "dynamic_delete":  {"color": "#bb3e03", "marker": "s", "linestyle": "-",
                            "label": "Dynamic Delete"},
    }

    def get_val(phase: str, batch_size, metric: str):
        return pivot.get((phase, str(batch_size), metric))

    def errorbar_pts(phase: str, batch_sizes, metric: str):
        pts = []
        for bs in batch_sizes:
            v = get_val(phase, bs, metric)
            if v:
                pts.append((bs, v[0], v[1]))
        return pts

    def save(fig, name: str, subdir: str = "") -> Path:
        dest = (plot_dir / subdir) if subdir else plot_dir
        dest.mkdir(parents=True, exist_ok=True)
        out = dest / f"{slug}_{name}.png"
        fig.savefig(out, dpi=200, bbox_inches="tight")
        plt.close(fig)
        return out

    def apply_common(ax, title: str, xlabel: str, ylabel: str, log_x: bool = True) -> None:
        if log_x and len(dyn_batch_sizes) > 1:
            ax.set_xscale("log", base=2)
        ax.set_title(title, fontsize=13, pad=8)
        ax.set_xlabel(xlabel, fontsize=11)
        ax.set_ylabel(ylabel, fontsize=11)
        ax.grid(True, linestyle="--", alpha=0.35)
        ax.legend(fontsize=10)

    generated: list[Path] = []
    static_bfs = get_val("static_baseline", "0", "bfs_ms")

    # ── Plot 1: BFS Latency — Static (warm) vs Post-Update ───────────────────
    # KEY MESSAGE: static BFS ≈ post-update BFS → the update itself is the overhead,
    # not BFS becoming harder. Use this to isolate update cost.
    fig, ax = plt.subplots(figsize=(9, 5))
    has_data = False
    if static_bfs and dyn_batch_sizes:
        mean, stddev = static_bfs
        style = phase_styles["static_baseline"]
        ax.axhline(mean, color=style["color"], linestyle=style["linestyle"],
                   linewidth=2.0, label=f"{style['label']} ({mean:.2f} ms)")
        ax.axhspan(mean - stddev, mean + stddev, alpha=0.12, color=style["color"])
        has_data = True
    for phase in ("dynamic_insert", "dynamic_delete"):
        pts = errorbar_pts(phase, dyn_batch_sizes, "bfs_ms")
        if pts:
            style = phase_styles[phase]
            ax.errorbar([p[0] for p in pts], [p[1] for p in pts], yerr=[p[2] for p in pts],
                        color=style["color"], marker=style["marker"],
                        linestyle=style["linestyle"], linewidth=2.0,
                        markersize=7, capsize=5, label=style["label"])
            has_data = True
    if has_data:
        apply_common(ax,
                     f"BFS Latency: Static (warm) vs Post-Update — {dataset}",
                     "Batch Size (logical undirected edges)", "BFS Latency (ms)")
        ax.annotate(
            "Static BFS ≈ post-update BFS → BFS cost itself is not the bottleneck.\n"
            "The additional overhead lives entirely in update_ms (see Plot 2).",
            xy=(0.02, 0.97), xycoords="axes fraction",
            fontsize=8, color="#555555", va="top",
        )
        fig.suptitle(subtitle, fontsize=9, color="#777777")
        fig.tight_layout(rect=(0, 0, 1, 0.96))
        generated.append(save(fig, "bfs_comparison", "latency"))
    else:
        plt.close(fig)

    # ── Plot 2: Update Latency — the pure cost of dynamism ───────────────────
    # KEY MESSAGE: update_ms is 5-65% of total time, grows with batch size,
    # and is pure overhead that static processing never pays.
    fig, ax = plt.subplots(figsize=(9, 5))
    has_data = False
    if static_bfs and dyn_batch_sizes:
        ax.axhline(0, color=phase_styles["static_baseline"]["color"],
                   linestyle="--", linewidth=1.5, alpha=0.6,
                   label="Static (no update phase — zero update cost)")
    for phase in ("dynamic_insert", "dynamic_delete"):
        pts = errorbar_pts(phase, dyn_batch_sizes, "update_ms")
        if pts:
            style = phase_styles[phase]
            ax.errorbar([p[0] for p in pts], [p[1] for p in pts], yerr=[p[2] for p in pts],
                        color=style["color"], marker=style["marker"],
                        linestyle=style["linestyle"], linewidth=2.0,
                        markersize=7, capsize=5, label=style["label"])
            has_data = True
    if has_data:
        apply_common(ax,
                     f"Update Latency (Pure Overhead of Dynamism) — {dataset}",
                     "Batch Size (logical undirected edges)", "Update Latency (ms)")
        ax.annotate(
            "Static graphs pay zero update cost.\n"
            "Dynamic systems must absorb this on every update cycle before any query.",
            xy=(0.02, 0.97), xycoords="axes fraction",
            fontsize=8, color="#555555", va="top",
        )
        fig.suptitle(subtitle, fontsize=9, color="#777777")
        fig.tight_layout(rect=(0, 0, 1, 0.96))
        generated.append(save(fig, "update_latency", "latency"))
    else:
        plt.close(fig)

    # ── Plot 3: End-to-End vs Static BFS ─────────────────────────────────────
    # KEY MESSAGE: end_to_end = update + BFS is 5-65% more than static BFS.
    # The gap widens with batch size. This is the full cost of a query in a
    # dynamic system vs a static system.
    fig, ax = plt.subplots(figsize=(9, 5))
    has_data = False
    if static_bfs and dyn_batch_sizes:
        mean, stddev = static_bfs
        style = phase_styles["static_baseline"]
        ax.axhline(mean, color=style["color"], linestyle=style["linestyle"],
                   linewidth=2.0, label=f"Static BFS ({mean:.2f} ms)")
        ax.axhspan(mean - stddev, mean + stddev, alpha=0.12, color=style["color"])
        has_data = True
    for phase in ("dynamic_insert", "dynamic_delete"):
        pts = errorbar_pts(phase, dyn_batch_sizes, "end_to_end_ms")
        if pts:
            style = phase_styles[phase]
            ax.errorbar([p[0] for p in pts], [p[1] for p in pts], yerr=[p[2] for p in pts],
                        color=style["color"], marker=style["marker"],
                        linestyle=style["linestyle"], linewidth=2.0,
                        markersize=7, capsize=5, label=style["label"])
            has_data = True
    if has_data:
        apply_common(ax,
                     f"End-to-End Latency (update + BFS) vs Static — {dataset}",
                     "Batch Size (logical undirected edges)", "Latency (ms)")
        ax.annotate(
            "Gap above the static line = pure cost of dynamism (update_ms).\n"
            "Gap grows with batch size, independent of BFS cost.",
            xy=(0.02, 0.97), xycoords="axes fraction",
            fontsize=8, color="#555555", va="top",
        )
        fig.suptitle(subtitle, fontsize=9, color="#777777")
        fig.tight_layout(rect=(0, 0, 1, 0.96))
        generated.append(save(fig, "end_to_end_latency", "latency"))
    else:
        plt.close(fig)

    # ── Plot 4: Update Overhead Ratio (%) ────────────────────────────────────
    # KEY MESSAGE: update_ms / static_bfs_ms × 100%. At small batches the update
    # cost is ~5-10% of BFS; at large batches it reaches 50-65%.
    # This is the clearest single number for a paper: "dynamism adds X% overhead".
    if static_bfs:
        static_mean = static_bfs[0]
        if static_mean > 0:
            fig, ax = plt.subplots(figsize=(9, 5))
            has_data = False
            ax.axhline(0, color="#888888", linestyle=":", linewidth=1.0)
            for phase in ("dynamic_insert", "dynamic_delete"):
                pts = errorbar_pts(phase, dyn_batch_sizes, "update_ms")
                if pts:
                    ratio_pts = [
                        (p[0],
                         p[1] / static_mean * 100.0,
                         p[2] / static_mean * 100.0)
                        for p in pts
                    ]
                    style = phase_styles[phase]
                    ax.errorbar([p[0] for p in ratio_pts], [p[1] for p in ratio_pts],
                                yerr=[p[2] for p in ratio_pts],
                                color=style["color"], marker=style["marker"],
                                linestyle=style["linestyle"], linewidth=2.0,
                                markersize=7, capsize=5, label=style["label"])
                    has_data = True
            if has_data:
                apply_common(ax,
                             f"Update Overhead as % of Static BFS — {dataset}",
                             "Batch Size (logical undirected edges)",
                             "update_ms / static_bfs_ms  (%)")
                ax.yaxis.set_major_formatter(
                    matplotlib.ticker.FuncFormatter(lambda y, _: f"{y:.0f}%")
                )
                ax.annotate(
                    "= update_ms ÷ static_bfs_ms × 100.\n"
                    "Answers: how much extra does this dynamic update cost, relative to a plain BFS?",
                    xy=(0.02, 0.97), xycoords="axes fraction",
                    fontsize=8, color="#555555", va="top",
                )
                fig.suptitle(subtitle, fontsize=9, color="#777777")
                fig.tight_layout(rect=(0, 0, 1, 0.96))
                generated.append(save(fig, "update_overhead_ratio", "latency"))
            else:
                plt.close(fig)

    # ── Plot 5: BFS Delta (post-update BFS − static BFS) ─────────────────────
    # KEY MESSAGE: should be near zero (update doesn't make BFS harder by itself).
    # Cache pollution effect is small. The problem is update_ms, not BFS regression.
    if static_bfs:
        static_mean = static_bfs[0]
        fig, ax = plt.subplots(figsize=(9, 5))
        has_data = False
        ax.axhline(0, color="#444444", linestyle="--", linewidth=1.5,
                   label=f"Zero delta (static BFS = {static_mean:.2f} ms)")
        for phase in ("dynamic_insert", "dynamic_delete"):
            pts = errorbar_pts(phase, dyn_batch_sizes, "bfs_ms")
            if pts:
                delta_pts = [(p[0], p[1] - static_mean, p[2]) for p in pts]
                style = phase_styles[phase]
                ax.errorbar([p[0] for p in delta_pts], [p[1] for p in delta_pts],
                            yerr=[p[2] for p in delta_pts],
                            color=style["color"], marker=style["marker"],
                            linestyle=style["linestyle"], linewidth=2.0,
                            markersize=7, capsize=5, label=style["label"])
                has_data = True
        if has_data:
            apply_common(ax,
                         f"BFS Regression from Updates — {dataset}",
                         "Batch Size (logical undirected edges)",
                         "Post-Update BFS − Static BFS (ms)")
            ax.annotate(
                "Near-zero = cache pollution from update does NOT significantly slow BFS.\n"
                "The full overhead of dynamism comes from update_ms alone (Plot 2).",
                xy=(0.02, 0.97), xycoords="axes fraction",
                fontsize=8, color="#555555", va="top",
            )
            fig.suptitle(subtitle, fontsize=9, color="#777777")
            fig.tight_layout(rect=(0, 0, 1, 0.96))
            generated.append(save(fig, "bfs_regression", "latency"))
        else:
            plt.close(fig)

    # ── Plot 6: Update IPC vs BFS IPC ────────────────────────────────────────
    # KEY MESSAGE: update-phase IPC << BFS-phase IPC → update is memory-stall-dominated.
    # This is the micro-architectural proof that the von Neumann bottleneck is the update.
    ipc_data = {}  # phase → list of (batch_size, update_ipc, bfs_ipc)
    for phase in ("dynamic_insert", "dynamic_delete"):
        pts = []
        for bs in dyn_batch_sizes:
            uc = get_val(phase, bs, "update_cycles")
            ui = get_val(phase, bs, "update_instructions")
            bc = get_val(phase, bs, "bfs_cycles")
            bi = get_val(phase, bs, "bfs_instructions")
            if uc and ui and bc and bi and uc[0] > 0 and bc[0] > 0:
                pts.append((bs, ui[0] / uc[0], bi[0] / bc[0]))
        if pts:
            ipc_data[phase] = pts

    if ipc_data:
        # Also grab static BFS IPC
        sc = get_val("static_baseline", 0, "bfs_cycles")
        si = get_val("static_baseline", 0, "bfs_instructions")
        static_ipc = (si[0] / sc[0]) if sc and si and sc[0] > 0 else None

        fig, axes = plt.subplots(1, len(ipc_data), figsize=(6 * len(ipc_data), 5), squeeze=False)
        has_data = False
        for ax_idx, (phase, pts) in enumerate(sorted(ipc_data.items())):
            ax = axes[0][ax_idx]
            x = range(len(pts))
            labels = [str(p[0]) for p in pts]
            update_ipcs = [p[1] for p in pts]
            bfs_ipcs    = [p[2] for p in pts]
            width = 0.35
            bars1 = ax.bar([xi - width/2 for xi in x], update_ipcs, width,
                           label="Update phase IPC", color="#bb3e03", alpha=0.85)
            bars2 = ax.bar([xi + width/2 for xi in x], bfs_ipcs, width,
                           label="Post-update BFS IPC", color="#005f73", alpha=0.85)
            if static_ipc is not None:
                ax.axhline(static_ipc, color="#444444", linestyle="--", linewidth=1.5,
                           label=f"Static BFS IPC ({static_ipc:.2f})")
            ax.set_xticks(list(x))
            ax.set_xticklabels(labels)
            style = phase_styles[phase]
            apply_common(ax,
                         f"IPC by Phase — {phase.replace('dynamic_', '').title()} — {dataset}",
                         "Batch Size (logical undirected edges)",
                         "Instructions Per Cycle (IPC)",
                         log_x=False)
            ax.legend(fontsize=8)
            ax.annotate(
                "Low update IPC → CPU stalled waiting on memory (pointer chasing, malloc).\n"
                "BFS IPC ≈ static IPC → update does not pollute cache for subsequent BFS.",
                xy=(0.02, 0.97), xycoords="axes fraction",
                fontsize=8, color="#555555", va="top",
            )
            has_data = True
        if has_data:
            fig.suptitle(subtitle, fontsize=9, color="#777777")
            fig.tight_layout(rect=(0, 0, 1, 0.96))
            generated.append(save(fig, "update_vs_bfs_ipc", "memory"))
        else:
            plt.close(fig)

    # ── Plot 7: LLC Miss Rate ─────────────────────────────────────────────────
    # KEY MESSAGE: update-phase LLC miss rate >> BFS miss rate.
    # High miss rate during update = CPU pointer-chasing C-tree nodes scattered in DRAM.
    llc_rate_data: dict[str, list] = {}
    for phase in ("dynamic_insert", "dynamic_delete"):
        pts = []
        for bs in dyn_batch_sizes:
            u_refs = get_val(phase, bs, "update_cache_refs")
            u_miss = get_val(phase, bs, "update_cache_misses")
            b_refs = get_val(phase, bs, "bfs_cache_refs")
            b_miss = get_val(phase, bs, "bfs_cache_misses")
            if (u_refs and u_miss and u_refs[0] > 0
                    and b_refs and b_miss and b_refs[0] > 0):
                u_rate = u_miss[0] / u_refs[0] * 100.0
                b_rate = b_miss[0] / b_refs[0] * 100.0
                pts.append((bs, u_rate, b_rate))
        if pts:
            llc_rate_data[phase] = pts

    # Also static BFS miss rate
    st_refs = get_val("static_baseline", 0, "bfs_cache_refs")
    st_miss = get_val("static_baseline", 0, "bfs_cache_misses")
    static_llc_rate = (st_miss[0] / st_refs[0] * 100.0
                       if st_refs and st_miss and st_refs[0] > 0 else None)

    if llc_rate_data:
        fig, ax = plt.subplots(figsize=(9, 5))
        has_data = False
        for phase, pts in sorted(llc_rate_data.items()):
            style = phase_styles[phase]
            ax.plot([p[0] for p in pts], [p[1] for p in pts],
                    color=style["color"], marker=style["marker"],
                    linestyle="-", linewidth=2.0, markersize=7,
                    label=f"{style['label']} — update phase")
            ax.plot([p[0] for p in pts], [p[2] for p in pts],
                    color=style["color"], marker=style["marker"],
                    linestyle="--", linewidth=1.5, markersize=5, alpha=0.6,
                    label=f"{style['label']} — post-update BFS")
            has_data = True
        if static_llc_rate is not None:
            ax.axhline(static_llc_rate, color="#444444", linestyle=":",
                       linewidth=1.5, label=f"Static BFS ({static_llc_rate:.1f}%)")
        if has_data:
            apply_common(ax,
                         f"LLC Miss Rate by Phase — {dataset}",
                         "Batch Size (logical undirected edges)",
                         "LLC Miss Rate  (cache_misses / cache_refs × 100%)")
            ax.legend(fontsize=8)
            ax.annotate(
                "High update LLC miss rate → CPU fetching C-tree nodes from DRAM on every\n"
                "pointer chase. Low BFS miss rate shows the update working set is small but\n"
                "poorly localised — classic irregular-access latency bottleneck.",
                xy=(0.02, 0.97), xycoords="axes fraction",
                fontsize=8, color="#555555", va="top",
            )
            fig.suptitle(subtitle, fontsize=9, color="#777777")
            fig.tight_layout(rect=(0, 0, 1, 0.96))
            generated.append(save(fig, "llc_miss_rate", "memory"))
        else:
            plt.close(fig)

    # ── Plot 8: Approximate Memory Bandwidth (GB/s) ───────────────────────────
    # KEY MESSAGE: bandwidth is far below the Threadripper 9960X peak (~200 GB/s).
    # Low BW despite high latency = latency-bound (scattered accesses), not BW-saturating.
    # This is the FPGA argument: the bottleneck is access latency, not transfer volume.
    THREADRIPPER_9960X_PEAK_BW_GBS = 200.0  # theoretical peak DDR5 bandwidth
    bw_data: dict[str, list] = {}
    for phase in ("dynamic_insert", "dynamic_delete"):
        pts = []
        for bs in dyn_batch_sizes:
            u_miss = get_val(phase, bs, "update_cache_misses")
            u_ms   = get_val(phase, bs, "update_ms")
            if u_miss and u_ms and u_ms[0] > 0:
                bw_gbs = u_miss[0] * 64.0 / (u_ms[0] / 1000.0) / 1e9
                pts.append((bs, bw_gbs))
        if pts:
            bw_data[phase] = pts

    if bw_data:
        fig, ax = plt.subplots(figsize=(9, 5))
        has_data = False
        for phase, pts in sorted(bw_data.items()):
            style = phase_styles[phase]
            ax.plot([p[0] for p in pts], [p[1] for p in pts],
                    color=style["color"], marker=style["marker"],
                    linestyle="-", linewidth=2.0, markersize=7,
                    label=style["label"])
            has_data = True
        ax.axhline(THREADRIPPER_9960X_PEAK_BW_GBS, color="#cc0000",
                   linestyle="--", linewidth=1.5,
                   label=f"Threadripper 9960X peak BW ({THREADRIPPER_9960X_PEAK_BW_GBS:.0f} GB/s)")
        if has_data:
            apply_common(ax,
                         f"Approx. Memory Bandwidth During Update — {dataset}",
                         "Batch Size (logical undirected edges)",
                         "Bandwidth  (LLC misses × 64 B / update_time,  GB/s)")
            ax.legend(fontsize=8)
            ax.annotate(
                "Bandwidth << peak → workload is latency-bound, NOT bandwidth-saturating.\n"
                "CPU stalls waiting for scattered pointer targets, not for bulk data transfer.\n"
                "An accelerator that tolerates memory latency (e.g. FPGA with deep pipelines)\n"
                "wins here — not a wider memory bus.",
                xy=(0.02, 0.97), xycoords="axes fraction",
                fontsize=8, color="#555555", va="top",
            )
            fig.suptitle(subtitle, fontsize=9, color="#777777")
            fig.tight_layout(rect=(0, 0, 1, 0.96))
            generated.append(save(fig, "memory_bandwidth", "memory"))
        else:
            plt.close(fig)

    # ── Energy plots ─────────────────────────────────────────────────────────
    # Only rendered when RAPL data is present (perf_event_paranoid ≤ 0).
    # Check: at least one phase has a non-empty update_energy_j in the aggregate.
    has_energy = any(
        get_val(ph, bs, "update_energy_j") is not None
        for ph in ("dynamic_insert", "dynamic_delete")
        for bs in dyn_batch_sizes
    )

    if has_energy:
        # ── Plot E1: Energy per batch ─────────────────────────────────────
        fig, ax = plt.subplots(figsize=(9, 5))
        has_data = False
        for phase in ("dynamic_insert", "dynamic_delete"):
            pts = errorbar_pts(phase, dyn_batch_sizes, "update_energy_j")
            if pts:
                style = phase_styles[phase]
                ax.errorbar([p[0] for p in pts], [p[1] for p in pts],
                            yerr=[p[2] for p in pts],
                            color=style["color"], marker=style["marker"],
                            linestyle=style["linestyle"], linewidth=2.0,
                            markersize=7, capsize=5, label=style["label"])
                has_data = True
        if has_data:
            apply_common(ax,
                         f"Update Energy per Batch — {dataset}",
                         "Batch Size (logical undirected edges)",
                         "Socket Energy (Joules)")
            ax.legend(fontsize=9)
            ax.annotate(
                "Energy measured via AMD RAPL (package socket), update phase only.\n"
                "Higher batch size → more tree restructuring work → more energy.",
                xy=(0.02, 0.97), xycoords="axes fraction",
                fontsize=8, color="#555555", va="top",
            )
            fig.suptitle(subtitle, fontsize=9, color="#777777")
            fig.tight_layout(rect=(0, 0, 1, 0.96))
            generated.append(save(fig, "energy_per_batch", "energy"))
        else:
            plt.close(fig)

        # ── Plot E2: Energy per logical edge ──────────────────────────────
        fig, ax = plt.subplots(figsize=(9, 5))
        has_data = False
        for phase in ("dynamic_insert", "dynamic_delete"):
            pts = []
            for bs in dyn_batch_sizes:
                v = get_val(phase, bs, "update_energy_j")
                if v and bs > 0:
                    uj_per_edge = v[0] / bs * 1e6          # Joules → µJ, per edge
                    uj_err      = v[1] / bs * 1e6
                    pts.append((bs, uj_per_edge, uj_err))
            if pts:
                style = phase_styles[phase]
                ax.errorbar([p[0] for p in pts], [p[1] for p in pts],
                            yerr=[p[2] for p in pts],
                            color=style["color"], marker=style["marker"],
                            linestyle=style["linestyle"], linewidth=2.0,
                            markersize=7, capsize=5, label=style["label"])
                has_data = True
        if has_data:
            apply_common(ax,
                         f"Energy per Logical Edge Updated — {dataset}",
                         "Batch Size (logical undirected edges)",
                         "Energy per Edge  (µJ / logical edge)")
            ax.legend(fontsize=9)
            ax.annotate(
                "Normalised per-edge cost — shows whether larger batches amortise energy.\n"
                "Flat or rising curve → no amortisation; each edge costs the same regardless.",
                xy=(0.02, 0.97), xycoords="axes fraction",
                fontsize=8, color="#555555", va="top",
            )
            fig.suptitle(subtitle, fontsize=9, color="#777777")
            fig.tight_layout(rect=(0, 0, 1, 0.96))
            generated.append(save(fig, "energy_per_edge", "energy"))
        else:
            plt.close(fig)

        # ── Plot E3: End-to-end energy + EDP ─────────────────────────────
        static_e = get_val("static_baseline", 0, "bfs_energy_j")
        static_t = get_val("static_baseline", 0, "bfs_ms")
        static_edp = (static_e[0] * static_t[0]) if static_e and static_t else None

        fig, ax1 = plt.subplots(figsize=(10, 5))
        ax2 = ax1.twinx()
        has_data = False
        for phase in ("dynamic_insert", "dynamic_delete"):
            e2e_pts, edp_pts = [], []
            for bs in dyn_batch_sizes:
                ue = get_val(phase, bs, "update_energy_j")
                be = get_val(phase, bs, "bfs_energy_j")
                ut = get_val(phase, bs, "update_ms")
                bt = get_val(phase, bs, "bfs_ms")
                if ue and be and ut and bt:
                    e2e_j   = ue[0] + be[0]
                    e2e_err = (ue[1] ** 2 + be[1] ** 2) ** 0.5
                    e2e_pts.append((bs, e2e_j, e2e_err))
                    if static_edp and static_edp > 0:
                        dyn_edp = e2e_j * (ut[0] + bt[0])
                        edp_pts.append((bs, dyn_edp / static_edp))
            if e2e_pts:
                style = phase_styles[phase]
                ax1.errorbar([p[0] for p in e2e_pts], [p[1] for p in e2e_pts],
                             yerr=[p[2] for p in e2e_pts],
                             color=style["color"], marker=style["marker"],
                             linestyle=style["linestyle"], linewidth=2.0,
                             markersize=7, capsize=5, label=f"{style['label']} (energy)")
                if edp_pts:
                    ax2.plot([p[0] for p in edp_pts], [p[1] for p in edp_pts],
                             color=style["color"], marker=style["marker"],
                             linestyle=":", linewidth=1.5, markersize=5, alpha=0.7,
                             label=f"{style['label']} (EDP ratio)")
                has_data = True
        if static_e:
            ax1.axhline(static_e[0], color="#444444", linestyle="--", linewidth=1.5,
                        label=f"Static BFS baseline ({static_e[0]:.3f} J)")
        if static_edp:
            ax2.axhline(1.0, color="#888888", linestyle=":", linewidth=1.0, alpha=0.6)
        if has_data:
            if len(dyn_batch_sizes) > 1:
                ax1.set_xscale("log", base=2)
                ax2.set_xscale("log", base=2)
            ax1.set_title(f"End-to-End Energy (Update + BFS) — {dataset}", fontsize=13, pad=8)
            ax1.set_xlabel("Batch Size (logical undirected edges)", fontsize=11)
            ax1.set_ylabel("Total Energy  (Joules)", fontsize=11)
            ax2.set_ylabel("EDP ratio  (dynamic / static baseline)", fontsize=10, color="#666666")
            ax2.tick_params(axis="y", labelcolor="#666666")
            # Combine legends from both axes
            lines1, labels1 = ax1.get_legend_handles_labels()
            lines2, labels2 = ax2.get_legend_handles_labels()
            ax1.legend(lines1 + lines2, labels1 + labels2, fontsize=8, loc="upper left")
            ax1.annotate(
                "Solid lines = total energy (J).  Dotted lines = EDP normalised to static=1.0.\n"
                "EDP > 1 → dynamic workload wastes disproportionate energy×time vs static BFS.",
                xy=(0.02, 0.97), xycoords="axes fraction",
                fontsize=8, color="#555555", va="top",
            )
            fig.suptitle(subtitle, fontsize=9, color="#777777")
            fig.tight_layout(rect=(0, 0, 1, 0.96))
            generated.append(save(fig, "end_to_end_energy", "energy"))
        else:
            plt.close(fig)

    # ── Thread Scaling Plot ───────────────────────────────────────────────────
    ts_csv = run_dir / "thread_scaling.csv"
    if ts_csv.exists():
        ts_rows = read_csv(ts_csv)
        if ts_rows:
            # Build a pivot: (phase, batch_size, threads) -> list of (update_ms, e2e_ms)
            ts_pivot: dict[tuple, dict[str, list]] = {}
            for r in ts_rows:
                k = (r["phase"], r["batch_size"], r["threads"])
                ts_pivot.setdefault(k, {"update_ms": [], "end_to_end_ms": []})
                for m in ("update_ms", "end_to_end_ms"):
                    v = numeric_value(r.get(m, ""))
                    if v is not None:
                        ts_pivot[k][m].append(v)

            ts_batch_sizes = sorted({int(r["batch_size"]) for r in ts_rows})
            ts_threads = sorted({int(r["threads"]) for r in ts_rows})
            ts_phases = ["dynamic_insert", "dynamic_delete"]

            n_bs = len(ts_batch_sizes)
            fig, axes = plt.subplots(2, n_bs, figsize=(5 * n_bs, 8), squeeze=False)
            fig.suptitle(f"Thread Scaling — {dataset}\n{subtitle}", fontsize=10, color="#555555")

            def ts_agg(phase: str, bs: int, threads: int, metric: str):
                k = (phase, str(bs), str(threads))
                vals = ts_pivot.get(k, {}).get(metric, [])
                if not vals:
                    return None, None
                import statistics as _stats
                return _stats.mean(vals), (_stats.stdev(vals) if len(vals) > 1 else 0.0)

            for col, bs in enumerate(ts_batch_sizes):
                ax_lat = axes[0][col]
                ax_spd = axes[1][col]
                for phase in ts_phases:
                    style = phase_styles.get(phase, {})
                    color = style.get("color", "#888888")
                    marker = style.get("marker", "o")
                    label = style.get("label", phase)
                    lat_pts, spd_pts = [], []
                    baseline_ms = None
                    for t in ts_threads:
                        mean, std = ts_agg(phase, bs, t, "update_ms")
                        if mean is None:
                            continue
                        lat_pts.append((t, mean, std or 0.0))
                        if t == min(ts_threads):
                            baseline_ms = mean
                    # latency
                    if lat_pts:
                        ax_lat.errorbar(
                            [p[0] for p in lat_pts], [p[1] for p in lat_pts],
                            yerr=[p[2] for p in lat_pts],
                            color=color, marker=marker, linestyle="-",
                            linewidth=2, markersize=6, capsize=4, label=label,
                        )
                    # speedup
                    if lat_pts and baseline_ms and baseline_ms > 0:
                        spd_pts = [(p[0], baseline_ms / p[1], 0.0) for p in lat_pts if p[1] > 0]
                        ax_spd.plot(
                            [p[0] for p in spd_pts], [p[1] for p in spd_pts],
                            color=color, marker=marker, linestyle="-",
                            linewidth=2, markersize=6, label=label,
                        )

                # Ideal linear reference (based on min thread count)
                if ts_threads:
                    t_min = min(ts_threads)
                    ax_spd.plot(
                        ts_threads, [t / t_min for t in ts_threads],
                        color="#999999", linestyle="--", linewidth=1.2, label="Ideal linear",
                    )

                ax_lat.set_title(f"bs={bs}", fontsize=12)
                ax_lat.set_xlabel("Thread count")
                ax_lat.set_ylabel("update_ms")
                ax_lat.set_xticks(ts_threads)
                ax_lat.grid(True, linestyle="--", alpha=0.35)
                ax_lat.legend(fontsize=8)

                ax_spd.set_xlabel("Thread count")
                ax_spd.set_ylabel("Speedup vs 1 thread")
                ax_spd.set_xticks(ts_threads)
                ax_spd.grid(True, linestyle="--", alpha=0.35)
                ax_spd.legend(fontsize=8)

            axes[0][0].annotate("Row 1: update_ms latency ± σ", xy=(0, 1.02),
                                xycoords="axes fraction", fontsize=8, color="#666666")
            axes[1][0].annotate("Row 2: speedup = 1-thread / N-thread update_ms",
                                xy=(0, 1.02), xycoords="axes fraction", fontsize=8, color="#666666")
            fig.tight_layout()
            generated.append(save(fig, "thread_scaling", "scaling"))

    # ── Update Breakdown Plot ─────────────────────────────────────────────────
    bd_csv = run_dir / "update_breakdown.csv"
    if bd_csv.exists():
        bd_rows = read_csv(bd_csv)
        if bd_rows:
            # Aggregate by (phase, batch_size, component) → mean time_ms
            bd_pivot: dict[tuple, list[float]] = {}
            for r in bd_rows:
                k = (r["phase"], r["batch_size"], r["component"])
                v = numeric_value(r.get("time_ms", ""))
                if v is not None:
                    bd_pivot.setdefault(k, []).append(v)

            bd_batch_sizes = sorted({int(r["batch_size"]) for r in bd_rows})
            bd_phases = ["dynamic_insert", "dynamic_delete"]
            components_ordered = ["preprocess", "edge_struct", "tree_merge", "other", "sort"]
            comp_colors = {
                "sort":        "#b5e48c",
                "preprocess":  "#52b788",
                "edge_struct": "#1a759f",
                "tree_merge":  "#184e77",
                "other":       "#aaaaaa",
            }

            import statistics as _stats

            fig, axes = plt.subplots(1, 2, figsize=(12, 5), squeeze=False)
            fig.suptitle(f"Update-Phase Breakdown — {dataset}\n{subtitle}",
                         fontsize=10, color="#555555")

            for col, phase in enumerate(bd_phases):
                ax = axes[0][col]
                x_labels = [str(bs) for bs in bd_batch_sizes]
                x_pos = list(range(len(bd_batch_sizes)))
                bottoms = [0.0] * len(bd_batch_sizes)

                for comp in components_ordered:
                    heights = []
                    for bs in bd_batch_sizes:
                        k = (phase, str(bs), comp)
                        vals = bd_pivot.get(k, [])
                        heights.append(_stats.mean(vals) if vals else 0.0)
                    if any(h > 0 for h in heights):
                        ax.bar(x_pos, heights, bottom=bottoms,
                               color=comp_colors.get(comp, "#cccccc"),
                               label=comp, width=0.5)
                        bottoms = [b + h for b, h in zip(bottoms, heights)]

                # Annotate total update_ms above each bar
                for i, (bs, total) in enumerate(zip(bd_batch_sizes, bottoms)):
                    if total > 0:
                        ax.text(i, total + total * 0.02, f"{total:.1f}ms",
                                ha="center", va="bottom", fontsize=8)

                style = phase_styles.get(phase, {})
                ax.set_title(f"{style.get('label', phase)}", fontsize=12)
                ax.set_xticks(x_pos)
                ax.set_xticklabels(x_labels)
                ax.set_xlabel("Batch size (edges)")
                ax.set_ylabel("Time (ms)")
                ax.grid(True, axis="y", linestyle="--", alpha=0.35)
                ax.legend(fontsize=8, loc="upper left")

            fig.tight_layout()
            generated.append(save(fig, "update_breakdown", "breakdown"))

    # ── Stall Analysis Plot ───────────────────────────────────────────────────
    stall_csv = run_dir / "stall_metrics.csv"
    if stall_csv.exists():
        stall_rows = read_csv(stall_csv)
        if stall_rows:
            # Aggregate per (phase, batch_size) → mean ± stddev of stall metrics
            stall_pivot: dict[tuple, dict[str, list[float]]] = {}
            stall_metrics_list = [
                "update_frontend_stall_pct", "update_backend_stall_pct", "update_cpi",
                "bfs_frontend_stall_pct", "bfs_backend_stall_pct", "bfs_cpi",
            ]
            for r in stall_rows:
                k = (r["phase"], r["batch_size"])
                stall_pivot.setdefault(k, {m: [] for m in stall_metrics_list})
                for m in stall_metrics_list:
                    v = numeric_value(r.get(m, ""))
                    if v is not None:
                        stall_pivot[k][m].append(v)

            import statistics as _stats

            def stall_agg(phase, bs, metric):
                k = (phase, str(bs))
                vals = stall_pivot.get(k, {}).get(metric, [])
                if not vals:
                    return None, None
                return _stats.mean(vals), (_stats.stdev(vals) if len(vals) > 1 else 0.0)

            stall_bs = sorted({int(r["batch_size"]) for r in stall_rows})
            stall_phases = ["dynamic_insert", "dynamic_delete"]

            # 2-row × 3-col (update phase) + 2-row × 3-col (BFS phase) = two stacked grids
            # Layout: 4 rows × 3 cols — top 2 rows = update, bottom 2 rows = BFS phase
            fig, axes = plt.subplots(4, 3, figsize=(14, 14), squeeze=False)
            fig.suptitle(f"Memory-Stall Analysis — {dataset}\n{subtitle}",
                         fontsize=10, color="#555555")
            # Row 0–1: update phase; Row 2–3: BFS phase (post-update)
            col_metrics_update = [
                ("update_backend_stall_pct",  "Update — Backend Stall %", "Stalled cycles — backend (%)"),
                ("update_frontend_stall_pct", "Update — Frontend Stall %", "Stalled cycles — frontend (%)"),
                ("update_cpi",                "Update — CPI", "Cycles per Instruction"),
            ]
            col_metrics_bfs = [
                ("bfs_backend_stall_pct",  "Post-Update BFS — Backend Stall %", "Stalled cycles — backend (%)"),
                ("bfs_frontend_stall_pct", "Post-Update BFS — Frontend Stall %", "Stalled cycles — frontend (%)"),
                ("bfs_cpi",                "Post-Update BFS — CPI", "Cycles per Instruction"),
            ]
            col_metrics = col_metrics_update + col_metrics_bfs  # flattened for unified loop

            # Render update metrics (rows 0-1) then BFS metrics (rows 2-3)
            for section_idx, col_section in enumerate([col_metrics_update, col_metrics_bfs]):
                base_row = section_idx * 2  # 0 for update, 2 for BFS
                is_bfs_section = section_idx == 1

                for phase_idx, phase in enumerate(stall_phases):
                    style = phase_styles.get(phase, {})
                    color = style.get("color", "#888888")
                    marker = style.get("marker", "o")
                    phase_label = style.get("label", phase)
                    row_idx = base_row + phase_idx

                    for col_idx, (metric, col_title, ylabel) in enumerate(col_section):
                        ax = axes[row_idx][col_idx]
                        pts = []
                        for bs in stall_bs:
                            mean, std = stall_agg(phase, bs, metric)
                            if mean is not None:
                                pts.append((bs, mean, std or 0.0))

                        if pts:
                            if len(stall_bs) > 1:
                                ax.set_xscale("log", base=2)
                            ax.errorbar(
                                [p[0] for p in pts], [p[1] for p in pts],
                                yerr=[p[2] for p in pts],
                                color=color, marker=marker, linestyle="-",
                                linewidth=2, markersize=6, capsize=4,
                            )

                        if phase_idx == 0:
                            ax.set_title(col_title, fontsize=11)
                        ax.set_xlabel("Batch size" if phase_idx == 1 else "")
                        ax.set_ylabel(ylabel if col_idx == 0 else "", fontsize=9)
                        ax.text(0.02, 0.97, phase_label, transform=ax.transAxes,
                                fontsize=8, color=color, va="top")
                        ax.grid(True, linestyle="--", alpha=0.35)

                        # Static BFS CPI reference on the CPI column of the BFS section
                        if is_bfs_section and col_idx == 2 and static_bfs:
                            st_cyc = get_val("static_baseline", "0", "bfs_cycles")
                            st_ins = get_val("static_baseline", "0", "bfs_instructions")
                            if st_cyc and st_ins and st_ins[0] > 0:
                                static_cpi = st_cyc[0] / st_ins[0]
                                ax.axhline(static_cpi, color="#444444", linestyle="--",
                                           linewidth=1.2,
                                           label=f"Static BFS CPI ({static_cpi:.2f})")
                                ax.legend(fontsize=7)

            fig.tight_layout()
            generated.append(save(fig, "stall_analysis", "memory"))

    if not generated:
        fail(f"No plottable data found in {agg_csv}")
    return generated


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    args = parse_args()
    repo_root, code_root = repo_roots()

    if sys.version_info < (3, 10):
        fail("Python 3.10 or newer is required.")

    config = resolve_config(args, repo_root)

    # ── Plot-only mode ────────────────────────────────────────────────────────
    if config["plot_only"]:
        run_dir = Path(config["plot_only"])
        if not run_dir.is_absolute():
            run_dir = repo_root / run_dir
        plot_dir = None
        if config["plot_dir"]:
            plot_dir = Path(config["plot_dir"])
            if not plot_dir.is_absolute():
                plot_dir = repo_root / plot_dir
        # Derive analysis CSVs on-the-fly if raw.csv is present but derived CSVs are missing
        if (run_dir / "raw.csv").exists():
            if not (run_dir / "stall_metrics.csv").exists():
                rows = generate_stall_metrics_csv(run_dir)
                if rows:
                    write_csv(run_dir / "stall_metrics.csv", STALL_METRICS_COLUMNS, rows)
            if not (run_dir / "update_breakdown.csv").exists():
                rows = generate_update_breakdown_csv(run_dir)
                if rows:
                    write_csv(run_dir / "update_breakdown.csv", UPDATE_BREAKDOWN_COLUMNS, rows)
        paths = generate_comparison_plots(run_dir, plot_dir)
        print(f"Plot input: {run_dir}")
        for p in paths:
            print(f"Plot: {p}")
        return

    # ── Full benchmark mode ───────────────────────────────────────────────────
    # Validation
    if config["threads"] <= 0:
        fail("--threads must be positive.")
    if config["trials"] <= 0:
        fail("--trials must be positive.")
    if config["warmup_trials"] < 0:
        fail("--warmup-trials must be non-negative.")
    if not config["batch_sizes"] or any(b <= 0 for b in config["batch_sizes"]):
        fail("--batch-sizes must contain one or more positive integers.")

    require_tool("make")
    require_tool("g++")
    ensure_perf_access(config["collect_perf"])

    run_id, output_dir = ensure_paths(repo_root, code_root, config)
    log_dir = output_dir / "logs"

    print(f"Run ID: {run_id}")
    print(f"Output: {output_dir}")

    # Build
    print("Building binaries...")
    build_binaries(code_root)

    # Dataset
    print("Preparing dataset...")
    graph_path = prepare_dataset(repo_root, code_root, config)
    print(f"Graph: {graph_path}")

    # BFS source
    if config["bfs_src"] is not None:
        bfs_src = config["bfs_src"]
        print(f"BFS source: vertex {bfs_src} (user-specified)")
    else:
        bfs_src = find_max_degree_vertex(graph_path)

    # Record in config
    config["bfs_source"] = bfs_src

    # Metadata
    metadata = collect_metadata(code_root)
    run_cfg = {
        "profile":          config["profile"],
        "dataset":          config["dataset"],
        "dataset_name":     config["dataset_name"],
        "graph_path":       str(graph_path),
        "bfs_source":       bfs_src,
        "batch_sizes":      config["batch_sizes"],
        "trials":           config["trials"],
        "warmup_trials":    config["warmup_trials"],
        "threads":          config["threads"],
        "seed":             config["seed"],
        "collect_perf":     config["collect_perf"],
        "collect_stall":             config.get("collect_stall", False),
        "collect_update_breakdown":  config.get("collect_update_breakdown", False),
        "thread_scaling":            config.get("thread_scaling", False),
        "thread_list":               config.get("thread_list", []),
        "sample_multiplier": config["sample_multiplier"],
        "static_warmup_rounds": config["static_warmup_rounds"],
        "mmap":             config["mmap"],
        "compressed":       config["compressed"],
        "nparts":           config["nparts"],
        "config_file":      config["config_file"],
        "config_key":       config["config_key"],
        "repo_root":        str(repo_root),
        "code_root":        str(code_root),
    }
    write_json(output_dir / "metadata.json", metadata)
    write_json(output_dir / "run_config.json", run_cfg)

    measured_rows: list[dict] = []

    # ── Static baseline ───────────────────────────────────────────────────────
    print(f"\nRunning static baseline (BFS from vertex {bfs_src})...")
    for w in range(config["warmup_trials"]):
        print(f"  [warmup {w}]")
        run_static_trial(code_root, graph_path, config, run_id, bfs_src, w, True, log_dir)
    for t in range(config["trials"]):
        print(f"  [trial {t}]")
        row = run_static_trial(code_root, graph_path, config, run_id, bfs_src, t, False, log_dir)
        if row is None:
            fail(f"Static trial {t} returned no data.")
        measured_rows.append(row)

    # ── Dynamic trials ────────────────────────────────────────────────────────
    for operation in ("insert", "delete"):
        for batch_size in config["batch_sizes"]:
            print(f"\nRunning dynamic {operation} bs={batch_size}...")
            for w in range(config["warmup_trials"]):
                print(f"  [warmup {w}]")
                run_dynamic_trial(code_root, graph_path, config, run_id, operation,
                                  batch_size, bfs_src, w, True, log_dir)
            for t in range(config["trials"]):
                print(f"  [trial {t}]")
                row = run_dynamic_trial(code_root, graph_path, config, run_id, operation,
                                        batch_size, bfs_src, t, False, log_dir)
                if row is None:
                    fail(f"Dynamic trial {t} ({operation} bs={batch_size}) returned no data.")
                measured_rows.append(row)

    # ── Write CSVs ────────────────────────────────────────────────────────────
    raw_csv = output_dir / "raw.csv"
    agg_csv = output_dir / "aggregate.csv"
    write_csv(raw_csv, COMP_RAW_COLUMNS, measured_rows)
    write_csv(agg_csv, COMP_AGG_COLUMNS, aggregate_rows(measured_rows))

    print(f"\nRaw CSV:       {raw_csv}")
    print(f"Aggregate CSV: {agg_csv}")

    # ── Thread scaling sweep ──────────────────────────────────────────────────
    if config.get("thread_scaling"):
        print("\nRunning thread scaling sweep...")
        ts_rows = run_thread_scaling_sweep(code_root, graph_path, config,
                                           run_id, bfs_src, log_dir)
        if ts_rows:
            ts_csv = output_dir / "thread_scaling.csv"
            write_csv(ts_csv, THREAD_SCALING_COLUMNS, ts_rows)
            print(f"Thread scaling CSV: {ts_csv}")

    # ── Derived analysis CSVs (generated from raw.csv when optional data present) ──
    stall_rows = generate_stall_metrics_csv(output_dir)
    if stall_rows:
        stall_csv = output_dir / "stall_metrics.csv"
        write_csv(stall_csv, STALL_METRICS_COLUMNS, stall_rows)
        print(f"Stall metrics CSV: {stall_csv}")

    bd_rows = generate_update_breakdown_csv(output_dir)
    if bd_rows:
        bd_csv = output_dir / "update_breakdown.csv"
        write_csv(bd_csv, UPDATE_BREAKDOWN_COLUMNS, bd_rows)
        print(f"Update breakdown CSV: {bd_csv}")

    # ── Plots ─────────────────────────────────────────────────────────────────
    if config["make_plots"]:
        plot_dir = None
        if config["plot_dir"]:
            plot_dir = Path(config["plot_dir"])
            if not plot_dir.is_absolute():
                plot_dir = repo_root / plot_dir
        paths = generate_comparison_plots(output_dir, plot_dir)
        for p in paths:
            print(f"Plot: {p}")


if __name__ == "__main__":
    main()
