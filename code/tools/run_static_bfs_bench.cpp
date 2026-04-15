// run_static_bfs_bench.cpp
//
// Measures BFS latency on a fixed Aspen graph snapshot (no updates).
// Emits one CSV row per invocation using the same column layout as
// run_dynamic_cpu_benchmark so the comparison orchestrator can parse both
// outputs with a single reader.
//
// Static-specific field values:
//   operation_type = "static_baseline"
//   batch_size     = 0
//   batch_id       = 0
//   update_ms      = "" (empty)
//   pr_ms          = "" (empty)
//   seed           = 0  (not applicable)
//   perf_scope     = bfs_only  (counters span BFS only, not an update)

#include "../graph/api.h"
#include "../algorithms/BFS.h"

#include <chrono>
#include <cstdint>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "linux_perf_counters.h"
#include "rapl_energy.h"

namespace {

// ── Static-only column layout (shorter than run_dynamic_cpu_benchmark; parsed
//    separately by run_comparison_bench.py via _RAW_COLUMNS_FROM_STATIC_BINARY) ─
constexpr const char* kRawCsvHeader =
    "run_id,timestamp,dataset,num_vertices,num_edges_initial,batch_id,batch_size,operation_type,"
    "update_ms,bfs_ms,pr_ms,cycles,instructions,cache_refs,cache_misses,branch_misses,"
    "bfs_energy_j,seed,trial,threads,notes";

struct BfsSummary {
  size_t reachable    = 0;
  size_t source_degree = 0;
};

[[noreturn]] void fail(const std::string& message) {
  throw std::runtime_error(message);
}

std::string csv_escape(const std::string& value) {
  if (value.find_first_of(",\"\n") == std::string::npos) {
    return value;
  }
  std::string escaped = "\"";
  for (char ch : value) {
    if (ch == '"') {
      escaped += "\"\"";
    } else {
      escaped += ch;
    }
  }
  escaped += "\"";
  return escaped;
}

std::string format_double(double value) {
  std::ostringstream out;
  out << std::fixed << std::setprecision(6) << value;
  return out.str();
}

std::string utc_timestamp() {
  const std::time_t now = std::time(nullptr);
  std::tm tm_now;
  gmtime_r(&now, &tm_now);
  std::ostringstream out;
  out << std::put_time(&tm_now, "%Y-%m-%dT%H:%M:%SZ");
  return out.str();
}

std::string join_notes(const std::vector<std::string>& notes) {
  std::ostringstream out;
  for (size_t i = 0; i < notes.size(); i++) {
    if (i > 0) out << ';';
    out << notes[i];
  }
  return out.str();
}

class CoutRedirect {
 public:
  explicit CoutRedirect(std::streambuf* target)
      : original_(std::cout.rdbuf(target)), restored_(false) {}
  ~CoutRedirect() { restore(); }
  void restore() {
    if (!restored_) {
      std::cout.rdbuf(original_);
      restored_ = true;
    }
  }
 private:
  std::streambuf* original_;
  bool restored_;
};

// ── BFS on a flat snapshot (same implementation as run_dynamic_cpu_benchmark) ─
template <class Graph>
BfsSummary run_bfs_fetch_count(Graph& graph, uintV src) {
  const size_t n = graph.num_vertices();
  if (src >= n) {
    fail("BFS source " + std::to_string(src) + " is outside the graph vertex range.");
  }

  auto fetched_vertices = graph.fetch_all_vertices();
  const size_t source_degree = fetched_vertices[src].degree();

  auto frontier = vertex_subset(n, src);
  auto parents  = pbbs::sequence<uintV>(n, [](size_t) {
    return static_cast<uintV>(UINT_V_MAX);
  });
  parents[src] = src;

  timer sparse_t("sparse", false);
  timer dense_t("dense", false);
  timer other_t("other", false);
  size_t reachable = 0;
  while (!frontier.is_empty()) {
    reachable += frontier.size();
    auto output = graph.edge_map(frontier, BFS_F(parents.begin()), fetched_vertices,
                                  sparse_t, dense_t, other_t, stay_dense);
    frontier.del();
    frontier = output;
  }
  frontier.del();

  return {reachable, source_degree};
}

}  // namespace

int main(int argc, char** argv) {
  commandLine P(argc, argv,
                "./run_static_bfs_bench -f <graph> -s --bfs-src <vertex_id> "
                "[--run-id <id>] [--dataset-name <name>] [--trial <n>] "
                "[--threads <n>] [--collect-perf] [--csv-header]");

  CoutRedirect redirect(std::cerr.rdbuf());

  try {
    // ── Required arguments ────────────────────────────────────────────────
    const std::string input_path = P.getOptionValue("-f", "");
    if (input_path.empty()) {
      fail("Missing required Aspen input graph. Pass -f <graph_path>.");
    }
    if (!P.getOption("-s")) {
      fail("The benchmark expects a symmetric graph; pass -s.");
    }

    const long bfs_src_long = P.getOptionLongValue("--bfs-src", -1);
    if (bfs_src_long < 0) {
      fail("--bfs-src <vertex_id> is required and must be non-negative.");
    }
    const uintV bfs_src = static_cast<uintV>(bfs_src_long);

    // ── Optional arguments ────────────────────────────────────────────────
    const std::string run_id      = P.getOptionValue("--run-id",       "single_run");
    const std::string dataset_name = P.getOptionValue("--dataset-name", input_path);
    const size_t trial            = static_cast<size_t>(P.getOptionLongValue("--trial",   0));
    const size_t req_threads      = static_cast<size_t>(P.getOptionLongValue("--threads", num_workers()));
    const size_t actual_threads   = static_cast<size_t>(num_workers());
    // Internal warmup: BFS runs within the same process before the timed measurement.
    // This ensures the cache state matches the dynamic benchmark, where the update
    // phase inherently warms the cache before BFS runs. Without this, static BFS
    // measures cold-cache latency while dynamic BFS measures warm-cache latency,
    // creating a spurious appearance that static BFS is slower.
    const size_t warmup_rounds    = static_cast<size_t>(P.getOptionLongValue("--warmup-rounds", 3));
    const bool collect_perf       = P.getOption("--collect-perf");
    const bool print_header       = P.getOption("--csv-header");

    if (req_threads != actual_threads) {
      std::ostringstream out;
      out << "--threads was set to " << req_threads
          << " but Aspen is running with " << actual_threads
          << " worker threads. Set NUM_THREADS=" << req_threads
          << " in the environment before launching the benchmark.";
      fail(out.str());
    }

    // ── Load graph ────────────────────────────────────────────────────────
    auto graph_version = initialize_treeplus_graph(P);

    size_t num_vertices = 0;
    size_t num_edges    = 0;
    {
      auto snap      = graph_version.acquire_version();
      num_vertices   = snap.graph.num_vertices();
      num_edges      = snap.graph.num_edges();
      graph_version.release_version(std::move(snap));
    }

    if (bfs_src >= num_vertices) {
      fail("--bfs-src " + std::to_string(bfs_src) +
           " is outside the vertex range [0, " + std::to_string(num_vertices - 1) + "].");
    }

    // ── Internal warmup rounds (same process — warms the cache) ──────────
    // These are discarded; they bring the BFS working set into L3 before the
    // timed measurement, matching the warm-cache state the dynamic benchmark
    // naturally achieves through its update phase.
    for (size_t w = 0; w < warmup_rounds; w++) {
      auto warmup_snap = graph_version.acquire_version();
      run_bfs_fetch_count(warmup_snap.graph, bfs_src);  // result discarded
      graph_version.release_version(std::move(warmup_snap));
    }

    // ── Open perf counters (BFS-only scope) ───────────────────────────────
    aspen_bench::PerfCounterCollector perf_collector;
    if (collect_perf) {
      std::string perf_error;
      if (!perf_collector.open(/*branch_misses=*/true, /*stall_counters=*/false, &perf_error)) {
        fail("Unable to open Linux perf counters. " + perf_error);
      }
      if (!perf_collector.reset_and_enable(&perf_error)) {
        fail("Unable to start Linux perf counters. " + perf_error);
      }
    }

    // RAPL energy reader — best-effort, silently disabled if perf_event_paranoid > 0.
    aspen_bench::RaplEnergyReader rapl;
    rapl.open();

    // ── Acquire snapshot and run timed BFS ───────────────────────────────
    auto snap      = graph_version.acquire_version();
    const uint64_t bfs_energy_before = rapl.snapshot();
    auto bfs_start = std::chrono::steady_clock::now();
    BfsSummary bfs_result = run_bfs_fetch_count(snap.graph, bfs_src);
    auto bfs_end   = std::chrono::steady_clock::now();
    const uint64_t bfs_energy_after = rapl.snapshot();
    double bfs_ms      = std::chrono::duration<double, std::milli>(bfs_end - bfs_start).count();
    double bfs_energy_j = rapl.delta_joules(bfs_energy_before, bfs_energy_after);
    graph_version.release_version(std::move(snap));

    // ── Read perf counters ────────────────────────────────────────────────
    aspen_bench::PerfCounterValues perf_vals;
    bool perf_ok = false;
    if (collect_perf) {
      std::string perf_error;
      perf_collector.disable(&perf_error);
      perf_ok = perf_collector.read(&perf_vals, &perf_error);
    }

    // ── Sanity check ──────────────────────────────────────────────────────
    if (bfs_result.reachable < 1 || bfs_result.reachable > num_vertices) {
      std::ostringstream out;
      out << "BFS reachable-count sanity check failed: reachable="
          << bfs_result.reachable << " num_vertices=" << num_vertices;
      fail(out.str());
    }

    // ── Build notes ───────────────────────────────────────────────────────
    std::vector<std::string> notes;
    notes.push_back("perf_scope=bfs_only");
    notes.push_back("bfs_src=" + std::to_string(bfs_src));
    notes.push_back("bfs_reachable=" + std::to_string(bfs_result.reachable));
    notes.push_back("warmup_rounds=" + std::to_string(warmup_rounds));
    if (!collect_perf) {
      notes.push_back("perf_disabled");
    } else if (!perf_vals.branch_misses_available) {
      notes.push_back("branch_misses_unsupported");
    }
    notes.push_back(rapl.available() ? "rapl=ok" : "rapl=unavailable");

    // ── Build CSV row (identical column layout to run_dynamic_cpu_benchmark) ─
    std::vector<std::string> fields = {
        run_id,
        utc_timestamp(),
        dataset_name,
        std::to_string(num_vertices),
        std::to_string(num_edges),          // num_edges_initial
        "0",                                 // batch_id
        "0",                                 // batch_size  (0 = no update)
        "static_baseline",                   // operation_type
        "",                                  // update_ms   (empty — no update phase)
        format_double(bfs_ms),
        "",                                  // pr_ms       (empty)
        perf_ok ? std::to_string(perf_vals.cycles)            : "",
        perf_ok ? std::to_string(perf_vals.instructions)      : "",
        perf_ok ? std::to_string(perf_vals.cache_references)  : "",
        perf_ok ? std::to_string(perf_vals.cache_misses)      : "",
        (perf_ok && perf_vals.branch_misses_available)
            ? std::to_string(perf_vals.branch_misses) : "",
        rapl.available() ? format_double(bfs_energy_j) : "",   // bfs_energy_j
        "0",                                 // seed (N/A)
        std::to_string(trial),
        std::to_string(actual_threads),
        join_notes(notes),
    };

    // ── Emit ──────────────────────────────────────────────────────────────
    redirect.restore();
    if (print_header) {
      std::cout << kRawCsvHeader << '\n';
    }
    std::ostringstream row;
    for (size_t i = 0; i < fields.size(); i++) {
      if (i > 0) row << ',';
      row << csv_escape(fields[i]);
    }
    std::cout << row.str() << '\n';
    return 0;

  } catch (const std::exception& ex) {
    redirect.restore();
    std::cerr << "run_static_bfs_bench: " << ex.what() << std::endl;
    return 1;
  }
}
