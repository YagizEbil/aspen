#include "../graph/api.h"
#include "../algorithms/BFS.h"
#include "../lib_extensions/sparse_table_hash.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "linux_perf_counters.h"
#include "rapl_energy.h"
#include "../graph/tree_plus/update_breakdown.h"

namespace {

using logical_edge = std::pair<uintV, uintV>;
using directed_edge = std::tuple<uintV, uintV>;

// cache_references = LLC accesses (AMD: cache-references = L3 lookup count)
// cache_misses     = LLC misses   (AMD: cache-misses     = L3 misses to DRAM)
// update_energy_j / bfs_energy_j: socket energy (RAPL) scoped to each phase;
//   empty when perf_event_paranoid > 0 (run `sudo sysctl -w kernel.perf_event_paranoid=0`)
// update_sort_ms .. update_tree_merge_ms: breakdown of update-phase internals;
//   empty unless --breakdown is passed.  sort_ms is always 0 in normal benchmarks
//   because the binary pre-sorts edges and calls insert_edges_batch(sorted=true).
// update_stall_frontend/backend, bfs_stall_frontend/backend: stall-cycle PMU counters;
//   empty unless --collect-stall is passed and the events are supported on this PMU.
//   When stall counters are collected, branch_misses are NOT collected (same PMC slots).
constexpr const char* kRawCsvHeader =
    "run_id,timestamp,dataset,num_vertices,num_edges_initial,batch_id,batch_size,operation_type,"
    "update_ms,bfs_ms,pr_ms,"
    "update_cycles,update_instructions,update_cache_refs,update_cache_misses,update_branch_misses,"
    "bfs_cycles,bfs_instructions,bfs_cache_refs,bfs_cache_misses,bfs_branch_misses,"
    "update_energy_j,bfs_energy_j,"
    "update_sort_ms,update_preprocess_ms,update_edge_struct_ms,update_tree_merge_ms,"
    "update_stall_frontend,update_stall_backend,bfs_stall_frontend,bfs_stall_backend,"
    "seed,trial,threads,notes";

struct PreparedBenchmark {
  size_t num_vertices = 0;
  size_t num_edges_initial = 0;
  std::vector<logical_edge> logical_updates;
  std::vector<directed_edge> directed_updates;
  uintV bfs_source = 0;
  bool bfs_source_auto = true;
};

struct BfsSummary {
  size_t reachable = 0;
  size_t source_degree = 0;
};

struct MeasurementResult {
  double update_ms = 0.0;
  double bfs_ms = 0.0;
  BfsSummary bfs;
  aspen_bench::PerfCounterValues update_perf;
  aspen_bench::PerfCounterValues bfs_perf;
  bool perf_collected = false;
  double update_energy_j = 0.0;
  double bfs_energy_j    = 0.0;
  bool energy_collected  = false;
  UpdateBreakdownAccum breakdown;
  bool breakdown_collected = false;
};

[[noreturn]] void fail(const std::string& message) {
  throw std::runtime_error(message);
}

uint64_t edge_key(uintV u, uintV v) {
  const uint64_t left = pbbs::hash64(static_cast<uint64_t>(u));
  const uint64_t right = pbbs::hash64(static_cast<uint64_t>(v) + 0x9e3779b97f4a7c15ULL);
  return left ^ (right + 0x9e3779b97f4a7c15ULL + (left << 6ULL) + (left >> 2ULL));
}

uint64_t canonical_edge_key(const logical_edge& edge) {
  return edge_key(edge.first, edge.second);
}

uint64_t seeded_edge_hash(const logical_edge& edge, uint64_t seed) {
  return pbbs::hash64(canonical_edge_key(edge) ^ seed);
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
    if (i > 0) {
      out << ';';
    }
    out << notes[i];
  }
  return out.str();
}

class CoutRedirect {
 public:
  explicit CoutRedirect(std::streambuf* target)
      : original_(std::cout.rdbuf(target)), restored_(false) {}

  ~CoutRedirect() {
    restore();
  }

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

template <class Graph>
BfsSummary run_bfs_fetch_count(Graph& graph, uintV src) {
  const size_t n = graph.num_vertices();
  if (src >= n) {
    fail("BFS source " + std::to_string(src) + " is outside the graph vertex range.");
  }

  auto fetched_vertices = graph.fetch_all_vertices();
  const size_t source_degree = fetched_vertices[src].degree();

  auto frontier = vertex_subset(n, src);
  auto parents = pbbs::sequence<uintV>(n, [&] (size_t) {
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

template <class VG>
std::vector<logical_edge> sample_existing_edges(VG& graph_version, size_t sample_size, uint64_t seed) {
  auto snapshot = graph_version.acquire_version();
  const auto& graph = snapshot.graph;
  const size_t directed_edges = graph.num_edges();
  if (directed_edges == 0) {
    graph_version.release_version(std::move(snapshot));
    fail("The loaded graph has zero edges; update sampling requires at least one existing edge.");
  }

  const size_t logical_edge_upper_bound = directed_edges / 2;
  if (logical_edge_upper_bound == 0) {
    graph_version.release_version(std::move(snapshot));
    fail("The loaded graph does not appear to contain any logical undirected edges.");
  }
  const size_t requested = std::min(sample_size, logical_edge_upper_bound);
  const double beta = std::min(1.0, static_cast<double>(requested) /
                                        static_cast<double>(std::max<size_t>(logical_edge_upper_bound, 1)));

  auto empty = std::make_tuple(std::make_pair(UINT_V_MAX, UINT_V_MAX), pbbs::empty());
  auto hash_fn = [&] (const logical_edge& edge) {
    return pbbs::hash64(canonical_edge_key(edge));
  };
  const size_t table_capacity = std::max<size_t>(1024, requested == 0 ? 1024 : requested * 2);
  auto table = make_sparse_table_hash<logical_edge, pbbs::empty>(table_capacity, empty, hash_fn);

  const size_t up_m = static_cast<size_t>(1) << pbbs::log2_up(std::max<size_t>(logical_edge_upper_bound, 1));
  const size_t mask = up_m - 1;
  const size_t threshold = static_cast<size_t>(std::ceil(beta * static_cast<double>(up_m)));

  auto map_f = [&] (const uintV& src, const uintV& dst) {
    const logical_edge edge(std::min(src, dst), std::max(src, dst));
    const size_t hashed = pbbs::hash64(canonical_edge_key(edge) ^ seed) & mask;
    if (hashed < threshold) {
      table.insert(std::make_tuple(edge, pbbs::empty()));
    }
  };
  graph.map_all_edges(map_f);
  graph_version.release_version(std::move(snapshot));

  auto entries = table.entries();
  std::vector<logical_edge> sampled;
  sampled.reserve(entries.size());
  for (size_t i = 0; i < entries.size(); i++) {
    sampled.push_back(std::get<0>(entries[i]));
  }
  table.del();

  std::sort(sampled.begin(), sampled.end(), [&] (const logical_edge& left, const logical_edge& right) {
    const uint64_t left_hash = seeded_edge_hash(left, seed);
    const uint64_t right_hash = seeded_edge_hash(right, seed);
    if (left_hash != right_hash) {
      return left_hash < right_hash;
    }
    return left < right;
  });
  return sampled;
}

std::vector<directed_edge> expand_to_directed_sorted(const std::vector<logical_edge>& logical_edges) {
  std::vector<directed_edge> updates;
  updates.reserve(logical_edges.size() * 2);
  for (const auto& edge : logical_edges) {
    updates.emplace_back(edge.first, edge.second);
    updates.emplace_back(edge.second, edge.first);
  }
  std::sort(updates.begin(), updates.end(), [] (const directed_edge& left, const directed_edge& right) {
    if (std::get<0>(left) != std::get<0>(right)) {
      return std::get<0>(left) < std::get<0>(right);
    }
    return std::get<1>(left) < std::get<1>(right);
  });
  return updates;
}

std::vector<logical_edge> choose_batch_updates(const std::vector<logical_edge>& sampled_edges, size_t batch_size) {
  if (sampled_edges.size() < batch_size) {
    std::ostringstream out;
    out << "Insufficient sampled edges for batch_size=" << batch_size
        << ". Sampled " << sampled_edges.size() << " unique logical edges.";
    fail(out.str());
  }
  return std::vector<logical_edge>(sampled_edges.begin(), sampled_edges.begin() + batch_size);
}

PreparedBenchmark prepare_benchmark(versioned_graph<treeplus_graph>& graph_version,
                                    const std::string& operation_type,
                                    size_t batch_size,
                                    size_t sample_size,
                                    uint64_t seed,
                                    const std::string& bfs_source_option) {
  if (batch_size == 0) {
    fail("--batch-size must be positive.");
  }

  auto sampled_edges = sample_existing_edges(graph_version, sample_size, seed);
  auto logical_updates = choose_batch_updates(sampled_edges, batch_size);
  auto directed_updates = expand_to_directed_sorted(logical_updates);

  PreparedBenchmark prepared;
  prepared.logical_updates = std::move(logical_updates);
  prepared.directed_updates = std::move(directed_updates);

  size_t original_num_edges = 0;
  {
    auto snapshot = graph_version.acquire_version();
    prepared.num_vertices = snapshot.graph.num_vertices();
    original_num_edges = snapshot.graph.num_edges();
    graph_version.release_version(std::move(snapshot));
  }

  if (operation_type == "insert") {
    graph_version.delete_edges_batch(prepared.directed_updates.size(), prepared.directed_updates.data(),
                                     true, false, prepared.num_vertices, false);
  } else if (operation_type != "delete") {
    fail("Unsupported --operation value: " + operation_type + ". Expected insert or delete.");
  }

  {
    auto snapshot = graph_version.acquire_version();
    prepared.num_edges_initial = snapshot.graph.num_edges();
    graph_version.release_version(std::move(snapshot));
  }

  if (operation_type == "insert") {
    if (prepared.num_edges_initial + prepared.directed_updates.size() != original_num_edges) {
      fail("Insert preconditioning edge-count check failed.");
    }
  } else if (prepared.num_edges_initial != original_num_edges) {
    fail("Delete baseline edge-count check failed before the measured update.");
  }

  if (bfs_source_option == "auto") {
    prepared.bfs_source = prepared.logical_updates.front().first;
    prepared.bfs_source_auto = true;
  } else {
    const long src_long = std::stol(bfs_source_option);
    if (src_long < 0) {
      fail("--bfs-src must be a non-negative integer or auto.");
    }
    prepared.bfs_source = static_cast<uintV>(src_long);
    prepared.bfs_source_auto = false;
  }

  if (prepared.bfs_source >= prepared.num_vertices) {
    fail("BFS source " + std::to_string(prepared.bfs_source) +
         " is outside the vertex range [0, " + std::to_string(prepared.num_vertices - 1) + "].");
  }

  return prepared;
}

MeasurementResult measure_benchmark(versioned_graph<treeplus_graph>& graph_version,
                                    const PreparedBenchmark& prepared,
                                    const std::string& operation_type,
                                    bool collect_perf,
                                    bool collect_stall,
                                    bool collect_breakdown) {
  auto* update_data = const_cast<directed_edge*>(prepared.directed_updates.data());
  aspen_bench::PerfCounterCollector perf_collector;
  if (collect_perf) {
    std::string perf_error;
    if (!perf_collector.open(/*branch_misses=*/!collect_stall, collect_stall, &perf_error)) {
      fail("Unable to open Linux perf counters. " + perf_error);
    }
  }

  // RAPL energy reader — best-effort, silently disabled if perf_event_paranoid > 0.
  aspen_bench::RaplEnergyReader rapl;
  rapl.open();

  MeasurementResult result;

  // ── Update phase ─────────────────────────────────────────────────────────
  if (collect_perf) {
    std::string perf_error;
    if (!perf_collector.reset_and_enable(&perf_error)) {
      fail("Unable to start Linux perf counters (update phase). " + perf_error);
    }
  }
  if (collect_breakdown) {
    result.breakdown.reset();
    g_update_breakdown = &result.breakdown;
  }
  const uint64_t update_energy_before = rapl.snapshot();
  auto update_start = std::chrono::steady_clock::now();
  if (operation_type == "insert") {
    graph_version.insert_edges_batch(prepared.directed_updates.size(), update_data,
                                     true, false, prepared.num_vertices, false);
  } else {
    graph_version.delete_edges_batch(prepared.directed_updates.size(), update_data,
                                     true, false, prepared.num_vertices, false);
  }
  auto update_end = std::chrono::steady_clock::now();
  const uint64_t update_energy_after = rapl.snapshot();
  if (collect_breakdown) {
    g_update_breakdown = nullptr;
    result.breakdown_collected = true;
  }
  result.update_ms       = std::chrono::duration<double, std::milli>(update_end - update_start).count();
  result.update_energy_j = rapl.delta_joules(update_energy_before, update_energy_after);
  if (collect_perf) {
    std::string perf_error;
    if (!perf_collector.disable(&perf_error)) {
      fail("Unable to stop Linux perf counters (update phase). " + perf_error);
    }
    if (!perf_collector.read(&result.update_perf, &perf_error)) {
      fail("Unable to read Linux perf counters (update phase). " + perf_error);
    }
  }

  // ── BFS phase ─────────────────────────────────────────────────────────────
  if (collect_perf) {
    std::string perf_error;
    if (!perf_collector.reset_and_enable(&perf_error)) {
      fail("Unable to start Linux perf counters (BFS phase). " + perf_error);
    }
  }
  auto snap = graph_version.acquire_version();
  const uint64_t bfs_energy_before = rapl.snapshot();
  auto bfs_start = std::chrono::steady_clock::now();
  result.bfs = run_bfs_fetch_count(snap.graph, prepared.bfs_source);
  auto bfs_end = std::chrono::steady_clock::now();
  const uint64_t bfs_energy_after = rapl.snapshot();
  result.bfs_ms       = std::chrono::duration<double, std::milli>(bfs_end - bfs_start).count();
  result.bfs_energy_j = rapl.delta_joules(bfs_energy_before, bfs_energy_after);
  graph_version.release_version(std::move(snap));
  if (collect_perf) {
    std::string perf_error;
    if (!perf_collector.disable(&perf_error)) {
      fail("Unable to stop Linux perf counters (BFS phase). " + perf_error);
    }
    if (!perf_collector.read(&result.bfs_perf, &perf_error)) {
      fail("Unable to read Linux perf counters (BFS phase). " + perf_error);
    }
    result.perf_collected = true;
  }
  result.energy_collected = rapl.available();

  return result;
}

void validate_result(versioned_graph<treeplus_graph>& graph_version,
                     const PreparedBenchmark& prepared,
                     const MeasurementResult& result,
                     const std::string& operation_type) {
  auto snapshot = graph_version.acquire_version();
  const size_t num_edges_after = snapshot.graph.num_edges();
  graph_version.release_version(std::move(snapshot));

  const size_t expected_delta = prepared.directed_updates.size();
  if (operation_type == "insert") {
    if (num_edges_after != prepared.num_edges_initial + expected_delta) {
      std::ostringstream out;
      out << "Insert edge-count check failed: expected " << (prepared.num_edges_initial + expected_delta)
          << " directed edges after update but observed " << num_edges_after << '.';
      fail(out.str());
    }
  } else {
    if (prepared.num_edges_initial < expected_delta) {
      fail("Delete baseline has fewer edges than the requested update batch.");
    }
    if (num_edges_after != prepared.num_edges_initial - expected_delta) {
      std::ostringstream out;
      out << "Delete edge-count check failed: expected " << (prepared.num_edges_initial - expected_delta)
          << " directed edges after update but observed " << num_edges_after << '.';
      fail(out.str());
    }
  }

  if (result.bfs.reachable < 1 || result.bfs.reachable > prepared.num_vertices) {
    std::ostringstream out;
    out << "BFS reachable-count sanity check failed: reachable=" << result.bfs.reachable
        << " num_vertices=" << prepared.num_vertices << '.';
    fail(out.str());
  }
  if (prepared.bfs_source_auto && result.bfs.source_degree > 0 && result.bfs.reachable <= 1) {
    fail("Auto-selected BFS source has nonzero degree after the update, but reachable <= 1.");
  }
}

std::string build_csv_row(const std::string& run_id,
                          const std::string& dataset_name,
                          size_t num_vertices,
                          size_t num_edges_initial,
                          size_t batch_size,
                          const std::string& operation_type,
                          uint64_t seed,
                          size_t trial,
                          size_t threads,
                          const MeasurementResult& result,
                          const std::vector<std::string>& notes) {
  std::vector<std::string> fields = {
      run_id,
      utc_timestamp(),
      dataset_name,
      std::to_string(num_vertices),
      std::to_string(num_edges_initial),
      "0",
      std::to_string(batch_size),
      operation_type,
      format_double(result.update_ms),
      format_double(result.bfs_ms),
      "",
      // update-phase perf counters
      result.perf_collected ? std::to_string(result.update_perf.cycles) : "",
      result.perf_collected ? std::to_string(result.update_perf.instructions) : "",
      result.perf_collected ? std::to_string(result.update_perf.cache_references) : "",
      result.perf_collected ? std::to_string(result.update_perf.cache_misses) : "",
      (result.perf_collected && result.update_perf.branch_misses_available)
          ? std::to_string(result.update_perf.branch_misses) : "",
      // BFS-phase perf counters
      result.perf_collected ? std::to_string(result.bfs_perf.cycles) : "",
      result.perf_collected ? std::to_string(result.bfs_perf.instructions) : "",
      result.perf_collected ? std::to_string(result.bfs_perf.cache_references) : "",
      result.perf_collected ? std::to_string(result.bfs_perf.cache_misses) : "",
      (result.perf_collected && result.bfs_perf.branch_misses_available)
          ? std::to_string(result.bfs_perf.branch_misses) : "",
      // energy columns (RAPL) — empty when perf_event_paranoid > 0
      result.energy_collected ? format_double(result.update_energy_j) : "",
      result.energy_collected ? format_double(result.bfs_energy_j)    : "",
      // update-phase breakdown — empty unless --breakdown is passed
      result.breakdown_collected ? format_double(result.breakdown.sort_ms)        : "",
      result.breakdown_collected ? format_double(result.breakdown.preprocess_ms)  : "",
      result.breakdown_collected ? format_double(result.breakdown.edge_struct_ms) : "",
      result.breakdown_collected ? format_double(result.breakdown.tree_merge_ms)  : "",
      // stall cycle counters — empty unless --collect-stall is passed and PMU supports them
      (result.perf_collected && result.update_perf.stall_counters_available)
          ? std::to_string(result.update_perf.stalled_cycles_frontend) : "",
      (result.perf_collected && result.update_perf.stall_counters_available)
          ? std::to_string(result.update_perf.stalled_cycles_backend) : "",
      (result.perf_collected && result.bfs_perf.stall_counters_available)
          ? std::to_string(result.bfs_perf.stalled_cycles_frontend) : "",
      (result.perf_collected && result.bfs_perf.stall_counters_available)
          ? std::to_string(result.bfs_perf.stalled_cycles_backend) : "",
      std::to_string(seed),
      std::to_string(trial),
      std::to_string(threads),
      join_notes(notes),
  };

  std::ostringstream out;
  for (size_t i = 0; i < fields.size(); i++) {
    if (i > 0) {
      out << ',';
    }
    out << csv_escape(fields[i]);
  }
  return out.str();
}

}  // namespace

int main(int argc, char** argv) {
  commandLine P(argc, argv,
                "./run_dynamic_cpu_benchmark -f <graph> -s [--dataset-name <name>] "
                "--operation <insert|delete> --batch-size <logical_edges> [--seed <seed>] "
                "[--trial <trial>] [--threads <threads>] [--bfs-src <id|auto>] "
                "[--sample-size <logical_edges>] [--collect-perf] [--collect-stall] "
                "[--breakdown] [--csv-header] [--run-id <id>]");

  CoutRedirect redirect(std::cerr.rdbuf());

  try {
    const std::string input_path = P.getOptionValue("-f", "");
    if (input_path.empty()) {
      fail("Missing required Aspen input graph. Pass -f <graph_path>.");
    }
    if (!P.getOption("-s")) {
      fail("The benchmark currently expects symmetric Aspen graphs; pass -s.");
    }

    const std::string run_id = P.getOptionValue("--run-id", "single_run");
    const std::string dataset_name = P.getOptionValue("--dataset-name", input_path);
    const std::string operation_type = P.getOptionValue("--operation", "");
    const size_t batch_size = static_cast<size_t>(P.getOptionLongValue("--batch-size", 0));
    const uint64_t seed = static_cast<uint64_t>(P.getOptionLongValue("--seed", 1));
    const size_t trial = static_cast<size_t>(P.getOptionLongValue("--trial", 0));
    const size_t requested_threads = static_cast<size_t>(P.getOptionLongValue("--threads", num_workers()));
    const std::string bfs_source_option = P.getOptionValue("--bfs-src", "auto");
    const size_t sample_size = static_cast<size_t>(
        P.getOptionLongValue("--sample-size", std::max<size_t>(batch_size * 8, 10000)));
    const bool collect_perf       = P.getOption("--collect-perf");
    const bool collect_stall      = P.getOption("--collect-stall");
    const bool collect_breakdown  = P.getOption("--breakdown");
    const bool print_csv_header   = P.getOption("--csv-header");
    const size_t actual_threads   = static_cast<size_t>(num_workers());

    if (requested_threads != actual_threads) {
      std::ostringstream out;
      out << "--threads was set to " << requested_threads
          << " but Aspen is running with " << actual_threads
          << " worker threads. Set NUM_THREADS=" << requested_threads
          << " in the environment before launching the benchmark.";
      fail(out.str());
    }

    auto graph_version = initialize_treeplus_graph(P);
    PreparedBenchmark prepared = prepare_benchmark(graph_version, operation_type, batch_size,
                                                   sample_size, seed, bfs_source_option);
    MeasurementResult result = measure_benchmark(graph_version, prepared, operation_type,
                                                  collect_perf, collect_stall, collect_breakdown);
    validate_result(graph_version, prepared, result, operation_type);

    std::vector<std::string> notes = {"pr_deferred", "perf_scope=update_separate;bfs_separate"};
    if (prepared.bfs_source_auto) {
      notes.push_back("bfs_src=auto:" + std::to_string(prepared.bfs_source));
    } else {
      notes.push_back("bfs_src=manual:" + std::to_string(prepared.bfs_source));
    }
    if (!collect_perf) {
      notes.push_back("perf_disabled");
    } else if (collect_stall) {
      notes.push_back(result.update_perf.stall_counters_available
                      ? "stall=ok" : "stall=unavailable");
    } else if (!result.update_perf.branch_misses_available) {
      notes.push_back("branch_misses_unsupported");
    }
    if (collect_breakdown) {
      notes.push_back("breakdown=ok");
    }
    notes.push_back(result.energy_collected ? "rapl=ok" : "rapl=unavailable");
    if (operation_type == "insert") {
      notes.push_back("baseline_precondition=selected_edges_deleted");
    }

    const std::string row = build_csv_row(run_id, dataset_name, prepared.num_vertices,
                                          prepared.num_edges_initial, batch_size, operation_type,
                                          seed, trial, actual_threads, result, notes);

    redirect.restore();
    if (print_csv_header) {
      std::cout << kRawCsvHeader << '\n';
    }
    std::cout << row << '\n';
    return 0;
  } catch (const std::exception& ex) {
    redirect.restore();
    std::cerr << "run_dynamic_cpu_benchmark: " << ex.what() << std::endl;
    return 1;
  }
}
