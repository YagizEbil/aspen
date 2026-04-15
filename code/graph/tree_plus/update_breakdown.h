#pragma once
// Update-phase timing breakdown for Aspen's insert_edges_batch / delete_edges_batch.
//
// Usage:
//   - In run_dynamic_cpu_benchmark.cpp: set g_update_breakdown to a non-null pointer before
//     calling insert/delete_edges_batch, then read the accumulated values afterwards.
//   - When g_update_breakdown is null (the default), the timing code compiles to a null-check
//     + branch that the compiler may eliminate, incurring negligible overhead.
//
// Phases timed inside insert_edges_batch / delete_edges_batch:
//   sort_ms        - sort_updates() (only when sorted=false; benchmark calls with sorted=true
//                    so this will be 0 in normal use)
//   preprocess_ms  - optional dedup pack + pack_index grouping
//   edge_struct_ms - parallel_for building per-vertex edge_struct objects
//   tree_merge_ms  - multi_insert_sorted_with_values (the dominant phase)
//
// other_ms is NOT stored here — it is computed in Python as:
//   other_ms = update_ms - (sort_ms + preprocess_ms + edge_struct_ms + tree_merge_ms)
// and represents version-install overhead + acquire/release_version latency.

#include <chrono>

struct UpdateBreakdownAccum {
  double sort_ms        = 0.0;
  double preprocess_ms  = 0.0;
  double edge_struct_ms = 0.0;
  double tree_merge_ms  = 0.0;
  int    count          = 0;    // number of insert/delete_edges_batch calls accumulated

  void reset() { *this = {}; }
};

// C++17 inline variable — safe to include from multiple translation units
// (both run_dynamic_cpu_benchmark and run_static_bfs_bench transitively include
// immutable_graph_tree_plus.h via api.h).
inline UpdateBreakdownAccum* g_update_breakdown = nullptr;

// Convenience: record one phase transition.  Called from immutable_graph_tree_plus.h.
inline std::chrono::steady_clock::time_point bd_now() {
  return g_update_breakdown ? std::chrono::steady_clock::now()
                            : std::chrono::steady_clock::time_point{};
}

inline double bd_ms(std::chrono::steady_clock::time_point a,
                    std::chrono::steady_clock::time_point b) {
  return std::chrono::duration<double, std::milli>(b - a).count();
}
