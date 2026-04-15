#include "../common/types.h"
#include "../pbbslib/utilities.h"
#include "../pbbslib/parse_command_line.h"

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "rmat_util.h"

namespace {

using logical_edge = std::pair<uint32_t, uint32_t>;

[[noreturn]] void fail(const std::string& message) {
  std::cerr << "rmat_to_adj: " << message << std::endl;
  std::exit(1);
}

uint64_t canonical_key(uint32_t u, uint32_t v) {
  uint32_t a = std::min(u, v);
  uint32_t b = std::max(u, v);
  return (static_cast<uint64_t>(a) << 32ULL) | static_cast<uint64_t>(b);
}

std::string default_output_name(size_t scale, size_t edge_factor, uint64_t seed) {
  return "inputs/rmat_scale" + std::to_string(scale) +
         "_ef" + std::to_string(edge_factor) +
         "_seed" + std::to_string(seed) + ".adj";
}

}  // namespace

int main(int argc, char** argv) {
  commandLine P(argc, argv,
                "./rmat_to_adj --scale <power_of_two_exponent> --edge-factor <logical_edges_per_vertex> "
                "[--seed <seed>] [--a <a>] [--b <b>] [--c <c>] [--output <file>]");

  const size_t scale = static_cast<size_t>(P.getOptionLongValue("--scale", 18));
  const size_t edge_factor = static_cast<size_t>(P.getOptionLongValue("--edge-factor", 8));
  const uint64_t seed = static_cast<uint64_t>(P.getOptionLongValue("--seed", 1));
  const double a = P.getOptionDoubleValue("--a", 0.5);
  const double b = P.getOptionDoubleValue("--b", 0.1);
  const double c = P.getOptionDoubleValue("--c", 0.1);

  if (scale == 0 || scale >= 32) {
    fail("--scale must be in the range [1, 31] for the current 32-bit Aspen vertex type.");
  }
  if (edge_factor == 0) {
    fail("--edge-factor must be positive.");
  }
  if (a <= 0.0 || b <= 0.0 || c <= 0.0 || (a + b + c) >= 1.0) {
    fail("RMAT parameters must satisfy a > 0, b > 0, c > 0, and a + b + c < 1.");
  }

  const uint32_t num_vertices = static_cast<uint32_t>(1ULL << scale);
  const size_t target_logical_edges = static_cast<size_t>(num_vertices) * edge_factor;
  const std::string output_path =
      P.getOptionValue("--output", default_output_name(scale, edge_factor, seed));

  std::cerr << "Generating RMAT graph with scale=" << scale
            << " vertices=" << num_vertices
            << " logical_edges=" << target_logical_edges
            << " seed=" << seed << std::endl;

  std::unordered_set<uint64_t> seen;
  seen.reserve(target_logical_edges * 2);

  std::vector<logical_edge> logical_edges;
  logical_edges.reserve(target_logical_edges);

  const uint32_t nn = num_vertices;
  rMat<uint32_t> generator(nn, static_cast<uint32_t>(seed), a, b, c);

  uint64_t attempts = 0;
  while (logical_edges.size() < target_logical_edges) {
    auto edge = generator(static_cast<uint32_t>(attempts++));
    uint32_t u = static_cast<uint32_t>(edge.first);
    uint32_t v = static_cast<uint32_t>(edge.second);
    if (u == v) {
      continue;
    }
    uint32_t a_v = std::min(u, v);
    uint32_t b_v = std::max(u, v);
    uint64_t key = canonical_key(a_v, b_v);
    if (seen.insert(key).second) {
      logical_edges.emplace_back(a_v, b_v);
    }
  }

  std::vector<logical_edge> directed_edges;
  directed_edges.reserve(logical_edges.size() * 2);
  for (const auto& edge : logical_edges) {
    directed_edges.emplace_back(edge.first, edge.second);
    directed_edges.emplace_back(edge.second, edge.first);
  }
  std::sort(directed_edges.begin(), directed_edges.end());

  std::vector<uint64_t> degree(num_vertices, 0);
  for (const auto& edge : directed_edges) {
    degree[edge.first]++;
  }

  std::vector<uint64_t> offsets(num_vertices, 0);
  uint64_t prefix = 0;
  for (uint32_t v = 0; v < num_vertices; v++) {
    offsets[v] = prefix;
    prefix += degree[v];
  }

  const uint64_t directed_edge_count = static_cast<uint64_t>(directed_edges.size());
  if (prefix != directed_edge_count) {
    fail("Internal prefix-sum mismatch while building offsets.");
  }

  std::ofstream out(output_path);
  if (!out.is_open()) {
    fail("Failed to open output path: " + output_path);
  }

  out << "AdjacencyGraph\n";
  out << num_vertices << "\n";
  out << directed_edge_count << "\n";
  for (uint32_t v = 0; v < num_vertices; v++) {
    out << offsets[v] << "\n";
  }
  for (const auto& edge : directed_edges) {
    out << edge.second << "\n";
  }
  out.close();

  if (!out) {
    fail("Failed while writing output path: " + output_path);
  }

  std::cerr << "Wrote adjacency graph to " << output_path
            << " with n=" << num_vertices
            << " and m=" << directed_edge_count << " directed edges." << std::endl;
  return 0;
}
