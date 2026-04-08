// Convert a SNAP edge list (e.g., soc-LiveJournal1.txt) into
// a PBBS/Ligra-style adjacency graph file (e.g., soc-LiveJournal1_sym.adj).
//
// Usage:
//   snap_to_adj <input_snap_edge_list> <output_adj_file>
//
// The input format is the standard SNAP text edge list:
//   - Lines starting with '#' are comments and ignored.
//   - Each non-comment line contains "u v" (whitespace-separated integers).
//
// The output format is the plaintext "AdjacencyGraph" format used by
// PBBS/Ligra/Aspen and described in this repo's README:
//
//   AdjacencyGraph
//   <n>
//   <m>
//   <o0>
//   <o1>
//   ...
//   <o(n-1)>
//   <e0>
//   <e1>
//   ...
//   <e(m-1)>
//
// where n is the number of vertices, m is the number of (directed) edges,
// oi is the start offset of vertex i's adjacency list in the edge array,
// and ej are the destination vertex IDs.
//
// This tool:
//   - Treats the input as a directed graph.
//   - Produces a symmetric graph by inserting both (u, v) and (v, u)
//     for each input edge with u != v.
//   - Keeps vertex IDs as-is, assuming they are non-negative integers
//     with maximum ID max_v; vertices are 0..max_v (isolated vertices
//     are allowed).

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <limits>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

int main(int argc, char** argv) {
  if (argc != 3) {
    std::cerr << "Usage: snap_to_adj <input_snap_edge_list> <output_adj_file>\n";
    return 1;
  }

  const std::string input_path = argv[1];
  const std::string output_path = argv[2];

  std::ifstream in(input_path);
  if (!in.is_open()) {
    std::cerr << "Failed to open input file: " << input_path << "\n";
    return 1;
  }

  std::vector<std::pair<uint32_t, uint32_t>> edges;
  edges.reserve(1'000'000);  // will grow as needed

  uint64_t max_v = 0;
  std::string line;
  uint64_t num_input_edges = 0;

  while (std::getline(in, line)) {
    if (line.empty() || line[0] == '#') {
      continue;
    }

    std::istringstream iss(line);
    uint64_t u64 = 0, v64 = 0;
    if (!(iss >> u64 >> v64)) {
      continue;  // skip malformed lines
    }

    // Skip self-loops to match common practice for undirected social graphs.
    if (u64 == v64) {
      continue;
    }

    if (u64 > static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) ||
        v64 > static_cast<uint64_t>(std::numeric_limits<uint32_t>::max())) {
      std::cerr << "Vertex ID exceeds 32-bit range: (" << u64 << ", " << v64
                << ")\n";
      return 1;
    }

    uint32_t u = static_cast<uint32_t>(u64);
    uint32_t v = static_cast<uint32_t>(v64);

    edges.emplace_back(u, v);
    edges.emplace_back(v, u);  // symmetrize

    if (u64 > max_v) max_v = u64;
    if (v64 > max_v) max_v = v64;
    ++num_input_edges;
  }

  in.close();

  if (edges.empty()) {
    std::cerr << "No edges were read from input file: " << input_path << "\n";
    return 1;
  }

  const uint64_t n = max_v + 1;           // allow isolated vertices
  // Number of directed edges after symmetrization (and later dedup if any).
  std::cerr << "Read " << num_input_edges << " input edges; "
            << edges.size() << " directed edges after symmetrization.\n";
  std::cerr << "Number of vertices (n) = " << n << "\n";

  // Sort by (source, dest) so that adjacency lists are contiguous and
  // duplicates can be removed.
  std::sort(edges.begin(), edges.end(),
            [](const auto& a, const auto& b) {
              if (a.first < b.first) return true;
              if (a.first > b.first) return false;
              return a.second < b.second;
            });

  // Deduplicate identical (u, v) pairs.
  std::vector<std::pair<uint32_t, uint32_t>> unique_edges;
  unique_edges.reserve(edges.size());
  for (size_t i = 0; i < edges.size(); i++) {
    if (i > 0 && edges[i] == edges[i - 1]) {
      continue;
    }
    unique_edges.push_back(edges[i]);
  }
  edges.swap(unique_edges);

  const uint64_t m = static_cast<uint64_t>(edges.size());
  std::cerr << "After deduplication, m = " << m << " directed edges.\n";

  // Compute out-degree of each vertex.
  std::vector<uint64_t> degree(n, 0);
  for (const auto& e : edges) {
    degree[static_cast<size_t>(e.first)]++;
  }

  // Build prefix-sum offsets array: offsets[v] is the index in the edge array
  // where vertex v's adjacency list starts.
  std::vector<uint64_t> offsets(n, 0);
  uint64_t prefix = 0;
  for (uint64_t v = 0; v < n; v++) {
    offsets[static_cast<size_t>(v)] = prefix;
    prefix += degree[static_cast<size_t>(v)];
  }

  if (prefix != m) {
    std::cerr << "Internal error: prefix-sum mismatch (prefix=" << prefix
              << ", m=" << m << ")\n";
    return 1;
  }

  std::ofstream out(output_path);
  if (!out.is_open()) {
    std::cerr << "Failed to open output file: " << output_path << "\n";
    return 1;
  }

  out << "AdjacencyGraph\n";
  out << n << "\n";
  out << m << "\n";

  for (uint64_t v = 0; v < n; v++) {
    out << offsets[static_cast<size_t>(v)] << "\n";
  }

  for (const auto& e : edges) {
    out << static_cast<uint64_t>(e.second) << "\n";
  }

  out.close();

  if (!out) {
    std::cerr << "Error while writing output file: " << output_path << "\n";
    return 1;
  }

  std::cerr << "Wrote adjacency graph to " << output_path << "\n";
  return 0;
}

