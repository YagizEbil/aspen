#include "../graph/api.h"
#include "../trees/utils.h"
#include "../lib_extensions/sparse_table_hash.h"
#include "../pbbslib/random_shuffle.h"

#include <cstring>
#include <papi.h>

#include <vector>
#include <algorithm>
#include <chrono>
#include <thread>
#include <cmath>
#include <limits>


#include <iostream>
#include <fstream>

#include "rmat_util.h"

using namespace std;
using edge_seq = pair<uintV, uintV>;

namespace {

constexpr long long kCounterUnavailable = std::numeric_limits<long long>::min();

string counter_to_csv(long long value) {
  return (value == kCounterUnavailable) ? "NA" : to_string(value);
}

class PapiProfiler {
 public:
  PapiProfiler() {
    int ret = PAPI_library_init(PAPI_VER_CURRENT);
    if (ret != PAPI_VER_CURRENT) {
      cerr << "[PAPI] Initialization failed";
      if (ret > 0) {
        cerr << ": version mismatch";
      } else {
        cerr << ": " << PAPI_strerror(ret);
      }
      cerr << ". Continuing with timing only." << endl;
      cout << "Phase,Time,Cycles,Instructions,L1_Misses,L2_Misses" << endl;
      return;
    }
    papi_initialized_ = true;

    ret = PAPI_create_eventset(&event_set_);
    if (ret != PAPI_OK) {
      cerr << "[PAPI] Failed to create event set: " << PAPI_strerror(ret)
           << ". Continuing with timing only." << endl;
      cout << "Phase,Time,Cycles,Instructions,L1_Misses,L2_Misses" << endl;
      return;
    }
    event_set_created_ = true;

    add_event(PAPI_TOT_CYC, "PAPI_TOT_CYC");
    add_event(PAPI_TOT_INS, "PAPI_TOT_INS");
    add_event(PAPI_L1_DCM, "PAPI_L1_DCM");
    add_event(PAPI_L2_DCM, "PAPI_L2_DCM");

    if (tracked_events_.empty()) {
      cerr << "[PAPI] No requested events available. Continuing with timing only."
           << endl;
    } else {
      counters_enabled_ = true;
    }

    cout << "Phase,Time,Cycles,Instructions,L1_Misses,L2_Misses" << endl;
  }

  ~PapiProfiler() {
    if (event_set_created_) {
      PAPI_cleanup_eventset(event_set_);
      PAPI_destroy_eventset(&event_set_);
    }
    if (papi_initialized_) {
      PAPI_shutdown();
    }
  }

  void start() {
    start_time_ = std::chrono::steady_clock::now();
    if (!counters_enabled_) {
      return;
    }

    int ret = PAPI_reset(event_set_);
    if (ret != PAPI_OK) {
      cerr << "[PAPI] Failed to reset counters: " << PAPI_strerror(ret)
           << ". Disabling counters for the rest of the run." << endl;
      counters_enabled_ = false;
      return;
    }

    ret = PAPI_start(event_set_);
    if (ret != PAPI_OK) {
      cerr << "[PAPI] Failed to start counters: " << PAPI_strerror(ret)
           << ". Disabling counters for the rest of the run." << endl;
      counters_enabled_ = false;
    }
  }

  void stop(const string& phase_name) {
    double elapsed =
        std::chrono::duration<double>(std::chrono::steady_clock::now() -
                                      start_time_).count();

    long long cycles = kCounterUnavailable;
    long long instructions = kCounterUnavailable;
    long long l1_misses = kCounterUnavailable;
    long long l2_misses = kCounterUnavailable;

    if (counters_enabled_) {
      std::vector<long long> values(tracked_events_.size(), 0);
      int ret = PAPI_stop(event_set_, values.data());
      if (ret != PAPI_OK) {
        cerr << "[PAPI] Failed to stop counters: " << PAPI_strerror(ret)
             << ". Disabling counters for the rest of the run." << endl;
        counters_enabled_ = false;
      } else {
        for (size_t i = 0; i < tracked_events_.size(); i++) {
          switch (tracked_events_[i]) {
            case PAPI_TOT_CYC:
              cycles = values[i];
              break;
            case PAPI_TOT_INS:
              instructions = values[i];
              break;
            case PAPI_L1_DCM:
              l1_misses = values[i];
              break;
            case PAPI_L2_DCM:
              l2_misses = values[i];
              break;
            default:
              break;
          }
        }
      }
    }

    cout << phase_name << "," << elapsed << "," << counter_to_csv(cycles) << ","
         << counter_to_csv(instructions) << "," << counter_to_csv(l1_misses)
         << "," << counter_to_csv(l2_misses) << endl;
  }

 private:
  void add_event(int event_code, const char* event_name) {
    int ret = PAPI_add_event(event_set_, event_code);
    if (ret == PAPI_OK) {
      tracked_events_.push_back(event_code);
      return;
    }

    cerr << "[PAPI] Event " << event_name
         << " unavailable on this platform: " << PAPI_strerror(ret)
         << ". Continuing without it." << endl;
  }

  bool papi_initialized_ = false;
  bool event_set_created_ = false;
  bool counters_enabled_ = false;
  int event_set_ = PAPI_NULL;
  std::vector<int> tracked_events_;
  std::chrono::steady_clock::time_point start_time_;
};

}  // namespace

void parallel_updates(commandLine& P) {
  string update_fname = P.getOptionValue("-update-file", "updates.dat");
  PapiProfiler profiler;

  auto VG = initialize_treeplus_graph(P);

  cout << "calling acquire_version" << endl;
  auto S = VG.acquire_version();
  cout << "acquired!" << endl;
  const auto& GA = S.graph;
  size_t n = GA.num_vertices();
  cout << "n = " << n << endl;
  VG.release_version(std::move(S));

  using pair_vertex = tuple<uintV, uintV>;

  auto r = pbbs::random();
  // 2. Generate the sequence of insertions and deletions

  auto update_sizes = pbbs::sequence<size_t>(10);
  update_sizes[0] = 10;
  update_sizes[1] = 100;
  update_sizes[2] = 1000;
  update_sizes[3] = 10000;
  update_sizes[4] = 100000;
  update_sizes[5] = 1000000;
  update_sizes[6] = 10000000;
  update_sizes[7] = 100000000;
  update_sizes[8] = 1000000000;
  update_sizes[9] = 2000000000;

  auto update_times = std::vector<double>();
  size_t n_trials = 3;
  size_t batch_index = 0;

  size_t start = 0;
  for (size_t us=start; us<update_sizes.size(); us++) {
    double avg_insert = 0.0;
    double avg_delete = 0.0;
    cout << "Running bs: " << update_sizes[us] << endl;

    if (update_sizes[us] < 100000000) {
      n_trials = 20;
    }
    else {
      n_trials = 3;
    }

    for (size_t ts=0; ts<n_trials; ts++) {
      size_t i = batch_index++;
      size_t updates_to_run = update_sizes[us];
      auto updates = pbbs::sequence<pair_vertex>(updates_to_run);

      double a = 0.5;
      double b = 0.1;
      double c = 0.1;
      size_t nn = 1 << (pbbs::log2_up(n) - 1);
      auto rmat = rMat<uintV>(nn, r.ith_rand(0), a, b, c);

      parallel_for(0, updates.size(), [&] (size_t i) {
        updates[i] = rmat(i);
      });

      {
        //cout << "Inserting" << endl;
        profiler.start();
        timer st; st.start();
        VG.insert_edges_batch(update_sizes[us], updates.begin(), false, true, nn, false);
        double batch_time = st.stop();
        profiler.stop("Insert_Batch_" + to_string(i));

        // cout << "batch time = " << batch_time << endl;
        avg_insert += batch_time;
      }

      {
        // cout << "Deleting" << endl;
        profiler.start();
        timer st; st.start();
        VG.delete_edges_batch(update_sizes[us], updates.begin(), false, true, nn, false);
        double batch_time = st.stop();
        profiler.stop("Delete_Batch_" + to_string(i));

        // cout << "batch time = " << batch_time << endl;
        avg_delete += batch_time;
      }

    }
    // cout << "Finished bs: " << update_sizes[us] << endl;
    cout << "Avg insert: " << (avg_insert / n_trials) << endl;
    cout << "Avg delete: " << (avg_delete / n_trials) << endl << endl;
  }
}

int main(int argc, char** argv) {
  cout << "Running with " << num_workers() << " threads" << endl;
  commandLine P(argc, argv, "./test_graph [-f file -m (mmap) <testid>]");

  parallel_updates(P);
}
