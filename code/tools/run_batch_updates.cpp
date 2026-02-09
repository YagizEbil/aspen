#include "../graph/api.h"
#include "../trees/utils.h"
#include "../lib_extensions/sparse_table_hash.h"
#include "../pbbslib/random_shuffle.h"

#include <cstring>
#include <cstdint>
#include <cerrno>
#if __has_include(<papi.h>)
#include <papi.h>
#define ASPEN_HAVE_PAPI 1
#elif __has_include(<papi/papi.h>)
#include <papi/papi.h>
#define ASPEN_HAVE_PAPI 1
#else
#define ASPEN_HAVE_PAPI 0
#endif

#include <vector>
#include <algorithm>
#include <chrono>
#include <thread>
#include <cmath>
#include <limits>
#include <sys/resource.h>
#include <unistd.h>

#if !ASPEN_HAVE_PAPI && defined(__linux__)
#include <linux/perf_event.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#endif


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

long long delta_or_na(long long end, long long start) {
  if (end == kCounterUnavailable || start == kCounterUnavailable) {
    return kCounterUnavailable;
  }
  return end - start;
}

struct SoftMetricsSnapshot {
  long long minor_faults;
  long long major_faults;
  long long rss_kb;
};

long long read_current_rss_kb() {
#if defined(__linux__)
  std::ifstream statm("/proc/self/statm");
  long long total_pages = 0;
  long long resident_pages = 0;
  if (statm >> total_pages >> resident_pages) {
    long long page_kb = sysconf(_SC_PAGESIZE) / 1024;
    return resident_pages * page_kb;
  }
#endif

#if defined(RUSAGE_SELF)
  struct rusage ru;
  if (getrusage(RUSAGE_SELF, &ru) == 0) {
    return ru.ru_maxrss;
  }
#endif
  return kCounterUnavailable;
}

SoftMetricsSnapshot capture_soft_metrics() {
  SoftMetricsSnapshot snap = {kCounterUnavailable,
                              kCounterUnavailable,
                              read_current_rss_kb()};
#if defined(RUSAGE_SELF)
  struct rusage ru;
  if (getrusage(RUSAGE_SELF, &ru) == 0) {
    snap.minor_faults = ru.ru_minflt;
    snap.major_faults = ru.ru_majflt;
  }
#endif
  return snap;
}

void print_csv_header() {
  cout << "Phase,Time,Cycles,Instructions,L1_Misses,L2_Misses,"
          "MinorFaults,MajorFaults,RSS_KB_Delta" << endl;
}

void print_csv_row(const string& phase_name,
                   double elapsed,
                   long long cycles,
                   long long instructions,
                   long long l1_misses,
                   long long l2_misses,
                   long long minor_faults_delta,
                   long long major_faults_delta,
                   long long rss_kb_delta) {
  cout << phase_name << "," << elapsed << "," << counter_to_csv(cycles) << ","
       << counter_to_csv(instructions) << "," << counter_to_csv(l1_misses)
       << "," << counter_to_csv(l2_misses) << ","
       << counter_to_csv(minor_faults_delta) << ","
       << counter_to_csv(major_faults_delta) << ","
       << counter_to_csv(rss_kb_delta) << endl;
}

class PapiProfiler {
 public:
#if ASPEN_HAVE_PAPI
  PapiProfiler() {
    print_csv_header();

    int ret = PAPI_library_init(PAPI_VER_CURRENT);
    if (ret != PAPI_VER_CURRENT) {
      cerr << "[PAPI] Initialization failed";
      if (ret > 0) {
        cerr << ": version mismatch";
      } else {
        cerr << ": " << PAPI_strerror(ret);
      }
      cerr << ". Continuing with timing only." << endl;
      return;
    }
    papi_initialized_ = true;

    ret = PAPI_create_eventset(&event_set_);
    if (ret != PAPI_OK) {
      cerr << "[PAPI] Failed to create event set: " << PAPI_strerror(ret)
           << ". Continuing with timing only." << endl;
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
    soft_start_ = capture_soft_metrics();
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

    SoftMetricsSnapshot soft_end = capture_soft_metrics();
    print_csv_row(
        phase_name,
        elapsed,
        cycles,
        instructions,
        l1_misses,
        l2_misses,
        delta_or_na(soft_end.minor_faults, soft_start_.minor_faults),
        delta_or_na(soft_end.major_faults, soft_start_.major_faults),
        delta_or_na(soft_end.rss_kb, soft_start_.rss_kb));
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
  SoftMetricsSnapshot soft_start_ = {kCounterUnavailable,
                                     kCounterUnavailable,
                                     kCounterUnavailable};
#else
  PapiProfiler() {
    print_csv_header();
#if defined(__linux__) && defined(__NR_perf_event_open)
    cycles_fd_ = open_counter(PERF_TYPE_HARDWARE,
                              PERF_COUNT_HW_CPU_CYCLES,
                              "cpu_cycles");
    instructions_fd_ = open_counter(PERF_TYPE_HARDWARE,
                                    PERF_COUNT_HW_INSTRUCTIONS,
                                    "instructions");
    l1_misses_fd_ = open_counter(
        PERF_TYPE_HW_CACHE,
        cache_miss_config(PERF_COUNT_HW_CACHE_L1D),
        "l1_dcache_read_misses");
    l2_misses_fd_ = open_counter(
        PERF_TYPE_HW_CACHE,
        cache_miss_config(PERF_COUNT_HW_CACHE_LL),
        "llc_read_misses_proxy_for_l2");

    if (l2_misses_fd_ != -1) {
      cerr << "[Profiler] Using LLC read misses as L2_Misses proxy in "
              "perf_event fallback." << endl;
    }
    if (cycles_fd_ == -1 &&
        instructions_fd_ == -1 &&
        l1_misses_fd_ == -1 &&
        l2_misses_fd_ == -1) {
      cerr << "[Profiler] perf_event counters unavailable. Using timing + "
              "fault/RSS deltas." << endl;
    }
#else
    cerr << "[Profiler] Built without PAPI and without Linux perf_event. "
            "Using timing + fault/RSS deltas only." << endl;
#endif
  }

  ~PapiProfiler() {
#if defined(__linux__) && defined(__NR_perf_event_open)
    close_counter(cycles_fd_);
    close_counter(instructions_fd_);
    close_counter(l1_misses_fd_);
    close_counter(l2_misses_fd_);
#endif
  }

  void start() {
    start_time_ = std::chrono::steady_clock::now();
    soft_start_ = capture_soft_metrics();
#if defined(__linux__) && defined(__NR_perf_event_open)
    reset_and_enable(cycles_fd_, "cpu_cycles");
    reset_and_enable(instructions_fd_, "instructions");
    reset_and_enable(l1_misses_fd_, "l1_dcache_read_misses");
    reset_and_enable(l2_misses_fd_, "llc_read_misses_proxy_for_l2");
#endif
  }

  void stop(const string& phase_name) {
    double elapsed =
        std::chrono::duration<double>(std::chrono::steady_clock::now() -
                                      start_time_).count();

    long long cycles = kCounterUnavailable;
    long long instructions = kCounterUnavailable;
    long long l1_misses = kCounterUnavailable;
    long long l2_misses = kCounterUnavailable;

#if defined(__linux__) && defined(__NR_perf_event_open)
    cycles = disable_and_read(cycles_fd_, "cpu_cycles");
    instructions = disable_and_read(instructions_fd_, "instructions");
    l1_misses = disable_and_read(l1_misses_fd_, "l1_dcache_read_misses");
    l2_misses = disable_and_read(l2_misses_fd_, "llc_read_misses_proxy_for_l2");
#endif

    SoftMetricsSnapshot soft_end = capture_soft_metrics();
    print_csv_row(
        phase_name,
        elapsed,
        cycles,
        instructions,
        l1_misses,
        l2_misses,
        delta_or_na(soft_end.minor_faults, soft_start_.minor_faults),
        delta_or_na(soft_end.major_faults, soft_start_.major_faults),
        delta_or_na(soft_end.rss_kb, soft_start_.rss_kb));
  }

 private:
#if defined(__linux__) && defined(__NR_perf_event_open)
  static uint64_t cache_miss_config(uint64_t cache_id) {
    return cache_id |
           (static_cast<uint64_t>(PERF_COUNT_HW_CACHE_OP_READ) << 8) |
           (static_cast<uint64_t>(PERF_COUNT_HW_CACHE_RESULT_MISS) << 16);
  }

  static int perf_event_open(struct perf_event_attr* pe) {
    return static_cast<int>(syscall(__NR_perf_event_open, pe, 0, -1, -1, 0));
  }

  static void close_counter(int& fd) {
    if (fd != -1) {
      close(fd);
      fd = -1;
    }
  }

  int open_counter(uint32_t type, uint64_t config, const char* label) {
    struct perf_event_attr pe;
    memset(&pe, 0, sizeof(struct perf_event_attr));
    pe.type = type;
    pe.size = sizeof(struct perf_event_attr);
    pe.config = config;
    pe.disabled = 1;
    pe.exclude_kernel = 1;
    pe.exclude_hv = 1;
    pe.inherit = 1;

    int fd = perf_event_open(&pe);
    if (fd == -1) {
      cerr << "[perf_event] Failed to open " << label << ": "
           << strerror(errno) << endl;
    }
    return fd;
  }

  void reset_and_enable(int& fd, const char* label) {
    if (fd == -1) {
      return;
    }
    if (ioctl(fd, PERF_EVENT_IOC_RESET, 0) == -1 ||
        ioctl(fd, PERF_EVENT_IOC_ENABLE, 0) == -1) {
      cerr << "[perf_event] Failed to start " << label << ": "
           << strerror(errno) << ". Disabling this counter." << endl;
      close_counter(fd);
    }
  }

  long long disable_and_read(int fd, const char* label) {
    if (fd == -1) {
      return kCounterUnavailable;
    }
    if (ioctl(fd, PERF_EVENT_IOC_DISABLE, 0) == -1) {
      cerr << "[perf_event] Failed to stop " << label << ": "
           << strerror(errno) << endl;
      return kCounterUnavailable;
    }

    long long value = 0;
    ssize_t nread = read(fd, &value, sizeof(value));
    if (nread != static_cast<ssize_t>(sizeof(value))) {
      cerr << "[perf_event] Failed to read " << label << ": "
           << strerror(errno) << endl;
      return kCounterUnavailable;
    }
    return value;
  }

  int cycles_fd_ = -1;
  int instructions_fd_ = -1;
  int l1_misses_fd_ = -1;
  int l2_misses_fd_ = -1;
#endif

  std::chrono::steady_clock::time_point start_time_;
  SoftMetricsSnapshot soft_start_ = {kCounterUnavailable,
                                     kCounterUnavailable,
                                     kCounterUnavailable};
#endif
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
