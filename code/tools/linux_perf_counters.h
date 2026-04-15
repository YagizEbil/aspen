#pragma once

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <exception>
#include <sstream>
#include <string>
#include <vector>

#ifdef __linux__

#include <filesystem>

#include <linux/perf_event.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

namespace aspen_bench {

// On AMD Zen CPUs PERF_COUNT_HW_CACHE_REFERENCES maps to LLC accesses and
// PERF_COUNT_HW_CACHE_MISSES maps to LLC misses.  The PERF_TYPE_HW_CACHE
// compound events (PERF_COUNT_HW_CACHE_LL) are not supported on AMD hardware.
// We therefore use the two generic hardware counters to compute LLC miss rate
// and approximate memory bandwidth.
//
// Stall counter mode: PERF_COUNT_HW_STALLED_CYCLES_FRONTEND and _BACKEND cannot
// share a perf event group with regular hardware counters on AMD Zen — doing so
// causes ALL events in the group to silently return 0.  We therefore open them in
// a SEPARATE fd group (stall_fds) per thread, synchronized with the same
// reset/enable/disable ioctls.  branch_misses is mutually exclusive with stall mode
// (caller chooses one or the other).
struct PerfCounterValues {
  uint64_t cycles = 0;
  uint64_t instructions = 0;
  uint64_t cache_references = 0;       // LLC accesses (AMD: cache-references)
  uint64_t cache_misses = 0;           // LLC misses   (AMD: cache-misses)
  uint64_t branch_misses = 0;
  uint64_t stalled_cycles_frontend = 0;
  uint64_t stalled_cycles_backend = 0;
  bool branch_misses_available = false;
  bool stall_counters_available = false;
};

class PerfCounterCollector {
 public:
  PerfCounterCollector() = default;
  ~PerfCounterCollector() { close(); }

  // request_stall_counters: when true, collect STALLED_CYCLES_FRONTEND and
  // STALLED_CYCLES_BACKEND instead of branch_misses (mutually exclusive to fit
  // within the 6-PMC-register limit on AMD Zen 5).
  bool open(bool request_branch_misses, bool request_stall_counters, std::string* error_out) {
    close();

    std::vector<pid_t> tids = enumerate_tids(error_out);
    if (tids.empty()) {
      set_error(error_out, "No threads were found under /proc/self/task.");
      return false;
    }

    // Stall counters and branch_misses use the same PMC slots — mutually exclusive.
    bool enable_branch_misses = request_branch_misses && !request_stall_counters;
    bool enable_stall = request_stall_counters;

    for (size_t i = 0; i < tids.size(); i++) {
      ThreadGroup group;
      OpenGroupResult r = open_group(tids[i], enable_branch_misses, enable_stall, &group, error_out);
      if (r == OpenGroupResult::kUnsupportedBranchMisses && enable_branch_misses) {
        close();
        enable_branch_misses = false;
        i = static_cast<size_t>(-1);
        continue;
      }
      if (r == OpenGroupResult::kUnsupportedStallCounters && enable_stall) {
        close();
        enable_stall = false;
        i = static_cast<size_t>(-1);
        continue;
      }
      if (r != OpenGroupResult::kSuccess) {
        close();
        return false;
      }
      thread_groups_.push_back(std::move(group));
    }

    branch_misses_available_ = enable_branch_misses;
    stall_counters_available_ = enable_stall;
    return true;
  }

  bool reset_and_enable(std::string* error_out) const {
    for (const auto& group : thread_groups_) {
      if (ioctl(group.fds.front(), PERF_EVENT_IOC_RESET, PERF_IOC_FLAG_GROUP) == -1) {
        set_error_with_errno(error_out,
            "Failed to reset perf counters for thread " + std::to_string(group.tid) + ".");
        return false;
      }
      if (ioctl(group.fds.front(), PERF_EVENT_IOC_ENABLE, PERF_IOC_FLAG_GROUP) == -1) {
        set_error_with_errno(error_out,
            "Failed to enable perf counters for thread " + std::to_string(group.tid) + ".");
        return false;
      }
      // Stall group is a separate fd group — needs its own reset + enable.
      if (!group.stall_fds.empty()) {
        if (ioctl(group.stall_fds.front(), PERF_EVENT_IOC_RESET, PERF_IOC_FLAG_GROUP) == -1) {
          set_error_with_errno(error_out,
              "Failed to reset stall counters for thread " + std::to_string(group.tid) + ".");
          return false;
        }
        if (ioctl(group.stall_fds.front(), PERF_EVENT_IOC_ENABLE, PERF_IOC_FLAG_GROUP) == -1) {
          set_error_with_errno(error_out,
              "Failed to enable stall counters for thread " + std::to_string(group.tid) + ".");
          return false;
        }
      }
    }
    return true;
  }

  bool disable(std::string* error_out) const {
    for (const auto& group : thread_groups_) {
      if (ioctl(group.fds.front(), PERF_EVENT_IOC_DISABLE, PERF_IOC_FLAG_GROUP) == -1) {
        set_error_with_errno(error_out,
            "Failed to disable perf counters for thread " + std::to_string(group.tid) + ".");
        return false;
      }
      if (!group.stall_fds.empty()) {
        if (ioctl(group.stall_fds.front(), PERF_EVENT_IOC_DISABLE, PERF_IOC_FLAG_GROUP) == -1) {
          set_error_with_errno(error_out,
              "Failed to disable stall counters for thread " + std::to_string(group.tid) + ".");
          return false;
        }
      }
    }
    return true;
  }

  bool read(PerfCounterValues* totals, std::string* error_out) const {
    if (!totals) {
      set_error(error_out, "PerfCounterCollector::read called with a null output pointer.");
      return false;
    }

    PerfCounterValues result;
    result.branch_misses_available  = branch_misses_available_;
    result.stall_counters_available = stall_counters_available_;

    // Main group buffer layout (PERF_FORMAT_GROUP):
    //   [num_events, cycles, instructions, cache_references, cache_misses, [branch_misses]]
    // In stall mode the main group has exactly 4 events (no branch_misses);
    // the stall events live in a separate group (stall_fds) to avoid an AMD Zen bug where
    // mixing STALLED_CYCLES_* events in a hardware group causes all counters to return 0.
    const size_t main_optional = branch_misses_available_ ? 1u : 0u;
    const size_t main_expected = 4 + main_optional;
    // Buffer: 1 header word + up to 5 event words = 6 slots. Keep 8 for safety.
    uint64_t buffer[8] = {0};

    for (const auto& group : thread_groups_) {
      // ── Read main group ────────────────────────────────────────────────────
      const ssize_t main_expected_bytes =
          static_cast<ssize_t>((1 + main_expected) * sizeof(uint64_t));
      ssize_t bytes_read = ::read(group.fds.front(), buffer, sizeof(buffer));
      if (bytes_read != main_expected_bytes) {
        std::ostringstream out;
        out << "Failed to read perf counters for thread " << group.tid
            << ": expected " << main_expected_bytes << " bytes but read " << bytes_read << ".";
        set_error(error_out, out.str());
        return false;
      }
      if (buffer[0] != main_expected) {
        std::ostringstream out;
        out << "Perf counter read for thread " << group.tid
            << " returned " << buffer[0] << " events; expected " << main_expected << ".";
        set_error(error_out, out.str());
        return false;
      }

      result.cycles           += buffer[1];
      result.instructions     += buffer[2];
      result.cache_references += buffer[3];
      result.cache_misses     += buffer[4];
      if (branch_misses_available_) {
        result.branch_misses  += buffer[5];
      }

      // ── Read stall group (separate from main group) ────────────────────────
      if (stall_counters_available_ && !group.stall_fds.empty()) {
        // Layout: [nr=2, stall_frontend, stall_backend]
        uint64_t sbuf[4] = {0};
        const ssize_t stall_expected_bytes = 3 * static_cast<ssize_t>(sizeof(uint64_t));
        ssize_t stall_bytes = ::read(group.stall_fds.front(), sbuf, sizeof(sbuf));
        if (stall_bytes != stall_expected_bytes) {
          std::ostringstream out;
          out << "Failed to read stall counters for thread " << group.tid
              << ": expected " << stall_expected_bytes << " bytes but read " << stall_bytes << ".";
          set_error(error_out, out.str());
          return false;
        }
        if (sbuf[0] != 2u) {
          std::ostringstream out;
          out << "Stall counter read for thread " << group.tid
              << " returned " << sbuf[0] << " events; expected 2.";
          set_error(error_out, out.str());
          return false;
        }
        result.stalled_cycles_frontend += sbuf[1];
        result.stalled_cycles_backend  += sbuf[2];
      }
    }

    *totals = result;
    return true;
  }

  bool branch_misses_available() const { return branch_misses_available_; }
  bool stall_counters_available() const { return stall_counters_available_; }

  void close() {
    for (auto& group : thread_groups_) {
      for (int fd : group.fds)       { if (fd != -1) ::close(fd); }
      for (int fd : group.stall_fds) { if (fd != -1) ::close(fd); }
    }
    thread_groups_.clear();
    branch_misses_available_ = false;
    stall_counters_available_ = false;
  }

 private:
  struct ThreadGroup {
    pid_t tid = 0;
    std::vector<int> fds;       // main group: cycles, instructions, cache_refs, cache_misses, [branch_misses]
    std::vector<int> stall_fds; // separate stall group: stall_frontend (leader), stall_backend
                                // AMD Zen: mixing stall events in the main group causes all counters
                                // to return 0, so they must live in their own group.
  };

  enum class OpenGroupResult {
    kSuccess,
    kUnsupportedBranchMisses,
    kUnsupportedStallCounters,
    kError,
  };

  static long perf_event_open(struct perf_event_attr* attr, pid_t pid, int cpu,
                               int group_fd, unsigned long flags) {
    return syscall(__NR_perf_event_open, attr, pid, cpu, group_fd, flags);
  }

  static void set_error(std::string* error_out, const std::string& message) {
    if (error_out) *error_out = message;
  }

  static void set_error_with_errno(std::string* error_out, const std::string& prefix) {
    if (!error_out) return;
    std::ostringstream out;
    out << prefix << " errno=" << errno << " (" << std::strerror(errno) << ")";
    *error_out = out.str();
  }

  static bool is_unsupported(int err) {
    return err == EINVAL || err == ENOENT || err == EOPNOTSUPP || err == ENOSYS;
  }

  static std::vector<pid_t> enumerate_tids(std::string* error_out) {
    std::vector<pid_t> tids;
    std::error_code ec;
    for (const auto& entry : std::filesystem::directory_iterator("/proc/self/task", ec)) {
      if (ec) break;
      const std::string name = entry.path().filename().string();
      try { tids.push_back(static_cast<pid_t>(std::stol(name))); }
      catch (const std::exception&) { continue; }
    }
    if (ec) {
      set_error(error_out, "Failed to enumerate /proc/self/task: " + ec.message());
      return {};
    }
    std::sort(tids.begin(), tids.end());
    return tids;
  }

  static int open_event(uint64_t config, pid_t tid, int group_fd,
                        bool is_group_leader, std::string* error_out) {
    struct perf_event_attr attr;
    std::memset(&attr, 0, sizeof(attr));
    attr.size        = sizeof(attr);
    attr.type        = PERF_TYPE_HARDWARE;
    attr.config      = config;
    attr.disabled    = is_group_leader ? 1 : 0;
    attr.exclude_kernel = 1;
    attr.exclude_hv  = 1;
    attr.read_format = PERF_FORMAT_GROUP;
    long fd = perf_event_open(&attr, tid, -1, group_fd, 0);
    if (fd == -1) {
      set_error_with_errno(error_out,
          "Failed to open perf event config=" + std::to_string(config) +
          " for thread " + std::to_string(tid) + ".");
      return -1;
    }
    return static_cast<int>(fd);
  }

  OpenGroupResult open_group(pid_t tid, bool include_branch_misses, bool include_stall,
                              ThreadGroup* group, std::string* error_out) {
    group->tid = tid;

    int leader = open_event(PERF_COUNT_HW_CPU_CYCLES, tid, -1, true, error_out);
    if (leader == -1) return OpenGroupResult::kError;
    group->fds.push_back(leader);

    int instr = open_event(PERF_COUNT_HW_INSTRUCTIONS, tid, leader, false, error_out);
    if (instr == -1) { close_group(group); return OpenGroupResult::kError; }
    group->fds.push_back(instr);

    // cache_references = LLC accesses (on AMD: maps to L3 lookup count)
    int cache_refs = open_event(PERF_COUNT_HW_CACHE_REFERENCES, tid, leader, false, error_out);
    if (cache_refs == -1) { close_group(group); return OpenGroupResult::kError; }
    group->fds.push_back(cache_refs);

    // cache_misses = LLC misses (on AMD: maps to L3 misses going to DRAM)
    int cache_miss = open_event(PERF_COUNT_HW_CACHE_MISSES, tid, leader, false, error_out);
    if (cache_miss == -1) { close_group(group); return OpenGroupResult::kError; }
    group->fds.push_back(cache_miss);

    if (include_branch_misses) {
      std::string bm_err;
      int bm = open_event(PERF_COUNT_HW_BRANCH_MISSES, tid, leader, false, &bm_err);
      if (bm == -1) {
        const int err = errno;
        close_group(group);
        if (is_unsupported(err)) return OpenGroupResult::kUnsupportedBranchMisses;
        set_error(error_out, bm_err);
        return OpenGroupResult::kError;
      }
      group->fds.push_back(bm);
    }

    if (include_stall) {
      // AMD Zen: PERF_COUNT_HW_STALLED_CYCLES_FRONTEND/BACKEND cannot be placed in the
      // same perf event group as regular hardware counters — doing so causes all counters
      // in the group to return 0.  Open them in a SEPARATE group (stall_frontend is the
      // leader of that group) so that the main group is unaffected.
      std::string sf_err;
      int sf = open_event(PERF_COUNT_HW_STALLED_CYCLES_FRONTEND, tid, -1, true, &sf_err);
      if (sf == -1) {
        const int err = errno;
        close_group(group);
        if (is_unsupported(err)) return OpenGroupResult::kUnsupportedStallCounters;
        set_error(error_out, sf_err);
        return OpenGroupResult::kError;
      }
      group->stall_fds.push_back(sf);

      std::string sb_err;
      int sb = open_event(PERF_COUNT_HW_STALLED_CYCLES_BACKEND, tid, sf, false, &sb_err);
      if (sb == -1) {
        const int err = errno;
        close_group(group);
        if (is_unsupported(err)) return OpenGroupResult::kUnsupportedStallCounters;
        set_error(error_out, sb_err);
        return OpenGroupResult::kError;
      }
      group->stall_fds.push_back(sb);
    }

    return OpenGroupResult::kSuccess;
  }

  static void close_group(ThreadGroup* group) {
    for (int fd : group->fds) { if (fd != -1) ::close(fd); }
    group->fds.clear();
    for (int fd : group->stall_fds) { if (fd != -1) ::close(fd); }
    group->stall_fds.clear();
  }

  std::vector<ThreadGroup> thread_groups_;
  bool branch_misses_available_  = false;
  bool stall_counters_available_ = false;
};

}  // namespace aspen_bench

#else

namespace aspen_bench {

struct PerfCounterValues {
  uint64_t cycles = 0;
  uint64_t instructions = 0;
  uint64_t cache_references = 0;
  uint64_t cache_misses = 0;
  uint64_t branch_misses = 0;
  uint64_t stalled_cycles_frontend = 0;
  uint64_t stalled_cycles_backend = 0;
  bool branch_misses_available = false;
  bool stall_counters_available = false;
};

class PerfCounterCollector {
 public:
  bool open(bool, bool, std::string* error_out) {
    if (error_out) *error_out = "Linux perf counters are only supported on Linux.";
    return false;
  }
  bool reset_and_enable(std::string*) const { return false; }
  bool disable(std::string*) const { return false; }
  bool read(PerfCounterValues*, std::string*) const { return false; }
  bool branch_misses_available() const { return false; }
  bool stall_counters_available() const { return false; }
  void close() {}
};

}  // namespace aspen_bench

#endif
