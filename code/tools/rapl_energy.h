#pragma once

// AMD/Intel RAPL energy reader via the Linux perf subsystem.
//
// On AMD Threadripper (Zen), socket energy is exposed through the `power` PMU:
//   /sys/bus/event_source/devices/power/type          → PMU type ID (e.g. 15)
//   /sys/bus/event_source/devices/power/events/energy-pkg → event config (e.g. event=0x02)
//
// This is equivalent to Intel RAPL energy-pkg.  The counter is system-wide (per socket),
// so perf_event_open must use pid=-1, cpu=0.  That requires perf_event_paranoid ≤ 0.
// When permission is denied the reader silently marks itself unavailable and all
// snapshot() calls return 0 — callers check available() before using deltas.
//
// Usage:
//   RaplEnergyReader rapl;
//   rapl.open();                             // no-op if unavailable
//   uint64_t before = rapl.snapshot();
//   do_work();
//   uint64_t after  = rapl.snapshot();
//   double joules   = rapl.delta_joules(before, after);

#include <cstdint>
#include <string>

#ifdef __linux__

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <linux/perf_event.h>
#include <sstream>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

namespace aspen_bench {

class RaplEnergyReader {
 public:
  RaplEnergyReader() = default;
  ~RaplEnergyReader() { close(); }

  // Try to open the AMD/Intel RAPL energy-pkg perf counter.
  // Returns true if energy measurement will be available.
  bool open() {
    available_ = false;
    fd_ = -1;
    max_range_nj_ = UINT64_MAX;

    uint32_t pmu_type = 0;
    uint64_t event_config = 0;

    if (!read_pmu_type(&pmu_type)) return false;
    if (!read_event_config(&event_config)) return false;

    // Best-effort: read wrap-around limit so we can detect counter rollover.
    read_max_range();

    // AMD Zen does not support PERF_FORMAT_TOTAL_TIME_ENABLED/_RUNNING for RAPL
    // events — use read_format=0 (simple 8-byte raw counter).
    // The AMD Threadripper power PMU counter unit is nanojoules (not microjoules).
    struct perf_event_attr attr{};
    attr.size        = sizeof(attr);
    attr.type        = pmu_type;
    attr.config      = event_config;
    attr.disabled    = 0;          // start counting immediately
    attr.exclude_kernel = 0;       // RAPL must include kernel time (socket-wide)
    attr.exclude_hv  = 0;
    attr.read_format = 0;          // simple uint64 raw read

    // Determine which CPU to open on from the PMU's cpumask.
    int cpu = read_cpumask_cpu();  // typically 0 on single-socket

    // pid=-1, cpu=N: system-wide on that CPU's die.  Requires perf_event_paranoid <= 0.
    long fd = syscall(__NR_perf_event_open, &attr,
                      static_cast<pid_t>(-1),
                      cpu,
                      -1,
                      static_cast<unsigned long>(PERF_FLAG_FD_CLOEXEC));
    if (fd == -1) {
      return false;  // EACCES / EPERM when paranoid > 0
    }

    fd_ = static_cast<int>(fd);
    available_ = true;
    return true;
  }

  void close() {
    if (fd_ != -1) {
      ::close(fd_);
      fd_ = -1;
    }
    available_ = false;
  }

  bool available() const { return available_; }

  // Read the current raw counter value (nanojoules on AMD Threadripper).
  // Returns 0 if unavailable.
  uint64_t snapshot() const {
    if (!available_) return 0;
    uint64_t val = 0;
    if (::read(fd_, &val, sizeof(val)) != static_cast<ssize_t>(sizeof(val))) return 0;
    return val;
  }

  // Compute Joules from two raw counter snapshots, handling counter wrap.
  // Counter unit is nanojoules on AMD Threadripper.
  double delta_joules(uint64_t before_nj, uint64_t after_nj) const {
    uint64_t delta_raw;
    if (after_nj >= before_nj) {
      delta_raw = after_nj - before_nj;
    } else {
      // Counter wrapped around max_energy_range.
      // max_range_nj_ is the wrap-around point in nanojoules.
      delta_raw = (max_range_nj_ - before_nj) + after_nj;
    }
    return static_cast<double>(delta_raw) * 1e-9;  // nJ → J
  }

 private:
  int      fd_           = -1;
  bool     available_    = false;
  uint64_t max_range_nj_ = UINT64_MAX;  // wrap-around point in nanojoules

  // Read the first CPU listed in the power PMU's cpumask (e.g. "0" → 0).
  static int read_cpumask_cpu() {
    std::ifstream f("/sys/bus/event_source/devices/power/cpumask");
    if (!f.is_open()) return 0;
    int cpu = 0;
    f >> cpu;
    return (f && cpu >= 0) ? cpu : 0;
  }

  static bool read_pmu_type(uint32_t* out) {
    std::ifstream f("/sys/bus/event_source/devices/power/type");
    if (!f.is_open()) return false;
    uint64_t v = 0;
    f >> v;
    if (!f) return false;
    *out = static_cast<uint32_t>(v);
    return true;
  }

  static bool read_event_config(uint64_t* out) {
    std::ifstream f("/sys/bus/event_source/devices/power/events/energy-pkg");
    if (!f.is_open()) return false;
    std::string line;
    if (!std::getline(f, line)) return false;
    // Format: "event=0x02"
    const auto eq = line.find('=');
    if (eq == std::string::npos) return false;
    std::string val = line.substr(eq + 1);
    try {
      *out = std::stoull(val, nullptr, 0);
    } catch (...) {
      return false;
    }
    return true;
  }

  void read_max_range() {
    // The sysfs file reports the max range in microjoules; convert to nanojoules
    // to match the unit of the raw perf counter on AMD Threadripper.
    std::ifstream f("/sys/bus/event_source/devices/power/max_energy_range_uj");
    if (!f.is_open()) return;
    uint64_t uj = 0;
    f >> uj;
    if (f && uj > 0) max_range_nj_ = uj * 1000;  // µJ → nJ
  }
};

}  // namespace aspen_bench

#else  // non-Linux stub

namespace aspen_bench {

class RaplEnergyReader {
 public:
  bool open() { return false; }
  void close() {}
  bool available() const { return false; }
  uint64_t snapshot() const { return 0; }
  double delta_joules(uint64_t, uint64_t) const { return 0.0; }
};

}  // namespace aspen_bench

#endif
