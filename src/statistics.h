#ifndef STATISTICS_H
#define STATISTICS_H

#include <algorithm>
#include <chrono>
#include <cmath>
#include <mutex>
#include <vector>

class Statistics {
 public:
  Statistics() : count_(0), total_latency_ns_(0), total_write_size_bytes_(0) {}

  // Record a latency measurement in nanoseconds
  void RecordLatency(int64_t latency_ns) {
    std::lock_guard<std::mutex> lock(mutex_);
    latencies_.push_back(latency_ns);
    total_latency_ns_ += latency_ns;
    count_++;
  }

  // Record a latency measurement with write size in bytes
  void RecordLatencyWithSize(int64_t latency_ns, int64_t write_size_bytes) {
    std::lock_guard<std::mutex> lock(mutex_);
    latencies_.push_back(latency_ns);
    total_latency_ns_ += latency_ns;
    total_write_size_bytes_ += write_size_bytes;
    count_++;
  }

  // Calculate and return statistics
  struct Stats {
    int64_t count;
    int64_t min_ns;
    int64_t max_ns;
    double avg_ns;
    double p50_ns;
    double p95_ns;
    double p99_ns;
    double throughput_ops_per_sec;
    double throughput_bytes_per_sec;
  };

  Stats GetStats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    Stats stats;
    stats.count = count_;

    if (latencies_.empty()) {
      stats.min_ns = 0;
      stats.max_ns = 0;
      stats.avg_ns = 0;
      stats.p50_ns = 0;
      stats.p95_ns = 0;
      stats.p99_ns = 0;
      stats.throughput_ops_per_sec = 0;
      stats.throughput_bytes_per_sec = 0;
      return stats;
    }

    // Create a sorted copy for percentile calculation
    std::vector<int64_t> sorted = latencies_;
    std::sort(sorted.begin(), sorted.end());

    stats.min_ns = sorted.front();
    stats.max_ns = sorted.back();
    stats.avg_ns = static_cast<double>(total_latency_ns_) / count_;

    // Calculate percentiles
    auto percentile = [&sorted](double p) -> double {
      if (sorted.empty()) return 0;
      double index = p * (sorted.size() - 1);
      size_t lower = static_cast<size_t>(std::floor(index));
      size_t upper = static_cast<size_t>(std::ceil(index));
      if (lower == upper) {
        return sorted[lower];
      }
      double weight = index - lower;
      return sorted[lower] * (1 - weight) + sorted[upper] * weight;
    };

    stats.p50_ns = percentile(0.50);
    stats.p95_ns = percentile(0.95);
    stats.p99_ns = percentile(0.99);

    // Calculate throughput (operations per second)
    // Assuming we have a total time, we'll need to track that separately
    // For now, we'll use average latency to estimate
    if (stats.avg_ns > 0) {
      stats.throughput_ops_per_sec = 1e9 / stats.avg_ns;
    } else {
      stats.throughput_ops_per_sec = 0;
    }

    // Calculate throughput in bytes per second (will be calculated properly in TimeBasedStatistics)
    stats.throughput_bytes_per_sec = 0;

    return stats;
  }

  // Reset all statistics
  void Reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    latencies_.clear();
    count_ = 0;
    total_latency_ns_ = 0;
    total_write_size_bytes_ = 0;
  }

  // Get count of recorded measurements
  int64_t GetCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return count_;
  }

  // Get total write size in bytes
  int64_t GetTotalWriteSizeBytes() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return total_write_size_bytes_;
  }

 private:
  mutable std::mutex mutex_;
  std::vector<int64_t> latencies_;
  int64_t count_;
  int64_t total_latency_ns_;
  int64_t total_write_size_bytes_;
};

// Enhanced statistics that tracks time-based throughput
class TimeBasedStatistics {
 public:
  TimeBasedStatistics() : start_time_(std::chrono::steady_clock::now()) {}

  void RecordLatency(int64_t latency_ns) {
    stats_.RecordLatency(latency_ns);
  }

  void RecordLatencyWithSize(int64_t latency_ns, int64_t write_size_bytes) {
    stats_.RecordLatencyWithSize(latency_ns, write_size_bytes);
  }

  Statistics::Stats GetStats() const {
    auto stats = stats_.GetStats();
    
    // Calculate actual throughput based on elapsed time
    auto end_time = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
                       end_time - start_time_)
                       .count();
    
    if (elapsed > 0 && stats.count > 0) {
      stats.throughput_ops_per_sec = 
          (static_cast<double>(stats.count) * 1e9) / elapsed;
      
      // Calculate throughput in bytes per second
      int64_t total_write_size = stats_.GetTotalWriteSizeBytes();
      stats.throughput_bytes_per_sec = 
          (static_cast<double>(total_write_size) * 1e9) / elapsed;
    }

    return stats;
  }

  // Get stats for a specific time period (from start_time to now)
  Statistics::Stats GetStatsForPeriod(
      std::chrono::steady_clock::time_point period_start) const {
    auto stats = stats_.GetStats();
    
    // Calculate actual throughput based on elapsed time from period_start
    auto end_time = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
                       end_time - period_start)
                       .count();
    
    if (elapsed > 0 && stats.count > 0) {
      stats.throughput_ops_per_sec = 
          (static_cast<double>(stats.count) * 1e9) / elapsed;
      
      // Calculate throughput in bytes per second
      int64_t total_write_size = stats_.GetTotalWriteSizeBytes();
      stats.throughput_bytes_per_sec = 
          (static_cast<double>(total_write_size) * 1e9) / elapsed;
    }

    return stats;
  }

  // Get stats and reset (for interval reporting)
  Statistics::Stats GetStatsAndReset() {
    auto stats = GetStats();
    Reset();
    return stats;
  }

  void Reset() {
    stats_.Reset();
    start_time_ = std::chrono::steady_clock::now();
  }

  std::chrono::steady_clock::time_point GetStartTime() const {
    return start_time_;
  }

 private:
  Statistics stats_;
  std::chrono::steady_clock::time_point start_time_;
};

#endif  // STATISTICS_H

