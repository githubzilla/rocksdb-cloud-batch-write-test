#ifndef RATE_LIMITER_H
#define RATE_LIMITER_H

#include <chrono>
#include <mutex>
#include <thread>

class RateLimiter {
 public:
  // Constructor: rate_limit is the maximum number of operations per second
  explicit RateLimiter(double rate_limit)
      : rate_limit_(rate_limit),
        min_interval_ns_(rate_limit > 0 ? static_cast<int64_t>(1e9 / rate_limit) : 0),
        initialized_(false) {}

  // Wait if necessary to maintain the rate limit
  void Wait() {
    if (rate_limit_ <= 0) {
      return;  // No rate limiting
    }

    std::lock_guard<std::mutex> lock(mutex_);
    auto now = std::chrono::steady_clock::now();

    if (initialized_) {
      auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         now - last_time_)
                         .count();
      if (elapsed < min_interval_ns_) {
        int64_t sleep_ns = min_interval_ns_ - elapsed;
        std::this_thread::sleep_for(std::chrono::nanoseconds(sleep_ns));
        now = std::chrono::steady_clock::now();
      }
    } else {
      initialized_ = true;
    }

    last_time_ = now;
  }

  // Reset the rate limiter
  void Reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    initialized_ = false;
  }

  double GetRateLimit() const { return rate_limit_; }

 private:
  double rate_limit_;           // Operations per second
  int64_t min_interval_ns_;     // Minimum interval between operations in nanoseconds
  std::chrono::steady_clock::time_point last_time_;  // Last operation time
  bool initialized_;            // Whether the rate limiter has been used
  mutable std::mutex mutex_;    // Mutex for thread safety
};

#endif  // RATE_LIMITER_H

