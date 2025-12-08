#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "rocksdb/cache.h"
#include "rocksdb/cloud/cloud_file_system.h"
#include "rocksdb/cloud/cloud_file_system_impl.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/cloud/db_cloud.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/options_util.h"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/s3/S3Client.h>

#include "rate_limiter.h"
#include "statistics.h"

using ROCKSDB_NAMESPACE::BlockBasedTableOptions;
using ROCKSDB_NAMESPACE::ColumnFamilyHandle;
using ROCKSDB_NAMESPACE::ConfigOptions;
using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::Env;
using ROCKSDB_NAMESPACE::FileSystem;
using ROCKSDB_NAMESPACE::NewBlockBasedTableFactory;
using ROCKSDB_NAMESPACE::NewLRUCache;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;

using ROCKSDB_NAMESPACE::CloudFileSystem;
using ROCKSDB_NAMESPACE::DBCloud;
using ROCKSDB_NAMESPACE::S3ClientFactory;

DEFINE_string(batch_sizes, "10000,50000,100000",
              "Comma-separated list of batch sizes in bytes");
DEFINE_string(db_path, "rocksdb_batch_test_data", "Path to RocksDB database");
DEFINE_string(config_file, "config/db_options.ini",
              "Path to INI configuration file");
DEFINE_int32(test_duration_seconds, 120,
             "Duration of test in seconds (0 means run indefinitely)");
DEFINE_int32(value_size_bytes, 60, "Size of each value in bytes");
DEFINE_int32(report_interval_seconds , 0,
             "Statistics report interval in seconds (0 means no periodic dump)");
DEFINE_int64(block_cache_size, 0,
             "Block cache size in bytes (0 means no block cache)");

// Global stop flag for signal handler
static std::atomic<bool> g_stop_requested(false);

// Signal handler for Ctrl+C (SIGINT)
static void SignalHandler(int signal) {
  if (signal == SIGINT) {
    LOG(INFO) << "Received SIGINT (Ctrl+C), stopping test gracefully...";
    g_stop_requested.store(true);
  }
}

// Simple INI file parser
class IniParser {
public:
  static std::map<std::string, std::map<std::string, std::string>>
  Parse(const std::string &filename) {
    std::map<std::string, std::map<std::string, std::string>> config;
    std::ifstream file(filename);
    if (!file.is_open()) {
      LOG(WARNING) << "Cannot open config file: " << filename;
      return config;
    }

    std::string line;
    std::string current_section;
    while (std::getline(file, line)) {
      // Remove whitespace
      line.erase(0, line.find_first_not_of(" \t\r\n"));
      line.erase(line.find_last_not_of(" \t\r\n") + 1);

      // Skip empty lines and comments
      if (line.empty() || line[0] == '#' || line[0] == ';') {
        continue;
      }

      // Check for section header
      if (line[0] == '[' && line.back() == ']') {
        current_section = line.substr(1, line.length() - 2);
        continue;
      }

      // Parse key=value
      size_t pos = line.find('=');
      if (pos != std::string::npos) {
        std::string key = line.substr(0, pos);
        std::string value = line.substr(pos + 1);
        // Trim whitespace
        key.erase(0, key.find_first_not_of(" \t"));
        key.erase(key.find_last_not_of(" \t") + 1);
        value.erase(0, value.find_first_not_of(" \t"));
        value.erase(value.find_last_not_of(" \t") + 1);
        config[current_section][key] = value;
      }
    }
    return config;
  }
};

// Parse batch sizes from comma-separated string
std::vector<int> ParseBatchSizes(const std::string &batch_sizes_str) {
  std::vector<int> sizes;
  std::istringstream iss(batch_sizes_str);
  std::string token;
  while (std::getline(iss, token, ',')) {
    try {
      int size = std::stoi(token);
      if (size > 0) {
        sizes.push_back(size);
      }
    } catch (const std::exception &e) {
      LOG(WARNING) << "Invalid batch size: " << token;
    }
  }
  return sizes;
}

// Create directory if it doesn't exist
bool CreateDirectoryIfNotExists(const std::string &path) {
  try {
    std::filesystem::path dir_path(path);
    if (std::filesystem::exists(dir_path)) {
      if (std::filesystem::is_directory(dir_path)) {
        LOG(INFO) << "Directory already exists: " << path;
        return true;
      } else {
        LOG(ERROR) << "Path exists but is not a directory: " << path;
        return false;
      }
    }
    
    // Create parent directories if needed
    std::filesystem::create_directories(dir_path);
    LOG(INFO) << "Created directory: " << path;
    return true;
  } catch (const std::exception &e) {
    LOG(ERROR) << "Failed to create directory " << path << ": " << e.what();
    return false;
  }
}

// Helper function to calculate row size (key + value)
size_t CalculateRowSize(int value_size_bytes) {
  const size_t estimated_key_size = 50;
  return estimated_key_size + value_size_bytes;
}

// Helper function to calculate number of rows needed for a target batch size in bytes
int CalculateRowsForBatchSize(int batch_size_bytes, int value_size_bytes) {
  size_t row_size = CalculateRowSize(value_size_bytes);
  if (row_size == 0) return 0;
  return batch_size_bytes / row_size;
}

// Helper function to generate a string of specified size
std::string GenerateStringOfSize(size_t target_size, const std::string &base) {
  std::string result = base;
  if (result.size() < target_size) {
    // Pad with 'X' characters to reach target size
    result.append(target_size - result.size(), 'X');
  } else if (result.size() > target_size) {
    // Truncate if base string is larger than target
    result = result.substr(0, target_size);
  }
  return result;
}

// Helper function to get current timestamp as string
std::string GetCurrentTimestamp() {
  auto now = std::chrono::system_clock::now();
  auto time_t = std::chrono::system_clock::to_time_t(now);
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      now.time_since_epoch()) % 1000;
  
  std::stringstream ss;
  ss << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S");
  ss << "." << std::setfill('0') << std::setw(3) << ms.count();
  return ss.str();
}

// Helper function to format throughput in KB/s and MB/s
void PrintDataThroughput(double throughput_bytes_per_sec) {
  double throughput_kb_per_sec = throughput_bytes_per_sec / 1024.0;
  double throughput_mb_per_sec = throughput_kb_per_sec / 1024.0;
  
  if (throughput_mb_per_sec >= 1.0) {
    std::cout << "Throughput: " << throughput_mb_per_sec << " MB/s ("
              << throughput_kb_per_sec << " KB/s)" << std::endl;
  } else if (throughput_kb_per_sec >= 1.0) {
    std::cout << "Throughput: " << throughput_kb_per_sec << " KB/s" << std::endl;
  } else {
    std::cout << "Throughput: " << throughput_bytes_per_sec << " bytes/s" << std::endl;
  }
}

// Function to print interval statistics
void PrintIntervalStats(const Statistics::Stats &stats_result,
                        int64_t interval_seconds) {
  std::cout << "\n=== Interval Stats (interval: " << interval_seconds << "s) ===" << std::endl;
  std::cout << "Timestamp: " << GetCurrentTimestamp() << std::endl;
  std::cout << "Batches in interval: " << stats_result.count << std::endl;
  std::cout << "Latency statistics (nanoseconds):" << std::endl;
  std::cout << "  Min:    " << stats_result.min_ns << std::endl;
  std::cout << "  Max:    " << stats_result.max_ns << std::endl;
  std::cout << "  Avg:    " << static_cast<int64_t>(stats_result.avg_ns)
            << std::endl;
  std::cout << "  P50:    " << static_cast<int64_t>(stats_result.p50_ns)
            << std::endl;
  std::cout << "  P95:    " << static_cast<int64_t>(stats_result.p95_ns)
            << std::endl;
  std::cout << "  P99:    " << static_cast<int64_t>(stats_result.p99_ns)
            << std::endl;
  std::cout << "Throughput: " << stats_result.throughput_ops_per_sec
            << " batches/sec" << std::endl;
  PrintDataThroughput(stats_result.throughput_bytes_per_sec);
  
  std::cout << std::endl;
}

// Function to print cumulative statistics
void PrintCumulativeStats(const Statistics::Stats &stats_result,
                          int64_t total_seconds) {
  std::cout << "\n=== Final Cumulative Results ===" << std::endl;
  std::cout << "Timestamp: " << GetCurrentTimestamp() << std::endl;
  std::cout << "Total batches: " << stats_result.count << std::endl;
  std::cout << "Test duration: " << total_seconds << " seconds" << std::endl;
  std::cout << "Latency statistics (nanoseconds):" << std::endl;
  std::cout << "  Min:    " << stats_result.min_ns << std::endl;
  std::cout << "  Max:    " << stats_result.max_ns << std::endl;
  std::cout << "  Avg:    " << static_cast<int64_t>(stats_result.avg_ns)
            << std::endl;
  std::cout << "  P50:    " << static_cast<int64_t>(stats_result.p50_ns)
            << std::endl;
  std::cout << "  P95:    " << static_cast<int64_t>(stats_result.p95_ns)
            << std::endl;
  std::cout << "  P99:    " << static_cast<int64_t>(stats_result.p99_ns)
            << std::endl;
  std::cout << "Throughput: " << stats_result.throughput_ops_per_sec
            << " batches/sec" << std::endl;
  PrintDataThroughput(stats_result.throughput_bytes_per_sec);
  
  std::cout << std::endl;
}

// Worker function for batch writes - cycles through batch sizes
// OPTIMIZED: Reduced CPU usage and thread contention
void BatchWriteWorker(DB *db, const std::vector<int> &batch_sizes, int num_batches,
                      RateLimiter *rate_limiter,
                      TimeBasedStatistics *interval_stats,
                      TimeBasedStatistics *cumulative_stats,
                      std::atomic<bool> *stop_flag,
                      int value_size_bytes) {
  WriteOptions write_options;
  write_options.sync = false;
  write_options.disableWAL = true;

  size_t batch_size_index = 0;
  int batch_count = 0;
  
  // Pre-compute thread ID hash once (avoid repeated computation)
  const size_t thread_id_hash = std::hash<std::thread::id>{}(std::this_thread::get_id());
  const std::string thread_id_str = std::to_string(thread_id_hash);
  
  // Pre-generate value template to reduce string operations
  const std::string value_template(value_size_bytes, 'X');
  
  // Use per-thread batch counter to avoid contention on global atomic
  // Each thread gets a unique base ID, then increments locally
  static std::atomic<uint64_t> global_thread_base_counter(0);
  const uint64_t thread_base_id = global_thread_base_counter.fetch_add(1000000);
  uint64_t local_batch_counter = thread_base_id;
  
  // Batch statistics updates to reduce mutex contention
  // Store measurements locally and flush in batches
  struct LocalStats {
    std::vector<int64_t> latencies;
    std::vector<int64_t> write_sizes;
  };
  LocalStats local_interval_stats;
  LocalStats local_cumulative_stats;
  const int STATS_BATCH_SIZE = 20;  // Flush every N writes to reduce mutex contention
  
  while (!stop_flag->load() && !g_stop_requested.load() &&
         (num_batches <= 0 || batch_count < num_batches)) {
    // Cycle through batch sizes (in bytes): e.g., 10000, 50000, 100000, ...
    int batch_size_bytes = batch_sizes[batch_size_index];
    batch_size_index = (batch_size_index + 1) % batch_sizes.size();

    // Calculate number of rows needed to achieve target batch size
    int num_rows = CalculateRowsForBatchSize(batch_size_bytes, value_size_bytes);
    if (num_rows == 0) {
      LOG(WARNING) << "Batch size " << batch_size_bytes 
                   << " bytes is too small for at least one row, skipping";
      continue;
    }

    // Pre-generate keys and prepare batch (OUTSIDE timing measurement)
    WriteBatch batch;
    
    // Get unique batch ID from local counter (no contention)
    const uint64_t batch_id = local_batch_counter++;
    
    // Cache string conversion - only convert when batch_id changes significantly
    // Reuse string if batch_id is close to last converted value
    static thread_local uint64_t last_converted_batch_id = 0;
    static thread_local std::string cached_batch_id_str;
    if (batch_id - last_converted_batch_id > 100 || last_converted_batch_id == 0) {
      cached_batch_id_str = std::to_string(batch_id);
      last_converted_batch_id = batch_id;
    }
    const std::string& batch_id_str = cached_batch_id_str;
    
    // Pre-allocate key strings to reduce allocations
    // Estimate key size: "key_" + batch_id + "_" + row_id + "_" + thread_id
    const size_t estimated_key_size = 20 + batch_id_str.size() + thread_id_str.size();
    
    // Generate keys and values efficiently
    for (int i = 0; i < num_rows; ++i) {
      // Build key efficiently using reserve and append
      std::string key;
      key.reserve(estimated_key_size);
      key = "key_";
      key += batch_id_str;
      key += "_";
      key += std::to_string(i);
      key += "_";
      key += thread_id_str;
      
      // Use pre-generated value template (no string operations needed)
      batch.Put(key, value_template);
    }
    
    // Calculate approximate batch size for statistics
    // Note: WriteBatch doesn't expose GetDataSize(), so we use target size
    // The actual size may be slightly larger due to RocksDB overhead
    const size_t actual_batch_size = batch_size_bytes;
    
    // Rate limit AFTER batch preparation, BEFORE write
    rate_limiter->Wait();
    
    // Measure ONLY the write operation time (not batch preparation)
    auto start = std::chrono::steady_clock::now();
    Status s = db->Write(write_options, &batch);
    auto end = std::chrono::steady_clock::now();

    if (s.ok()) {
      auto latency_ns =
          std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
              .count();
      
      // Store statistics locally to reduce mutex contention
      local_interval_stats.latencies.push_back(latency_ns);
      local_interval_stats.write_sizes.push_back(actual_batch_size);
      
      local_cumulative_stats.latencies.push_back(latency_ns);
      local_cumulative_stats.write_sizes.push_back(actual_batch_size);
      
      // Flush statistics periodically to reduce mutex contention
      // Batch multiple updates together to amortize mutex cost
      if (local_interval_stats.latencies.size() >= STATS_BATCH_SIZE) {
        // Flush interval stats - all measurements in one batch
        for (size_t i = 0; i < local_interval_stats.latencies.size(); ++i) {
          interval_stats->RecordLatencyWithSize(
              local_interval_stats.latencies[i],
              local_interval_stats.write_sizes[i]);
        }
        local_interval_stats.latencies.clear();
        local_interval_stats.write_sizes.clear();
      }
      
      if (local_cumulative_stats.latencies.size() >= STATS_BATCH_SIZE) {
        // Flush cumulative stats - all measurements in one batch
        for (size_t i = 0; i < local_cumulative_stats.latencies.size(); ++i) {
          cumulative_stats->RecordLatencyWithSize(
              local_cumulative_stats.latencies[i],
              local_cumulative_stats.write_sizes[i]);
        }
        local_cumulative_stats.latencies.clear();
        local_cumulative_stats.write_sizes.clear();
      }
      
      batch_count++;
    } else {
      LOG(ERROR) << "Write failed: " << s.ToString();
      // Retry logic for transient failures
      int retries = 0;
      while (!s.ok() && retries < 3) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10 * (retries + 1)));
        start = std::chrono::steady_clock::now();
        s = db->Write(write_options, &batch);
        end = std::chrono::steady_clock::now();
        if (s.ok()) {
          auto latency_ns =
              std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
                  .count();
          // Store locally
          local_interval_stats.latencies.push_back(latency_ns);
          local_interval_stats.write_sizes.push_back(actual_batch_size);
          local_cumulative_stats.latencies.push_back(latency_ns);
          local_cumulative_stats.write_sizes.push_back(actual_batch_size);
          batch_count++;
          break;
        }
        retries++;
      }
      if (!s.ok()) {
        LOG(ERROR) << "Write failed after " << retries << " retries: " << s.ToString();
      }
    }
  }
  
  // Flush any remaining local statistics
  if (!local_interval_stats.latencies.empty()) {
    for (size_t i = 0; i < local_interval_stats.latencies.size(); ++i) {
      interval_stats->RecordLatencyWithSize(
          local_interval_stats.latencies[i],
          local_interval_stats.write_sizes[i]);
    }
  }
  if (!local_cumulative_stats.latencies.empty()) {
    for (size_t i = 0; i < local_cumulative_stats.latencies.size(); ++i) {
      cumulative_stats->RecordLatencyWithSize(
          local_cumulative_stats.latencies[i],
          local_cumulative_stats.write_sizes[i]);
    }
  }
}

static std::string toLower(const std::string &str) {
  std::string lowerStr = str;
  std::transform(lowerStr.begin(), lowerStr.end(), lowerStr.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  return lowerStr;
}

int main(int argc, char **argv) {
  // Initialize AWS SDK if using S3
  Aws::SDKOptions aws_options;
  aws_options.httpOptions.installSigPipeHandler = true;
  aws_options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;
  Aws::InitAPI(aws_options);

  // Initialize gflags and glog
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();

  // Register signal handler for Ctrl+C
  std::signal(SIGINT, SignalHandler);
  g_stop_requested.store(false);

  LOG(INFO) << "Starting RocksDB-Cloud batch write test";

  // Parse INI configuration
  auto config = IniParser::Parse(FLAGS_config_file);

  // Extract test configuration
  int thread_count = 4;
  double throughput_limit = 0;     // 0 means no limit
  int report_interval_seconds = FLAGS_report_interval_seconds; // 0 means only report at the end
  int test_duration_seconds = FLAGS_test_duration_seconds; // Default from command line
  int value_size_bytes = FLAGS_value_size_bytes; // Default from command line
  std::string batch_sizes_str = FLAGS_batch_sizes; // Default from command line
  int64_t block_cache_size = FLAGS_block_cache_size; // Default from command line

  if (config.find("TestConfig") != config.end()) {
    auto &test_config = config["TestConfig"];
    if (test_config.find("thread_count") != test_config.end()) {
      thread_count = std::stoi(test_config["thread_count"]);
    }
    if (test_config.find("throughput_limit") != test_config.end()) {
      throughput_limit = std::stod(test_config["throughput_limit"]);
    }
    if (test_config.find("report_interval") != test_config.end()) {
      report_interval_seconds = std::stoi(test_config["report_interval"]);
    }
    if (test_config.find("test_duration_seconds") != test_config.end()) {
      test_duration_seconds = std::stoi(test_config["test_duration_seconds"]);
    }
    if (test_config.find("value_size_bytes") != test_config.end()) {
      value_size_bytes = std::stoi(test_config["value_size_bytes"]);
    }
    if (test_config.find("batch_sizes") != test_config.end()) {
      batch_sizes_str = test_config["batch_sizes"];
    }
  }

  // Also check DBOptions section for block_cache_size
  if (config.find("DBOptions") != config.end()) {
    auto &db_config = config["DBOptions"];
    if (db_config.find("block_cache_size") != db_config.end()) {
      try {
        block_cache_size = std::stoll(db_config["block_cache_size"]);
      } catch (const std::exception &e) {
        LOG(WARNING) << "Invalid block_cache_size value in DBOptions, using 0 (no cache): "
                     << db_config["block_cache_size"];
        block_cache_size = 0;
      }
    }
  }

  // Command-line flags override INI config values if they differ from defaults
  // This allows INI config to be used unless user explicitly sets command-line flags
  if (FLAGS_test_duration_seconds != 120) {  // Default is 120
    test_duration_seconds = FLAGS_test_duration_seconds;
  }
  if (FLAGS_value_size_bytes != 60) {  // Default is 60
    value_size_bytes = FLAGS_value_size_bytes;
  }
  if (FLAGS_batch_sizes != "10000,50000,100000") {  // Default batch sizes
    batch_sizes_str = FLAGS_batch_sizes;
  }
  if (FLAGS_block_cache_size != 0) {  // Default is 0 (no cache)
    block_cache_size = FLAGS_block_cache_size;
  }

  // Parse batch sizes
  std::vector<int> batch_sizes = ParseBatchSizes(batch_sizes_str);
  if (batch_sizes.empty()) {
    LOG(ERROR) << "No valid batch sizes provided";
    return 1;
  }

  LOG(INFO) << "Batch sizes (in bytes): ";
  for (int size : batch_sizes) {
    LOG(INFO) << "  - " << size << " bytes";
  }

  LOG(INFO) << "Thread count: " << thread_count;
  LOG(INFO) << "Throughput limit: " << throughput_limit << " ops/sec";
  LOG(INFO) << "Report interval: " << report_interval_seconds
            << " seconds (0 = end only)";
  LOG(INFO) << "Test duration: " << test_duration_seconds
            << " seconds (0 = run indefinitely until Ctrl+C)";
  LOG(INFO) << "Value size: " << value_size_bytes << " bytes";
  const size_t estimated_key_size = 50;
  LOG(INFO) << "Estimated total row size (key ~" << estimated_key_size 
            << " bytes + value " << value_size_bytes << " bytes): " 
            << (estimated_key_size + value_size_bytes) << " bytes";
  LOG(INFO) << "Block cache size: " << block_cache_size 
            << " bytes (" << (block_cache_size == 0 ? "disabled" : "enabled") << ")";

  // Create database directory if it doesn't exist
  if (!CreateDirectoryIfNotExists(FLAGS_db_path)) {
    LOG(ERROR) << "Failed to create database directory: " << FLAGS_db_path;
    return 1;
  }

  // Load DB options from INI file
  Options options;
  ConfigOptions config_options;
  config_options.ignore_unknown_options = false;
  config_options.input_strings_escaped = true;
  config_options.sanity_level = ConfigOptions::kSanityLevelLooselyCompatible;

  // Try to load options from existing database
  Status s = LoadLatestOptions(config_options, FLAGS_db_path, &options, nullptr);
  if (!s.ok()) {
    LOG(INFO) << "Cannot load options from DB, using defaults: "
              << s.ToString();
    options.create_if_missing = true;
  }

  // Enable statistics collection
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();

  // Set stats dump period if configured
  options.stats_dump_period_sec = report_interval_seconds * 2;
  LOG(INFO) << "DB Statistics dump period: " <<  report_interval_seconds * 2 << " seconds";

  // Apply INI configuration to DB options using RocksDB's built-in option parser
  // This automatically handles all options without manual parsing
  if (config.find("DBOptions") != config.end()) {
    auto &db_config = config["DBOptions"];
    
    // Build option string in format: "key1=value1;key2=value2;..."
    // Skip block_cache_size as it's handled separately
    std::string options_str;
    for (const auto &pair : db_config) {
      if (pair.first == "block_cache_size") {
        continue;  // Skip, handled separately
      }
      if (!options_str.empty()) {
        options_str += ";";
      }
      options_str += pair.first + "=" + pair.second;
    }
    
    if (!options_str.empty()) {
      s = GetOptionsFromString(config_options, options, options_str, &options);
      if (!s.ok()) {
        LOG(WARNING) << "Failed to parse some DBOptions from INI: " << s.ToString();
      } else {
        LOG(INFO) << "Applied DBOptions from INI config: " << options_str;
      }
    }
  }

  // Configure block cache if specified
  std::shared_ptr<ROCKSDB_NAMESPACE::Cache> block_cache;
  if (block_cache_size > 0) {
    // Create block cache with default number of shard bits (6)
    // This provides good concurrency for multi-threaded access
    const int block_cache_num_shard_bits = 6;
    block_cache = NewLRUCache(block_cache_size, block_cache_num_shard_bits);
    
    // Configure BlockBasedTableOptions to use the block cache
    BlockBasedTableOptions table_options;
    table_options.block_cache = block_cache;
    table_options.cache_index_and_filter_blocks = true;
    
    // Set the table factory with block cache configuration
    // Note: This will replace any existing table factory, but block cache
    // configuration takes precedence when explicitly set
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    
    LOG(INFO) << "Block cache configured: " << block_cache_size 
              << " bytes with " << block_cache_num_shard_bits << " shard bits"
              << ", cache_index_and_filter_blocks=true";
  } else {
    LOG(INFO) << "Block cache disabled (size = 0)";
  }

  // Setup RocksDB-Cloud filesystem options
  ROCKSDB_NAMESPACE::CloudFileSystemOptions cloud_fs_options;
  bool use_cloud = false;

  auto &cloud_config = config["CloudOptions"];

  // Get cloud configuration parameters
  std::string bucket_name;
  std::string object_path;
  std::string aws_access_key_id;
  std::string aws_secret_access_key;
  std::string aws_region = "us-east-1";
  std::string cloud_endpoint;
  std::string bucket_prefix;

  if (cloud_config.find("s3_bucket") != cloud_config.end()) {
    bucket_name = cloud_config["s3_bucket"];
  }
  if (cloud_config.find("object_path") != cloud_config.end()) {
    object_path = cloud_config["object_path"];
  } else {
    object_path = FLAGS_db_path;
  }
  if (cloud_config.find("aws_access_key_id") != cloud_config.end()) {
    aws_access_key_id = cloud_config["aws_access_key_id"];
  }
  if (cloud_config.find("aws_secret_access_key") != cloud_config.end()) {
    aws_secret_access_key = cloud_config["aws_secret_access_key"];
  }
  if (cloud_config.find("aws_region") != cloud_config.end()) {
    aws_region = cloud_config["aws_region"];
  }
  if (cloud_config.find("cloud_endpoint") != cloud_config.end()) {
    cloud_endpoint = cloud_config["cloud_endpoint"];
  }
  if (cloud_config.find("bucket_prefix") != cloud_config.end()) {
    bucket_prefix = cloud_config["bucket_prefix"];
  }

  // Parse use_aws_transfer_manager option
  bool use_aws_transfer_manager = false;
  if (cloud_config.find("use_aws_transfer_manager") != cloud_config.end()) {
    std::string value = cloud_config["use_aws_transfer_manager"];
    std::transform(value.begin(), value.end(), value.begin(), ::tolower);
    use_aws_transfer_manager =
        (value == "true" || value == "1" || value == "yes");
  }

  // Parse sst_file_cache_size option
  size_t sst_file_cache_size = 0;
  if (cloud_config.find("sst_file_cache_size") != cloud_config.end()) {
    try {
      sst_file_cache_size = std::stoull(cloud_config["sst_file_cache_size"]);
    } catch (const std::exception &e) {
      LOG(WARNING) << "Invalid sst_file_cache_size value, using 0 (no cache): "
                   << cloud_config["sst_file_cache_size"];
      sst_file_cache_size = 0;
    }
  }

  // Parse sst_file_cache_num_shard_bits option (default: 5)
  int sst_file_cache_num_shard_bits = 5;
  if (cloud_config.find("sst_file_cache_num_shard_bits") !=
      cloud_config.end()) {
    try {
      sst_file_cache_num_shard_bits =
          std::stoi(cloud_config["sst_file_cache_num_shard_bits"]);
    } catch (const std::exception &e) {
      LOG(WARNING)
          << "Invalid sst_file_cache_num_shard_bits value, using default 5: "
          << cloud_config["sst_file_cache_num_shard_bits"];
      sst_file_cache_num_shard_bits = 5;
    }
  }

  if (bucket_name.empty()) {
    LOG(ERROR) << "s3_bucket must be specified in CloudOptions";
    return 1;
  }

  LOG(INFO) << "Configuring RocksDB-Cloud with S3 bucket: " << bucket_name;
  LOG(INFO) << "  Object path: " << object_path;
  LOG(INFO) << "  Region: " << aws_region;
  LOG(INFO) << "  Cloud endpoint: "
            << (cloud_endpoint.empty() ? "(default)" : cloud_endpoint);
  LOG(INFO) << "  Use AWS Transfer Manager: "
            << (use_aws_transfer_manager ? "true" : "false");
  LOG(INFO) << "  SST file cache size: " << sst_file_cache_size << " bytes";
  LOG(INFO) << "  SST file cache num shard bits: "
            << sst_file_cache_num_shard_bits;

  // Create cloud filesystem options
  cloud_fs_options.src_bucket.SetBucketName(bucket_name);
  cloud_fs_options.src_bucket.SetObjectPath(object_path);

  // Set AWS Transfer Manager option
  cloud_fs_options.use_aws_transfer_manager = use_aws_transfer_manager;

  if (!bucket_prefix.empty()) {
    cloud_fs_options.src_bucket.SetBucketPrefix(bucket_prefix);
  } else {
    cloud_fs_options.src_bucket.SetBucketPrefix("");
  }

  // Set region for src_bucket
  cloud_fs_options.src_bucket.SetRegion(aws_region);

  cloud_fs_options.dest_bucket = cloud_fs_options.src_bucket;

  // Set region for dest_bucket (explicitly set to ensure it's configured)
  cloud_fs_options.dest_bucket.SetRegion(aws_region);

  cloud_fs_options.resync_on_open = true;

  // Set SST file cache option
  if (sst_file_cache_size > 0) {
    cloud_fs_options.sst_file_cache =
        NewLRUCache(sst_file_cache_size, sst_file_cache_num_shard_bits);
  } else {
    cloud_fs_options.sst_file_cache = nullptr;
  }

  // Set up S3 client factory if using S3
  if (!cloud_endpoint.empty()) {
    // Build S3 client factory similar to reference implementation
    auto s3_client_factory =
        [cloud_endpoint](
            const std::shared_ptr<Aws::Auth::AWSCredentialsProvider>
                &credentialsProvider,
            const Aws::Client::ClientConfiguration &baseConfig)
        -> std::shared_ptr<Aws::S3::S3Client> {
      if (cloud_endpoint.empty()) {
        return nullptr;
      }

      std::string endpoint_url = toLower(cloud_endpoint);

      bool secured_url = false;
      if (endpoint_url.rfind("http://", 0) == 0) {
        secured_url = false;
      } else if (endpoint_url.rfind("https://", 0) == 0) {
        secured_url = true;
      } else {
        LOG(ERROR) << "Invalid S3 endpoint url";
        std::abort();
      }

      // Create a new configuration based on the base config
      Aws::Client::ClientConfiguration config = baseConfig;
      config.endpointOverride = endpoint_url;
      if (secured_url) {
        config.scheme = Aws::Http::Scheme::HTTPS;
      } else {
        config.scheme = Aws::Http::Scheme::HTTP;
      }

      // Create and return the S3 client
      if (credentialsProvider) {
        return std::make_shared<Aws::S3::S3Client>(
            credentialsProvider, config,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            true /* useVirtualAddressing */);
      } else {
        return std::make_shared<Aws::S3::S3Client>(config);
      }
    };
    cloud_fs_options.s3_client_factory = s3_client_factory;
  }

  if (aws_access_key_id.empty() || aws_secret_access_key.empty()) {
    LOG(INFO) << "No AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY "
                 "provided, use default credential provider";
    cloud_fs_options.credentials.type = rocksdb::AwsAccessType::kUndefined;
  } else {
    cloud_fs_options.credentials.InitializeSimple(aws_access_key_id,
                                                  aws_secret_access_key);
  }

  s = cloud_fs_options.credentials.HasValid();
  if (!s.ok()) {
    LOG(ERROR) << "Valid AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY "
                  "is required, error: "
               << s.ToString();
    return 1;
  }

  rocksdb::CloudFileSystem *cfs;
  s = rocksdb::CloudFileSystemEnv::NewAwsFileSystem(
      rocksdb::FileSystem::Default(), cloud_fs_options, nullptr, &cfs);

  if (!s.ok()) {
    LOG(ERROR) << "Failed to create CloudFileSystem: " << s.ToString();
    return 1;
  }

  LOG(INFO) << "RocksDB-Cloud filesystem options configured successfully";

  std::shared_ptr<rocksdb::FileSystem> cloud_fs{nullptr};
  std::unique_ptr<rocksdb::Env> cloud_env{nullptr};

  cloud_fs.reset(cfs);
  cloud_env = rocksdb::NewCompositeEnv(cloud_fs);
  options.env = cloud_env.get();

  // Open database (using default column family only)
  DBCloud *db;

  // Use DBCloud::Open for cloud database with default column family only
  // Pass empty vector to use default column family
  s = DBCloud::Open(options, FLAGS_db_path, "", 0, &db);
  if (!s.ok()) {
    LOG(ERROR) << "Failed to open database: " << s.ToString();
    return 1;
  }

  LOG(INFO) << "Database opened successfully";

  // Track overall test start time
  auto overall_test_start = std::chrono::steady_clock::now();

  // Create statistics collectors: one for intervals, one for cumulative
  TimeBasedStatistics interval_stats;
  TimeBasedStatistics cumulative_stats;

  // Global stop flag (shared by all threads)
  std::atomic<bool> global_stop_flag(false);

  // Start interval reporting thread
  std::thread report_thread;
  auto last_report_time = overall_test_start;
  if (report_interval_seconds > 0) {
    report_thread =
        std::thread([&interval_stats, &last_report_time, &global_stop_flag,
                     report_interval_seconds]() {
          while (!global_stop_flag.load() && !g_stop_requested.load()) {
            std::this_thread::sleep_for(
                std::chrono::seconds(report_interval_seconds));
            if (!global_stop_flag.load() && !g_stop_requested.load()) {
              // Get stats for the interval and reset
              auto interval_start = last_report_time;
              auto now = std::chrono::steady_clock::now();
              auto interval_elapsed =
                  std::chrono::duration_cast<std::chrono::seconds>(
                      now - interval_start)
                      .count();

              // Get stats for this interval and reset the interval stats
              auto interval_stats_result = interval_stats.GetStatsAndReset();
              last_report_time = now;

              PrintIntervalStats(interval_stats_result, interval_elapsed);
            }
          }
        });
  }

  // Create rate limiter (shared across threads)
  RateLimiter rate_limiter(throughput_limit);

  // Create thread pool - workers will cycle through batch sizes internally
  std::vector<std::thread> threads;

  // Start worker threads (they will cycle through batch sizes internally)
  for (int i = 0; i < thread_count; ++i) {
    threads.emplace_back(BatchWriteWorker, db, batch_sizes, -1, &rate_limiter,
                         &interval_stats, &cumulative_stats, &global_stop_flag,
                         value_size_bytes);
  }

  // Run until test duration is reached or interrupted
  while (true) {
    // Check if overall test duration is reached
    if (test_duration_seconds > 0) {
      auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
          std::chrono::steady_clock::now() - overall_test_start).count();
      if (elapsed >= test_duration_seconds) {
        LOG(INFO) << "Test duration (" << test_duration_seconds 
                  << " seconds) reached. Stopping test.";
        break;
      }
    }
    
    // Check if interrupted by Ctrl+C
    if (g_stop_requested.load()) {
      LOG(INFO) << "Stopping test due to user interrupt (Ctrl+C)";
      break;
    }
    
    // Sleep briefly and check again
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  // Stop all threads (workers and reporter)
  global_stop_flag.store(true);
  for (auto &t : threads) {
    t.join();
  }
  if (report_thread.joinable()) {
    report_thread.join();
  }

  // Print final cumulative statistics
  auto overall_test_end = std::chrono::steady_clock::now();
  auto overall_test_duration =
      std::chrono::duration_cast<std::chrono::seconds>(
          overall_test_end - overall_test_start).count();

  LOG(INFO) << "\n=== Final Cumulative Statistics ===";
  auto cumulative_stats_result = cumulative_stats.GetStats();
  PrintCumulativeStats(cumulative_stats_result, overall_test_duration);

  // Cleanup
  db->Close();
  delete db;

  // Cleanup cloud filesystem (will be destroyed when shared_ptr goes out of
  // scope)
  cloud_fs.reset();
  cloud_env.reset();

  // Shutdown AWS SDK if initialized
  Aws::ShutdownAPI(aws_options);

  LOG(INFO) << "Test completed";
  return 0;
}
