# RocksDB-Cloud Batch Write Test

A performance testing tool for RocksDB-Cloud that measures batch write latency and throughput with configurable batch sizes, parallelism, and rate limiting.

## Features

- **Multiple Batch Sizes**: Test different batch sizes in a single run
- **Parallel Execution**: Configurable thread pool for concurrent batch writes
- **Throughput Control**: Rate limiting to control write rate
- **Comprehensive Statistics**: Detailed latency metrics (p50, p95, p99, min, max, avg) and throughput
- **Flexible Configuration**: All options loaded from INI file

## Prerequisites

- CMake 3.15 or later
- C++17 compatible compiler
- RocksDB library
- RocksDB-Cloud library
- gflags library
- glog library

## Building

```bash
mkdir build
cd build
cmake ..
make
```

The executable will be created at `build/bin/batch_write_test`.

## Usage

### Basic Usage

```bash
./batch_write_test \
  --batch_sizes=100,500,1000 \
  --db_path=/tmp/rocksdb_test \
  --config_file=../config/db_options.ini
```

### Command Line Arguments

- `--batch_sizes`: Comma-separated list of batch sizes to test (e.g., "100,500,1000")
- `--db_path`: Path to the RocksDB database directory
- `--config_file`: Path to the INI configuration file (default: `config/db_options.ini`)

### Configuration File

The configuration file uses standard INI format with the following sections:

#### [DBOptions]
General RocksDB database options:
- `create_if_missing`: Create database if it doesn't exist (true/false)
- `max_background_jobs`: Maximum number of background threads
- `write_buffer_size`: Write buffer size in bytes
- `max_write_buffer_number`: Maximum number of write buffers
- `min_write_buffer_number_to_merge`: Minimum number of write buffers to merge

#### [ColumnFamilyOptions]
Column family specific options:
- `compression`: Compression type (none, snappy, zlib, bzip2, lz4, lz4hc, xz, zstd)
- `block_size`: Block size for data storage in bytes
- `bloom_locality`: Enable bloom filter locality (0/1)

#### [CloudOptions]
RocksDB-Cloud specific options:
- `s3_bucket`: S3 bucket name (if using S3)
- `aws_access_key_id`: AWS access key ID
- `aws_secret_access_key`: AWS secret access key
- `aws_region`: AWS region
- `cloud_endpoint`: Cloud storage endpoint URL

#### [TestConfig]
Test execution parameters:
- `thread_count`: Number of threads for parallel batch writes
- `throughput_limit`: Throughput limit in operations per second (0 means no limit)
- `report_interval`: Report interval in seconds for periodic statistics output (0 means only report at the end)

### Example Configuration

See `config/db_options.ini` for a complete example configuration file.

## Output

The tool outputs statistics to stdout for each batch size tested. If `report_interval` is configured (greater than 0), intermediate statistics will be printed periodically during the test run. Final statistics are always printed at the end of each batch size test.

### Intermediate Statistics (when report_interval > 0)

Periodically during the test run, intermediate statistics are printed:

```
=== Intermediate Stats for batch size 100 (elapsed: 10s) ===
Total batches: 500
Latency statistics (nanoseconds):
  Min:    1234567
  Max:    9876543
  Avg:    2345678
  P50:    2000000
  P95:    5000000
  P99:    8000000
Throughput: 50.0 batches/sec
Throughput: 5000.0 writes/sec
```

### Final Statistics

At the end of each batch size test, final statistics are printed:

```
=== Results for batch size 100 ===
Total batches: 1500
Test duration: 30 seconds
Latency statistics (nanoseconds):
  Min:    1234567
  Max:    9876543
  Avg:    2345678
  P50:    2000000
  P95:    5000000
  P99:    8000000
Throughput: 50.0 batches/sec
Throughput: 5000.0 writes/sec
```

## How It Works

1. The tool parses command line arguments and loads configuration from the INI file
2. It opens or creates the RocksDB database using options from the INI file
3. For each batch size specified:
   - Creates a thread pool with the configured number of threads
   - Each thread performs batch writes with rate limiting
   - Latency statistics are collected for each batch write operation
4. Statistics are calculated and printed to stdout

## Notes

- The tool uses RocksDB's `LoadLatestOptions` to load database options from the database directory if available
- INI file options override or supplement the loaded options
- Rate limiting is applied per batch write operation
- Statistics are thread-safe and collected across all worker threads

## License

This project is provided as-is for testing purposes.

