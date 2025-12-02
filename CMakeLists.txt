cmake_minimum_required(VERSION 3.15)
project(rocksdb-cloud-batch-write-test)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find required packages
find_package(PkgConfig REQUIRED)

# Find gflags
find_path(GFLAGS_INCLUDE_DIR gflags/gflags.h)
find_library(GFLAGS_LIBRARY gflags)

# Find glog
find_path(GLOG_INCLUDE_DIR glog/logging.h)
find_library(GLOG_LIBRARY glog)

# Find RocksDB-Cloud (assuming it's installed similarly)
find_path(ROCKSDB_CLOUD_INCLUDE_DIR rocksdb/db.h PATH_SUFFIXES "rocksdb_cloud_header")
find_library(ROCKSDB_CLOUD_LIBRARY rocksdb-cloud-aws)

# Find AWS SDK Core
find_path(AWS_CORE_INCLUDE_PATH aws/core/Aws.h)
if(NOT AWS_CORE_INCLUDE_PATH)
  message(FATAL_ERROR "Fail to find aws/core include path")
endif()
message(STATUS "aws/core include path: ${AWS_CORE_INCLUDE_PATH}")

find_library(AWS_CORE_LIB aws-cpp-sdk-core)
if(NOT AWS_CORE_LIB)
  message(FATAL_ERROR "Fail to find aws-cpp-sdk-core lib")
endif()
message(STATUS "aws-cpp-sdk-core library: ${AWS_CORE_LIB}")

# Find AWS SDK S3
find_path(AWS_S3_INCLUDE_PATH aws/s3/S3Client.h)
if(NOT AWS_S3_INCLUDE_PATH)
  message(FATAL_ERROR "Fail to find aws/s3 include path")
endif()
message(STATUS "aws/s3 include path: ${AWS_S3_INCLUDE_PATH}")

find_library(AWS_S3_LIB aws-cpp-sdk-s3)
if(NOT AWS_S3_LIB)
  message(FATAL_ERROR "Fail to find aws-cpp-sdk-s3 lib")
endif()
message(STATUS "aws-cpp-sdk-s3 library: ${AWS_S3_LIB}")

# Include directories
include_directories(${GFLAGS_INCLUDE_DIR})
include_directories(${GLOG_INCLUDE_DIR})
include_directories(${ROCKSDB_CLOUD_INCLUDE_DIR})
include_directories(${AWS_CORE_INCLUDE_PATH})
include_directories(${AWS_S3_INCLUDE_PATH})

# Source files
set(SOURCES
    src/batch_write_test.cc
)

# Create executable
add_executable(batch_write_test ${SOURCES})

# Link libraries
target_link_libraries(batch_write_test
    ${GFLAGS_LIBRARY}
    ${GLOG_LIBRARY}
    ${ROCKSDB_CLOUD_LIBRARY}
    ${AWS_CORE_LIB}
    ${AWS_S3_LIB}
    pthread
)

# Set output directory
set_target_properties(batch_write_test PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)

