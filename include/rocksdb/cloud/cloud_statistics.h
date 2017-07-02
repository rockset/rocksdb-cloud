//  Copyright (c) 2017-present, Rockset, Inc.  All rights reserved.
#pragma once

#include "rocksdb/statistics.h"

namespace rocksdb {

/**
 * Keep adding cloud tickers here.
 *  1. Any ticker should be added between CLOUD_TICKER_ENUM_START and CLOUD_TICKER_ENUM_MAX.
 *  2. Add a readable string in CloudTickersNameMap below for the newly added ticker.
 */
enum CloudTickers : uint32_t {
  // # of times the manifest is written to the cloud
  CLOUD_TICKER_ENUM_START = 1u << 31,
  NUMBER_MANIFEST_WRITES,
  CLOUD_TICKER_ENUM_MAX
};

const std::vector<std::pair<CloudTickers, std::string>> CloudTickersNameMap = {
  {NUMBER_MANIFEST_WRITES, "rocksdb.cloud.number.manifest.writes"}
};


/**
 * Keep adding cloud histograms here.
 * 1. Any histogram should be added between CLOUD_HISTOGRAM_ENUM_START and CLOUD_HISTOGRAM_ENUM_MAX.
 * 2. Add a readable string in CloudHistogramsNameMap below for the newly added histogram.
 */
enum CloudHistograms : uint32_t {
  // histogram of # of milliseconds elapsed during manifest writes
  CLOUD_HISTOGRAM_ENUM_START = 1u << 31,
  MANIFEST_WRITES_TIME,
  CLOUD_HISTOGRAM_ENUM_MAX
};

const std::vector<std::pair<CloudHistograms, std::string>> CloudHistogramsNameMap = {
  {MANIFEST_WRITES_TIME, "rocksdb.cloud.manifest.writes.millis"}
};

class CloudStatistics : public Statistics {
};

// Create a concrete CloudStatistics object
std::shared_ptr<CloudStatistics> CreateCloudStatistics();

}
