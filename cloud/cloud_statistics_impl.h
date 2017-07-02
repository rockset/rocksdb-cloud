//  Copyright (c) 2017-present, Cloudset, Inc.  All rights reserved.
#pragma once

#include "rocksdb/cloud/cloud_statistics.h"
#include "monitoring/statistics.h"

namespace rocksdb {

class CloudStatisticsImpl : public CloudStatistics {
 public:
  CloudStatisticsImpl(std::shared_ptr<Statistics> stats,
                 bool enable_internal_stats);
  virtual ~CloudStatisticsImpl();

  // Ticker access and manipulation
  virtual uint64_t getTickerCount(uint32_t ticker_type) const override;
  virtual uint64_t getAndResetTickerCount(uint32_t ticker_type) override;
  virtual void recordTick(uint32_t ticker_type, uint64_t count) override;
  virtual void setTickerCount(uint32_t ticker_type, uint64_t count) override;

  // Histogram access and manipulation
  virtual bool HistEnabledForType(uint32_t type) const override;
  std::string getHistogramString(uint32_t histogram_type) const override;
  virtual void histogramData(uint32_t histogram_type,
                             HistogramData* const data) const override;
  virtual void measureTime(uint32_t histogram_type, uint64_t value) override;

  virtual Status Reset() override;
  virtual std::string ToString() const override;

private:
  StatisticsImpl stats_impl_;

  // Synchronizes anything that operates on other threads' thread-specific data
  // such that operations like Reset() can be performed atomically.
  mutable port::Mutex lock_;

  // Holds data maintained by each thread for implementing tickers.
  struct ThreadTickerInfo {
    std::atomic_uint_fast64_t value;

    // During teardown, value will be summed into *merged_sum.
    std::atomic_uint_fast64_t* merged_sum;

    ThreadTickerInfo(uint_fast64_t _value,
                     std::atomic_uint_fast64_t* _merged_sum)
        : value(_value), merged_sum(_merged_sum) {}
  };

  // Returns the info for this tickerType/thread. It sets a new info with zeroed
  // counter if none exists.
  ThreadTickerInfo* getThreadTickerInfo(uint32_t tickerIndex);

  // Holds data maintained by each thread for implementing histograms.
  struct ThreadHistogramInfo {
    HistogramImpl value;

    // During teardown, value will be merged into *merged_hist while holding
    // *merge_lock, which also syncs with the merges necessary for reads.
    HistogramImpl* merged_hist;
    port::Mutex* merge_lock;

    ThreadHistogramInfo(HistogramImpl* _merged_hist, port::Mutex* _merge_lock)
        : value(), merged_hist(_merged_hist), merge_lock(_merge_lock) {}
  };

  // Returns the info for this histogramType/thread. It sets a new histogram
  // with zeroed data if none exists.
  ThreadHistogramInfo* getThreadHistogramInfo(uint32_t tickerIndex);

  // Holds global data for implementing tickers.
  struct TickerInfo {
    TickerInfo() : thread_value(new ThreadLocalPtr(&mergeThreadValue)) {}

    // Sum of thread-specific values for tickers that have been reset due to
    // thread termination or ThreadLocalPtr destruction. Also, this is used by
    // setTickerCount() to conveniently change the global value by setting this
    // while simultaneously zeroing all thread-local values.
    std::atomic_uint_fast64_t merged_sum{0};

    // Holds thread-specific pointer to ThreadTickerInfo
    std::unique_ptr<ThreadLocalPtr> thread_value;

    static void mergeThreadValue(void* ptr) {
      auto info_ptr = static_cast<ThreadTickerInfo*>(ptr);
      *info_ptr->merged_sum += info_ptr->value;
      delete info_ptr;
    }
  };

  // Holds global data for implementing histograms.
  struct HistogramInfo {
    HistogramInfo() : thread_value(new ThreadLocalPtr(&mergeThreadValue)) {}

    // Merged thread-specific values for histograms that have been reset due to
    // thread termination or ThreadLocalPtr destruction. Note these must be
    // destroyed after thread_value since its destructor accesses them.
    mutable port::Mutex merge_lock{};
    HistogramImpl merged_hist{};

    // Holds thread-specific pointer to ThreadHistogramInfo
    std::unique_ptr<ThreadLocalPtr> thread_value;

    static void mergeThreadValue(void* ptr) {
      auto info_ptr = static_cast<ThreadHistogramInfo*>(ptr);
      {
        MutexLock lock(info_ptr->merge_lock);
        info_ptr->merged_hist->Merge(info_ptr->value);
      }
      delete info_ptr;
    }

    // Returns a histogram that merges all histograms (thread-specific and
    // previously merged ones).
    std::unique_ptr<HistogramImpl> getMergedHistogram() const;
  };

  TickerInfo tickers_[CLOUD_TICKER_ENUM_MAX - CLOUD_TICKER_ENUM_START - 1];
  HistogramInfo histograms_[CLOUD_HISTOGRAM_ENUM_MAX - CLOUD_HISTOGRAM_ENUM_START - 1];
};

}




