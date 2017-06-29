//  Copyright (c) 2017-present, Rockset, Inc.  All rights reserved.
#include "cloud/cloud_statistics_impl.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>

namespace rocksdb {

std::shared_ptr<CloudStatistics> CreateCloudStatistics() {
  return std::make_shared<CloudStatisticsImpl>(nullptr, false);
}

CloudStatisticsImpl::CloudStatisticsImpl(
    std::shared_ptr<Statistics> stats,
    bool enable_internal_stats)
    : stats_impl_(stats, enable_internal_stats) {
}

CloudStatisticsImpl::~CloudStatisticsImpl() {}

uint64_t CloudStatisticsImpl::getTickerCount(uint32_t tickerType) const {
  uint64_t sum = 0;
  if (tickerType & CLOUD_TICKER_ENUM_START) {
    {
        MutexLock lock(&lock_);
        assert(tickerType < CLOUD_TICKER_ENUM_MAX);
        int tickerIndex = tickerType ^ CLOUD_TICKER_ENUM_START;
        tickers_[tickerIndex].thread_value->Fold(
            [](void* curr_ptr, void* res) {
              auto* sum_ptr = static_cast<uint64_t*>(res);
              *sum_ptr += static_cast<std::atomic_uint_fast64_t*>(curr_ptr)->load(
                  std::memory_order_relaxed);
            },
            &sum);
        sum += tickers_[tickerIndex].merged_sum.load(std::memory_order_relaxed);
    }
    return sum;
  } else {
    sum += stats_impl_.getTickerCount(tickerType);
  }
  return sum;
}

uint64_t CloudStatisticsImpl::getAndResetTickerCount(uint32_t tickerType) {
  uint64_t sum = 0;
  if (tickerType & CLOUD_TICKER_ENUM_START) {
    {
      MutexLock lock(&lock_);
      assert(tickerType < CLOUD_TICKER_ENUM_MAX);
      int tickerIndex = tickerType ^ CLOUD_TICKER_ENUM_START;
      tickers_[tickerIndex].thread_value->Fold(
          [](void* curr_ptr, void* res) {
            auto* sum_ptr = static_cast<uint64_t*>(res);
            *sum_ptr += static_cast<std::atomic<uint64_t>*>(curr_ptr)->exchange(
                0, std::memory_order_relaxed);
          },
          &sum);
      sum += tickers_[tickerIndex].merged_sum.exchange(
          0, std::memory_order_relaxed);
    }
  } else {
    sum += stats_impl_.getAndResetTickerCount(tickerType);
  }
  return sum;
}

void CloudStatisticsImpl::recordTick(uint32_t tickerType, uint64_t count) {
  if (tickerType & CLOUD_TICKER_ENUM_START) {
    assert(tickerType < CLOUD_TICKER_ENUM_MAX);
    int tickerIndex = tickerType ^ CLOUD_TICKER_ENUM_START;
    auto info = getThreadTickerInfo(tickerIndex);
    info->value.fetch_add(count, std::memory_order_relaxed);
  } else {
    stats_impl_.recordTick(tickerType, count);
  }
}

void CloudStatisticsImpl::setTickerCount(uint32_t tickerType, uint64_t count) {
  if (tickerType & CLOUD_TICKER_ENUM_START) {
    {
      MutexLock lock(&lock_);
      assert(tickerType < CLOUD_TICKER_ENUM_MAX);
      int tickerIndex = tickerType ^ CLOUD_TICKER_ENUM_START;
      tickers_[tickerIndex].thread_value->Fold(
          [](void* curr_ptr, void* res) {
            static_cast<std::atomic<uint64_t>*>(curr_ptr)->store(
                0, std::memory_order_relaxed);
          },
          nullptr);
      tickers_[tickerIndex].merged_sum.store(count, std::memory_order_relaxed);
    }
  } else {
    stats_impl_.setTickerCount(tickerType, count);
  }
}

bool CloudStatisticsImpl::HistEnabledForType(uint32_t histogramType) const {
  if (histogramType & CLOUD_HISTOGRAM_ENUM_START) {
    return histogramType < CLOUD_HISTOGRAM_ENUM_MAX;
  } else {
    return stats_impl_.HistEnabledForType(histogramType);
  }
}

std::string CloudStatisticsImpl::getHistogramString(uint32_t histogramType) const {
  std::string result;
  if (histogramType & CLOUD_HISTOGRAM_ENUM_START) {
    {
      MutexLock lock(&lock_);
      assert(histogramType < CLOUD_HISTOGRAM_ENUM_MAX);
      int histogramIndex = histogramType ^ CLOUD_TICKER_ENUM_START;
      result = std::move(histograms_[histogramIndex].getMergedHistogram()->ToString());
    }
  } else {
    result = std::move(stats_impl_.getHistogramString(histogramType));
  }
  return result;
}

void CloudStatisticsImpl::histogramData(uint32_t histogramType,
                                   HistogramData* const data) const {
  if (histogramType & CLOUD_HISTOGRAM_ENUM_START) {
    {
      MutexLock lock(&lock_);
      assert(histogramType < CLOUD_HISTOGRAM_ENUM_MAX);
      int histogramIndex = histogramType ^ CLOUD_TICKER_ENUM_START;
      histograms_[histogramIndex].getMergedHistogram()->Data(data);
    }
  } else {
    stats_impl_.histogramData(histogramType, data);
  }
}

void CloudStatisticsImpl::measureTime(uint32_t histogramType, uint64_t value) {
  if (histogramType & CLOUD_HISTOGRAM_ENUM_START) {
    assert(histogramType < CLOUD_HISTOGRAM_ENUM_MAX);
    int histogramIndex = histogramType ^ CLOUD_TICKER_ENUM_START;
    getThreadHistogramInfo(histogramIndex)->value.Add(value);
  } else {
    stats_impl_.measureTime(histogramType, value);
  }
}

Status CloudStatisticsImpl::Reset() {
  {
    MutexLock lock(&lock_);
    for (uint32_t i = 0;
        i < CLOUD_TICKER_ENUM_MAX - CLOUD_TICKER_ENUM_START - 1; ++i) {
      tickers_[i].thread_value->Fold(
          [](void* curr_ptr, void* res) {
            static_cast<std::atomic<uint64_t>*>(curr_ptr)->store(
                0, std::memory_order_relaxed);
          },
          nullptr);
      tickers_[i].merged_sum.store(0, std::memory_order_relaxed);
    }
    for (uint32_t i = 0;
        i < CLOUD_HISTOGRAM_ENUM_MAX - CLOUD_HISTOGRAM_ENUM_START - 1; ++i) {
      histograms_[i].thread_value->Fold(
          [](void* curr_ptr, void* res) {
            static_cast<HistogramImpl*>(curr_ptr)->Clear();
          },
          nullptr /* res */);
    }
  }
  return stats_impl_.Reset();
}

std::string CloudStatisticsImpl::ToString() const {
  static constexpr int kTmpStrBufferSize = 200;
  std::string str;
  str.reserve(20000);
  {
    MutexLock lock(&lock_);
    char buffer[kTmpStrBufferSize];
    for (const auto& t : CloudTickersNameMap) {
      if ((t.first & CLOUD_TICKER_ENUM_START) && (t.first < CLOUD_TICKER_ENUM_MAX)) {
        uint64_t tickerIndex = t.first ^ CLOUD_TICKER_ENUM_START;
        uint64_t sum = 0;
        tickers_[tickerIndex].thread_value->Fold(
            [](void* curr_ptr, void* res) {
              auto* sum_ptr = static_cast<uint64_t*>(res);
              *sum_ptr += static_cast<std::atomic_uint_fast64_t*>(curr_ptr)->load(
                  std::memory_order_relaxed);
            },
            &sum);
        sum += tickers_[tickerIndex].merged_sum.load(std::memory_order_relaxed);

        snprintf(buffer, kTmpStrBufferSize, "%s COUNT : %" PRIu64 "\n", t.second.c_str(), sum);
        str.append(buffer);
      }
    }
    for (const auto& h : CloudHistogramsNameMap) {
      if ((h.first & CLOUD_HISTOGRAM_ENUM_START) && (h.first < CLOUD_HISTOGRAM_ENUM_MAX)) {
        uint64_t histogramIndex = h.first ^ CLOUD_HISTOGRAM_ENUM_START;
        HistogramData histogramData;
        histograms_[histogramIndex].getMergedHistogram()->Data(&histogramData);

        snprintf(
            buffer, kTmpStrBufferSize,
            "%s statistics Percentiles :=> 50 : %f 95 : %f 99 : %f 100 : %f\n",
            h.second.c_str(), histogramData.median, histogramData.percentile95,
            histogramData.percentile99, histogramData.max);
        str.append(buffer);
      }
    }
  }
  str.append(stats_impl_.ToString());
  str.shrink_to_fit();
  return str;
}

CloudStatisticsImpl::ThreadTickerInfo* CloudStatisticsImpl::getThreadTickerInfo(
    uint32_t tickerIndex) {
  auto info_ptr =
      static_cast<ThreadTickerInfo*>(tickers_[tickerIndex].thread_value->Get());
  if (info_ptr == nullptr) {
    info_ptr =
        new ThreadTickerInfo(0 /* value */, &tickers_[tickerIndex].merged_sum);
    tickers_[tickerIndex].thread_value->Reset(info_ptr);
  }
  return info_ptr;
}

CloudStatisticsImpl::ThreadHistogramInfo* CloudStatisticsImpl::getThreadHistogramInfo(
    uint32_t histogramIndex) {
  auto info_ptr = static_cast<ThreadHistogramInfo*>(
      histograms_[histogramIndex].thread_value->Get());
  if (info_ptr == nullptr) {
    info_ptr = new ThreadHistogramInfo(&histograms_[histogramIndex].merged_hist,
                                       &histograms_[histogramIndex].merge_lock);
    histograms_[histogramIndex].thread_value->Reset(info_ptr);
  }
  return info_ptr;
}

std::unique_ptr<HistogramImpl>
CloudStatisticsImpl::HistogramInfo::getMergedHistogram() const {
  std::unique_ptr<HistogramImpl> res_hist(new HistogramImpl());
  {
    MutexLock lock(&merge_lock);
    res_hist->Merge(merged_hist);
  }
  thread_value->Fold(
      [](void* curr_ptr, void* res) {
        auto tmp_res_hist = static_cast<HistogramImpl*>(res);
        auto curr_hist = static_cast<HistogramImpl*>(curr_ptr);
        tmp_res_hist->Merge(*curr_hist);
      },
      res_hist.get());
  return res_hist;
}

}
