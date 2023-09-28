#include "cloud/gcp/gcp_file_system.h"
#include "rocksdb/cloud/cloud_file_system.h"
#include <chrono>

#ifdef USE_GCP
#include <google/cloud/optional.h>
#include <google/cloud/options.h>
#include <google/cloud/retry_policy.h>
#include <google/cloud/storage/idempotency_policy.h>
#include <google/cloud/storage/options.h>
#include <google/cloud/storage/retry_policy.h>
#endif  // USE_GCP

namespace ROCKSDB_NAMESPACE {
#ifdef USE_GCP
namespace gcp = ::google::cloud;
namespace gcs = ::google::cloud::storage;

// A retry policy that limits the total time spent and counts retrying.
class GcpRetryPolicy : public gcs::RetryPolicy {
 public:
  template <typename DurationRep, typename DurationPeriod>
  explicit GcpRetryPolicy(
      CloudFileSystem* fs,
      std::chrono::duration<DurationRep, DurationPeriod> maximum_duration)
      : cfs_(fs),
        deadline_(std::chrono::system_clock::now() + maximum_duration),
        time_based_policy_(maximum_duration) {}

  std::chrono::milliseconds maximum_duration() const {
    return time_based_policy_.maximum_duration();
  }

  bool OnFailure(gcp::Status const& s) override {
    bool is_retryable = time_based_policy_.OnFailure(s);
    ++failure_count_;
    if (is_retryable) {
      // transient failure and resource available
      if (failure_count_ <= maximum_failures_) {
        Log(InfoLogLevel::INFO_LEVEL, cfs_->GetLogger(),
            "[gcs] Encountered failure: %s"
            "retried %d / %d times. Retrying...",
            s.message().c_str(), failure_count_, maximum_failures_);
        return true;
      } else {
        Log(InfoLogLevel::INFO_LEVEL, cfs_->GetLogger(),
            "[gcs] Encountered failure: %s"
            "retry attempt %d exceeds max retries %d. Aborting...",
            s.message().c_str(), failure_count_, maximum_failures_);
        // retry count exceed maxnum, but is not nonretryable
        return false;
      }
    } else {
      // non-transient failure or resource exhausted
      Log(InfoLogLevel::INFO_LEVEL, cfs_->GetLogger(),
          "[gcs] Encountered permanent failure: %s"
          "retry attempt %d / %d. Aborting...",
          s.message().c_str(), failure_count_, maximum_failures_);
      return false;
    }
  }

  bool IsExhausted() const override {
    return (time_based_policy_.IsExhausted() ||
            failure_count_ > maximum_failures_);
  }
  bool IsPermanentFailure(gcp::Status const& s) const override {
    return gcs::internal::StatusTraits::IsPermanentFailure(s);
  }

  std::unique_ptr<RetryPolicy> clone() const override {
    return std::make_unique<GcpRetryPolicy>(
        cfs_, time_based_policy_.maximum_duration());
  }

 private:
  // rocksdb retries, etc
  int failure_count_ = 0;
  int maximum_failures_ = 10;
  CloudFileSystem* cfs_;
  std::chrono::system_clock::time_point deadline_;
  // non-permermanent status in gcs::internal::StatusTraits
  gcp::internal::LimitedTimeRetryPolicy<gcs::internal::StatusTraits>
      time_based_policy_;
};

#endif /* USE_GCP */

#ifdef USE_GCP
Status GcpCloudOptions::GetClientConfiguration(CloudFileSystem* fs,
                                               std::string const& /*region*/,
                                               gcp::Options& options) {
  // Default gcs operation timeout is 10 minutes after all retrys.
  uint64_t timeout_ms = 600000;
  // All storage operations are idempotent, so we can use always retry.
  options.set<gcs::IdempotencyPolicyOption>(
      gcs::AlwaysRetryIdempotencyPolicy().clone());

  // Use exponential backoff with a 1ms initial delay, 1 minute maximum delay,
  options.set<gcs::BackoffPolicyOption>(
      gcs::ExponentialBackoffPolicy(std::chrono::milliseconds(1),
                                    std::chrono::minutes(1), 2.0)
          .clone());

  // Use request_timeout_ms from CloudFileSystemOptions if set.
  auto const& cloud_fs_options = fs->GetCloudFileSystemOptions();
  if (cloud_fs_options.request_timeout_ms != 0) {
    timeout_ms = cloud_fs_options.request_timeout_ms;
  }
  // Use timed and max retry count based retry policy.
  options.set<gcs::RetryPolicyOption>(
      GcpRetryPolicy(fs, std::chrono::milliseconds(timeout_ms)).clone());
  return Status::OK();
}
#else
Status GcpCloudOptions::GetClientConfiguration(CloudFileSystem*,
                                               std::string const&,
                                               gcp::Options&) {
  return Status::NotSupported("Not configured for GCP support");
}
#endif /* USE_GCP */

}  // namespace ROCKSDB_NAMESPACE