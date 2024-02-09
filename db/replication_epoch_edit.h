#pragma once

#include <cstdint>
#include <deque>
#include <optional>
#include <string>
#include <vector>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class Slice;
class Status;

// Replication epoch and first manifest update sequence in that epoch
class ReplicationEpochAddition {
  public:
    ReplicationEpochAddition() = default;
    ReplicationEpochAddition(uint64_t epoch, uint64_t first_mus);

    uint64_t GetEpoch() const { return epoch_; }
    uint64_t GetFirstMUS() const { return first_mus_; }

    void EncodeTo(std::string* output) const;
    Status DecodeFrom(Slice* input);

    std::string DebugString() const;
  private:
    // The replication epoch
    uint64_t epoch_{0};
    // First manifest update sequence of the replication epoch
    uint64_t first_mus_{0};
};

inline bool operator==(const ReplicationEpochAddition& ea1, const ReplicationEpochAddition& ea2) {
  return ea1.GetEpoch() == ea2.GetEpoch() && ea1.GetFirstMUS() == ea2.GetFirstMUS();
}
inline bool operator!=(const ReplicationEpochAddition& ea1, const ReplicationEpochAddition& ea2) {
  return !(ea1 == ea2);
}
inline bool operator<(const ReplicationEpochAddition& ea1, const ReplicationEpochAddition& ea2) {
  return (ea1.GetEpoch() < ea2.GetEpoch() &&
          ea1.GetFirstMUS() < ea2.GetFirstMUS()) ||
         (ea1.GetEpoch() == ea2.GetEpoch() &&
          ea1.GetFirstMUS() < ea2.GetFirstMUS());
}
inline bool operator>=(const ReplicationEpochAddition& ea1,
                       const ReplicationEpochAddition ea2) {
  return !(ea1 < ea2);
}
std::ostream& operator<<(std::ostream& os, const ReplicationEpochAddition& ea);

using ReplicationEpochAdditions = std::vector<ReplicationEpochAddition>;

class ReplicationEpochSet {
  public:
    Status AddEpoch(const ReplicationEpochAddition& epoch);
    Status AddEpochs(const ReplicationEpochAdditions& epochs);
    Status VerifyNewEpochs(const ReplicationEpochAdditions& epochs) const;

    void DeleteEpochsBefore(uint64_t epoch, uint32_t max_num_replication_epochs);

    // Find corresponding epoch for manifest update sequence.
    // Return std::nullopt if `mus` is not coverred by the
    // `ReplicationEpochSet`.
    std::optional<uint64_t> GetEpochForMUS(uint64_t mus) const;

    const auto& GetEpochs() const {
      return epochs_;
    }

    bool empty() const { return epochs_.empty(); }
    auto size() const { return epochs_.size(); }

   private:
    std::deque<ReplicationEpochAddition> epochs_;
};

bool operator==(const ReplicationEpochSet& es1, const ReplicationEpochSet& es2);

}
