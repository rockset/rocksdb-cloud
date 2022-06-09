#pragma once

#include <string>

#include "db/memtable.h"
#include "rocksdb/options.h"
#include "util/autovector.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

// log number related info we store in the log with each `kMemtableSwitch` event
struct MemTableLogNumber {
  uint64_t column_family;
  uint64_t memtable_id;
  uint64_t memtable_next_log_num;
};

struct MemTableLogNumAndReplSeq {
  MemTableLogNumber* lognum{nullptr};
  std::string* replication_sequence{nullptr};
  bool lognum_initialized{false};
};

// A record corresponds to `kMemtableSwitch` event
struct MemTableSwitchRecord {
  void AddLogNum(MemTableLogNumber lognum) {
    lognums.push_back(std::move(lognum));
  }

  MemTableLogNumAndReplSeq AddUninitializedLogNum() {
    lognums.push_back({});
    return {&lognums.back(), &replication_sequence, false};
  }

  // Assuming all the lognums has been initialized
  MemTableLogNumAndReplSeq GetLogNumAndReplSeq(size_t idx) {
    return MemTableLogNumAndReplSeq{
      &lognums[idx],
      &replication_sequence,
      true};
  }

  // next_log_number for all the switched memtables
  autovector<MemTableLogNumber> lognums;
  std::string replication_sequence;
};


Status SerializeMemtableSwitchRecord(
    std::string* dst,
    const MemTableSwitchRecord &record);
Status DeserializeMemtableSwitchRecord(
    Slice* src,
    MemTableSwitchRecord* record);

// Record `kMemtableSwitch` event.
//
// NOTE: this function has to be called before corresponding `kManifestWrite`.
// We rely on this assumption during recovery based on Manifest and repliation
// log
void MaybeRecordMemtableSwitch(
  const std::shared_ptr<rocksdb::ReplicationLogListener> &replication_log_listener,
  const MemTableSwitchRecord& mem_switch_record);
}
