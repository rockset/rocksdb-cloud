#pragma once

#include <string>

#include "db/memtable.h"
#include "rocksdb/options.h"
#include "util/autovector.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

// log number related info we store in the log with each `kMemtableSwitch` event
struct MemtableLogNumber {
  uint64_t column_family;
  uint64_t memtable_id;
  uint64_t memtable_next_log_num;
};

struct MemtableLogNumAndReplSeq {
  MemtableLogNumber* lognum{nullptr};
  std::string* replication_sequence{nullptr};
  bool lognum_initialized{false};
};

// A record corresponds to `kMemtableSwitch` event
struct MemtableSwitchRecord {
  void AddLogNum(MemtableLogNumber lognum) {
    lognums.push_back(std::move(lognum));
  }

  MemtableLogNumAndReplSeq AddUninitializedLogNum() {
    lognums.push_back({});
    return {&lognums.back(), &replication_sequence, false};
  }

  // Assuming all the lognums has been initialized
  MemtableLogNumAndReplSeq GetLogNumAndReplSeq(size_t idx) {
    return MemtableLogNumAndReplSeq{
      &lognums[idx],
      &replication_sequence,
      true};
  }

  // next_log_number for all the switched memtables
  autovector<MemtableLogNumber> lognums;
  std::string replication_sequence;
};


Status SerializeMemtableSwitchRecord(
    std::string* dst,
    const MemtableSwitchRecord &record);
Status DeserializeMemtableSwitchRecord(
    Slice* src,
    MemtableSwitchRecord* record);

void MaybeRecordMemtableSwitch(
  const std::shared_ptr<rocksdb::ReplicationLogListener> &replication_log_listener,
  const MemtableSwitchRecord& mem_switch_record);
}
