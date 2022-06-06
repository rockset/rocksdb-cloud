#include "db/db_impl/replication_codec.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

Status SerializeMemtableSwitchRecord(std::string* dst, const MemtableSwitchRecord &record) {
  PutFixed16(dst, 1); // version
  PutLengthPrefixedSlice(dst, record.replication_sequence);
  PutVarint64(dst, record.lognums.size());
  for (auto lognum: record.lognums) {
    PutVarint64(dst, lognum.column_family);
    PutVarint64(dst, lognum.memtable_id);
    PutVarint64(dst, lognum.memtable_next_log_num);
  }
  return Status::OK();
}
Status DeserializeMemtableSwitchRecord(Slice* src, MemtableSwitchRecord* record) {
  uint16_t version;
  if (!GetFixed16(src, &version)) {
    return Status::Corruption("Unable to decode memtable switch record version");
  }
  Slice replication_sequence_slice;
  if (!GetLengthPrefixedSlice(src, &replication_sequence_slice)) {
    return Status::Corruption("Unable to decode memtable switch replication sequence");
  }
  uint64_t size;
  if (!GetVarint64(src, &size)) {
    return Status::Corruption("Unable to decode memtable switch lognums array size");
  }
  autovector<MemtableLogNumber> lognums;
  for (uint64_t i = 0; i < size; i++) {
    uint64_t column_family, memtable_id, next_log_num;
    if (!GetVarint64(src, &column_family)) {
      return Status::Corruption("Unable to decode memtable switch record column family");
    }
    if (!GetVarint64(src, &memtable_id)) {
      return Status::Corruption("Unable to decode memtable switch record memtable id");
    }
    if (!GetVarint64(src, &next_log_num)) {
      return Status::Corruption("Unable to decode memtable switch next log num");
    }
    lognums.push_back({column_family, memtable_id, next_log_num});
  }
  record->replication_sequence = replication_sequence_slice.ToString(false);
  record->lognums = std::move(lognums);
  return Status::OK();
}

void MaybeRecordMemtableSwitch(
    const std::shared_ptr<rocksdb::ReplicationLogListener>&
        replication_log_listener,
    const MemtableSwitchRecord& mem_switch_record) {
  if (replication_log_listener) {
    ReplicationLogRecord rlr;
    rlr.type = ReplicationLogRecord::kMemtableSwitch;
    SerializeMemtableSwitchRecord(&rlr.contents, mem_switch_record);
    replication_log_listener->OnReplicationLogRecord(
      mem_switch_record.replication_sequence,
      std::move(rlr));
  }
}
}
