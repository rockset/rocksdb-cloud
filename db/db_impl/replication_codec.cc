#include "db/db_impl/replication_codec.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

Status SerializeMemTableSwitchRecord(std::string* dst, const MemTableSwitchRecord &record) {
  PutFixed16(dst, 1); // version
  PutVarint64(dst, record.next_log_num);
  PutLengthPrefixedSlice(dst, record.replication_sequence);
  return Status::OK();
}
Status DeserializeMemTableSwitchRecord(Slice* src, MemTableSwitchRecord* record) {
  uint16_t version;
  if (!GetFixed16(src, &version)) {
    return Status::Corruption("Unable to decode memtable switch record version");
  }
  uint64_t next_log_num;
  if (!GetVarint64(src, &next_log_num)) {
    return Status::Corruption("Unable to decode memtable switch next_log_num");
  }
  Slice replication_sequence_slice;
  if (!GetLengthPrefixedSlice(src, &replication_sequence_slice)) {
    return Status::Corruption("Unable to decode memtable switch replication sequence");
  }

  record->next_log_num = next_log_num;
  record->replication_sequence = replication_sequence_slice.ToString(false);
  return Status::OK();
}

void MaybeRecordMemTableSwitch(
    const std::shared_ptr<rocksdb::ReplicationLogListener>&
        replication_log_listener,
    uint64_t next_log_num, MemTableSwitchRecord* mem_switch_record) {
  if (replication_log_listener) {
    mem_switch_record->replication_sequence =
        replication_log_listener->NewReplicationSequence();
    mem_switch_record->next_log_num = next_log_num;

    ReplicationLogRecord rlr;
    rlr.type = ReplicationLogRecord::kMemtableSwitch;
    SerializeMemTableSwitchRecord(&rlr.contents, *mem_switch_record);
    replication_log_listener->OnReplicationLogRecord(std::move(rlr));
  }
}
}
