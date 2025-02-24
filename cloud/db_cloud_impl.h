// Copyright (c) 2017 Rockset

#pragma once

#ifndef ROCKSDB_LITE
#include <deque>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/cloud/db_cloud.h"
#include "rocksdb/db.h"
#include "port/port_posix.h"
#include "monitoring/instrumented_mutex.h"

namespace ROCKSDB_NAMESPACE {

class Env;

//
// All writes to this DB can be configured to be persisted
// in cloud storage.
//
class DBCloudImpl : public DBCloud {
  friend DBCloud;

 public:
  virtual ~DBCloudImpl();
  Status Savepoint() override;

  Status CheckpointToCloud(const BucketOptions& destination,
                           const CheckpointToCloudOptions& options) override;

  Status WarmUp(size_t max_warmup_threads) override;

 protected:
  // The CloudFileSystem used by this open instance.
  CloudFileSystem* cfs_;

 private:
  Status DoCheckpointToCloud(const BucketOptions& destination,
                             const CheckpointToCloudOptions& options);

  // Maximum manifest file size
  static const uint64_t max_manifest_file_size = 4 * 1024L * 1024L;

  DBCloudImpl(DB* db, std::unique_ptr<Env> local_env);

  std::unique_ptr<Env> local_env_;

  std::atomic<bool> warm_up_is_running_{false};
  std::vector<port::Thread> warm_up_threads_;
};
}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
