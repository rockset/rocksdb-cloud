// Copyright (c) 2017 Rockset.
#ifndef ROCKSDB_LITE
#include "rocksdb/cloud/cloud_env_options.h"

#include <cinttypes>
#include "rocksdb/cloud/cloud_log_controller.h"
#include "rocksdb/cloud/cloud_storage_provider.h"

namespace rocksdb {

void CloudEnvOptions::Dump(Logger* log) const {
  Header(log, "                         COptions.cloud_type: %s",
         ((storage_provider != nullptr) ?
          storage_provider->Name() : "None"));
  Header(log, "                           COptions.log_type: %s", 
         ((log_controller != nullptr) ?
          log_controller->Name() : "None"));
  Header(log, "               COptions.keep_local_sst_files: %d",
         keep_local_sst_files);
  Header(log, "               COptions.keep_local_log_files: %d",
         keep_local_log_files);
  Header(log, "             COptions.server_side_encryption: %d",
         server_side_encryption);
  Header(log, "                  COptions.encryption_key_id: %s",
         encryption_key_id.c_str());
  Header(log, "           COptions.create_bucket_if_missing: %s",
         create_bucket_if_missing ? "true" : "false");
  Header(log, "                         COptions.run_purger: %s",
         run_purger ? "true" : "false");
  Header(log, "           COptions.ephemeral_resync_on_open: %s",
         ephemeral_resync_on_open ? "true" : "false");
  Header(log, "             COptions.skip_dbid_verification: %s",
         skip_dbid_verification ? "true" : "false");
  Header(log, "           COptions.use_aws_transfer_manager: %s",
         use_aws_transfer_manager ? "true" : "false");
}

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
