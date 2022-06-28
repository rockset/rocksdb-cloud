// Copyright (c) 2017 Rockset

#pragma once

#ifndef ROCKSDB_LITE
#include <set>
#include <string>
#include <vector>

#include "cloud/cloud_env_impl.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {

//
// Operates on MANIFEST files stored in the cloud bucket
//
class ManifestReader {
 public:
  ManifestReader(std::shared_ptr<Logger> info_log, CloudEnv* cenv,
                 const std::string& bucket_prefix);

  virtual ~ManifestReader();

  // Retrieve all live files referred to by this bucket path
  Status GetLiveFiles(const std::string& bucket_path,
                      std::set<uint64_t>* list) {
    std::unique_ptr<CloudManifest> cloud_manifest;
    // TODO(wei): this is not correct if we can roll cloud manifest without
    // reopening the shard. We should fix this by reading from
    // cenv_->cloud_manifest directly
    return GetLiveFilesAndCloudManifest(
        bucket_path, cenv_->GetCloudEnvOptions().cookie_on_open,
        list, &cloud_manifest);
  }

  // Retrive all live files referred by the bucket path and cookie, also get the
  // constructed cloud_manifest
  Status GetLiveFilesAndCloudManifest(
      const std::string& bucket_path,
      const std::string& cookie,
      std::set<uint64_t>* list,
      std::unique_ptr<CloudManifest>* cloud_manifest);

  static Status GetMaxFileNumberFromManifest(Env* env, const std::string& fname,
                                             uint64_t* maxFileNumber);

 private:
  std::shared_ptr<Logger> info_log_;
  CloudEnv* cenv_;
  std::string bucket_prefix_;
};
}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
