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
  ManifestReader(CloudEnv* cenv, const std::string& bucket_prefix);

  virtual ~ManifestReader();

  // Retrieve all live files referred to by this bucket path
  Status GetLiveFiles(const std::string bucket_path, std::set<uint64_t>* list);

  static Status GetMaxFileNumberFromManifest(Env* env, const std::string& fname,
                                             uint64_t* maxFileNumber);

 private:
  CloudEnv* cenv_;
  std::string bucket_prefix_;
};
}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
