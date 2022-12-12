//  Copyright (c) 2022-present, Rockset, Inc.  All rights reserved.
#ifndef ROCKSDB_LITE

#include "rocksdb/cloud/fs_cloud.h"

#include "cloud/fs_cloud_impl.h"

namespace ROCKSDB_NAMESPACE {

  
std::shared_ptr<FileSystem> CloudFileSystem::CreateCloudFileSystem(const CloudEnvOptions& options, const std::shared_ptr<FileSystem>& base_fs,
									    Env* base_env, const std::shared_ptr<Logger>& l) {
  FileSystem* fs = new CloudFileSystemImpl(options, base_fs, base_env, l);
  return std::shared_ptr<FileSystem>(fs);
}

CloudFileSystem::CloudFileSystem(const CloudEnvOptions& options, const std::shared_ptr<FileSystem>& base_fs, Env* base_env, const std::shared_ptr<Logger>& l)
  : FileSystemWrapper(base_fs), cloud_env_options_(options), base_fs_(base_fs), base_env_(base_env), info_log_(l) {}

  
}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
