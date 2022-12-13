//  Copyright (c) 2022-present, Rockset, Inc.  All rights reserved.
//
#pragma once

#include <memory>

#include "rocksdb/file_system.h"

namespace ROCKSDB_NAMESPACE {

class CloudEnvImpl;
class FSCloudStorageReadableFile;

class CloudFileSystem : FileSystemWrapper {
 private:
  explicit CloudFileSystem(CloudEnvImpl* cloud_env);

 public:
  // Forwards all FileSystem calls to cloud_env (as per FileSystemWrapper),
  // except the ones that are overriden below
  static std::shared_ptr<FileSystem> CreateCloudFileSystem(
      CloudEnvImpl* cloud_env);

  const char* Name() const override { return "CloudFileSystem"; }

  IOStatus NewRandomAccessFile(const std::string& logical_fname,
                               const FileOptions& file_opts,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override;
  IOStatus NewSequentialFile(const std::string& fname,
                             const FileOptions& file_opts,
                             std::unique_ptr<FSSequentialFile>* result,
                             IODebugContext* dbg) override;

 private:
  // Gets the cloud object fname from the dest or src bucket
  IOStatus GetCloudObject(const std::string& fname,
                          const FileOptions& file_opts, IODebugContext* dbg);

  // Returns a FSCloudStorageReadableFile from the dest or src bucket
  IOStatus NewFSCloudReadableFile(
      const std::string& fname, const FileOptions& file_opts,
      std::unique_ptr<FSCloudStorageReadableFile>* result, IODebugContext* dbg);

  CloudEnvImpl* cloud_env_;
};

}  // namespace ROCKSDB_NAMESPACE
