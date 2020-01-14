// Copyright (c) 2017 Rockset

#pragma once

#ifndef ROCKSDB_LITE
#include "rocksdb/cloud/cloud_storage_provider.h"

namespace rocksdb {

//
// All writes to this DB can be configured to be persisted
// in cloud storage.
//
class CloudStorageProviderImpl : public CloudStorageProvider {
public:
  Status GetObject(const std::string& bucket_name,
                   const std::string& object_path,
                   const std::string& local_destination) override;
  Status PutObject(const std::string& local_file,
                   const std::string& bucket_name,
                   const std::string& object_path) override;
  Status NewCloudReadableFile(
                   const std::string& bucket, const std::string& fname,
                   std::unique_ptr<CloudStorageReadableFile>* result) override;
  virtual Status Prepare(CloudEnv *env) override;
  virtual Status Verify() const override;
 protected:
  virtual Status Initialize(CloudEnv* env);
  

  virtual Status DoNewCloudReadableFile(
      const std::string& bucket, const std::string& fname, uint64_t fsize,
      std::unique_ptr<CloudStorageReadableFile>* result) = 0;
  // Downloads object from the cloud into a local directory
  virtual Status DoGetObject(const std::string& bucket_name,
                             const std::string& object_path,
                             const std::string& local_path,
                             uint64_t* remote_size) = 0;
  virtual Status DoPutObject(const std::string& local_file,
                             const std::string& object_path,
                             const std::string& bucket_name,
                             uint64_t file_size) = 0;

  CloudEnv* env_;
  Status status_;
};
}  // namespace rocksdb
 
#endif // ROCKSDB_LITE
