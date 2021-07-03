// Copyright (c) 2017 Rockset

#pragma once

#ifndef ROCKSDB_LITE
#include "rocksdb/cloud/cloud_storage_provider.h"

namespace ROCKSDB_NAMESPACE {
class CloudEnv;
class FileSystem;
struct ConfigOptions;
  
// All writes to this DB can be configured to be persisted
// in cloud storage.
//
class MockCloudStorageProvider : public CloudStorageProvider {
 public:
  MockCloudStorageProvider() {}
  static const char *kClassName() { return "mock"; }
  const char *Name() const override { return kClassName(); }

  Status CreateBucket(const std::string& bucket_name) override;
  Status ExistsBucket(const std::string& bucket_name) override;
  Status EmptyBucket(const std::string& bucket_name,
                     const std::string& object_path) override;
  Status DeleteCloudObject(const std::string& bucket_name,
                           const std::string& object_path) override;
  Status ListCloudObjects(const std::string& bucket_name,
                          const std::string& object_path,
                          std::vector<std::string>* path_names) override;
  Status ExistsCloudObject(const std::string& bucket_name,
                           const std::string& object_path) override;
  // Get the size of the object in cloud storage
  Status GetCloudObjectSize(const std::string& bucket_name,
                            const std::string& object_path,
                            uint64_t* filesize) override;

  // Get the modification time of the object in cloud storage
  Status GetCloudObjectModificationTime(const std::string& bucket_name,
                                                const std::string& object_path,
                                                uint64_t* time) override;

  // Get the metadata of the object in cloud storage
  Status GetCloudObjectMetadata(const std::string& bucket_name,
                                const std::string& object_path,
                                CloudObjectInformation* info) override;

  Status CopyCloudObject(const std::string& src_bucket_name,
                         const std::string& src_object_path,
                         const std::string& dest_bucket_name,
                         const std::string& dest_object_path) override;
  // Updates/Sets the metadata of the object in cloud storage
  Status PutCloudObjectMetadata(
      const std::string& bucket_name, const std::string& object_path,
      const std::unordered_map<std::string, std::string>& metadata) override;

  // Create a new cloud file in the appropriate location from the input path.
  // Updates result with the file handle.
  Status NewCloudWritableFile(
      const std::string& local_path, const std::string& bucket_name,
      const std::string& object_path,
      std::unique_ptr<CloudStorageWritableFile>* result,
      const EnvOptions& options) override;

  // Create a new readable cloud file, returning the file handle in result.
  Status NewCloudReadableFile(
      const std::string& bucket, const std::string& fname,
      std::unique_ptr<CloudStorageReadableFile>* result,
      const EnvOptions& options) override;

  // Downloads object from the cloud into a local directory
  Status GetCloudObject(const std::string& bucket_name,
                        const std::string& object_path,
                        const std::string& local_path) override;
  Status PutCloudObject(const std::string& local_file,
                        const std::string& bucket_name,
                        const std::string& object_path) override;
  
  // Prepares/Initializes the storage provider for the input cloud environment
  virtual Status PrepareOptions(const ConfigOptions& options) override;
protected:
private:
  std::string GetLocalPath(const std::string& bucket) const;
  
  std::string GetLocalPath(const std::string& bucket,
                           const std::string& prefix) const;
                        
#ifdef MJR
  Status CopyFile(const std::string& from_path, const std::string& to_path);
#endif
  std::string root_;
  CloudEnv *cenv_;
  std::unordered_map<std::string, std::unordered_map<std::string, std::string>> metadata_;
  std::shared_ptr<FileSystem> fs_;
};
}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
