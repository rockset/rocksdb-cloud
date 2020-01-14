//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
#pragma once

#include <unordered_map>

#include "rocksdb/env.h"
#include "rocksdb/status.h"

namespace rocksdb {
class CloudEnv;
class CloudStorageProvider;
class Logger;
struct ColumnFamilyOptions;
struct DBOptions;

class CloudStorageReadableFile : virtual public SequentialFile,
                                 virtual public RandomAccessFile {
 public:
  CloudStorageReadableFile(const std::shared_ptr<Logger>& info_log,
                           const std::string& bucket, const std::string& fname,
                           uint64_t size);

  // sequential access, read data at current offset in file
  virtual Status Read(size_t n, Slice* result, char* scratch) override;

  // random access, read data from specified offset in file
  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const override;

  virtual Status Skip(uint64_t n) override;

  virtual size_t GetUniqueId(char* id, size_t max_size) const override;
  virtual const char* Name() const { return "cloud"; }

 protected:
  virtual Status DoCloudRead(uint64_t offset, size_t n, char* scratch,
                             uint64_t* bytes_read) const = 0;

  std::shared_ptr<Logger> info_log_;
  std::string bucket_;
  std::string fname_;
  uint64_t offset_;
  uint64_t file_size_;
};

// Appends to a file in S3.
class CloudStorageWritableFile : public WritableFile {
 protected:
  CloudEnv* env_;
  const char* class_;
  std::string fname_;
  std::string tmp_file_;
  Status status_;
  std::unique_ptr<WritableFile> local_file_;
  std::string bucket_;
  std::string cloud_fname_;
  bool is_manifest_;

 public:
  CloudStorageWritableFile(CloudEnv* env, const std::string& local_fname,
                           const std::string& bucket,
                           const std::string& cloud_fname,
                           const EnvOptions& options);

  virtual ~CloudStorageWritableFile();

  virtual Status Append(const Slice& data) override {
    assert(status_.ok());
    // write to temporary file
    return local_file_->Append(data);
  }

  Status PositionedAppend(const Slice& data, uint64_t offset) override {
    return local_file_->PositionedAppend(data, offset);
  }
  Status Truncate(uint64_t size) override {
    return local_file_->Truncate(size);
  }
  Status Fsync() override { return local_file_->Fsync(); }
  bool IsSyncThreadSafe() const override {
    return local_file_->IsSyncThreadSafe();
  }
  bool use_direct_io() const override { return local_file_->use_direct_io(); }
  size_t GetRequiredBufferAlignment() const override {
    return local_file_->GetRequiredBufferAlignment();
  }
  uint64_t GetFileSize() override { return local_file_->GetFileSize(); }
  size_t GetUniqueId(char* id, size_t max_size) const override {
    return local_file_->GetUniqueId(id, max_size);
  }
  Status InvalidateCache(size_t offset, size_t length) override {
    return local_file_->InvalidateCache(offset, length);
  }
  Status RangeSync(uint64_t offset, uint64_t nbytes) override {
    return local_file_->RangeSync(offset, nbytes);
  }
  Status Allocate(uint64_t offset, uint64_t len) override {
    return local_file_->Allocate(offset, len);
  }

  virtual Status Flush() override {
    assert(status_.ok());
    return local_file_->Flush();
  }

  virtual Status Sync() override;

  virtual Status status() { return status_; }

  virtual Status Close() override;
  virtual const char* Name() const { return "cloud"; }
};

class CloudStorageProvider  {
 public:
  static Status CreateS3Provider(std::unique_ptr<CloudStorageProvider>* result);
  virtual ~CloudStorageProvider();
  static const char *Type() { return "CloudStorageProvider"; }
  virtual const char* Name() const { return "cloud"; }
  virtual Status CreateBucket(const std::string& bucket_name) = 0;
  virtual Status ExistsBucket(const std::string& bucket_name) = 0;

  // Empties all contents of the associated cloud storage bucket.
  virtual Status EmptyBucket(const std::string& bucket_name,
                             const std::string& object_path) = 0;
  // Delete the specified object from the specified cloud bucket
  virtual Status DeleteObject(const std::string& bucket_name,
                              const std::string& object_path) = 0;

  // Does the specified object exist in the cloud storage
  // returns all the objects that have the specified path prefix and
  // are stored in a cloud bucket
  virtual Status ListObjects(const std::string& bucket_name,
                             const std::string& object_path,
                             std::vector<std::string>* path_names) = 0;

  // Does the specified object exist in the cloud storage
  virtual Status ExistsObject(const std::string& bucket_name,
                              const std::string& object_path) = 0;

  // Get the size of the object in cloud storage
  virtual Status GetObjectSize(const std::string& bucket_name,
                               const std::string& object_path,
                               uint64_t* filesize) = 0;

  // Get the modification time of the object in cloud storage
  virtual Status GetObjectModificationTime(const std::string& bucket_name,
                                           const std::string& object_path,
                                           uint64_t* time) = 0;

  // Get the metadata of the object in cloud storage
  virtual Status GetObjectMetadata(
      const std::string& bucket_name, const std::string& object_path,
      std::unordered_map<std::string, std::string>* metadata) = 0;

  // Copy the specified cloud object from one location in the cloud
  // storage to another location in cloud storage
  virtual Status CopyObject(const std::string& src_bucket_name,
                            const std::string& src_object_path,
                            const std::string& dest_bucket_name,
                            const std::string& dest_object_path) = 0;

  // Downloads object from the cloud into a local directory
  virtual Status GetObject(const std::string& bucket_name,
                           const std::string& object_path,
                           const std::string& local_path) = 0;
  // Uploads object to the cloud
  virtual Status PutObject(const std::string& local_path,
                           const std::string& bucket_name,
                           const std::string& object_path) = 0;
  // Updates/Sets the metadata of the object in cloud storage
  virtual Status PutObjectMetadata(
      const std::string& bucket_name, const std::string& object_path,
      const std::unordered_map<std::string, std::string>& metadata) = 0;
  virtual Status NewCloudWritableFile(
      const std::string& local_path, const std::string& bucket_name,
      const std::string& object_path,
      std::unique_ptr<CloudStorageWritableFile>* result,
      const EnvOptions& options) = 0;

  virtual Status NewCloudReadableFile(
      const std::string& bucket, const std::string& fname,
      std::unique_ptr<CloudStorageReadableFile>* result) = 0;
  virtual Status Prepare(CloudEnv *env);
  virtual Status Verify() const = 0;
};
}  // namespace rocksdb
