// Copyright (c) 2017 Rockset

#pragma once

#include "rocksdb/cloud/cloud_storage_provider.h"
#include <optional>

namespace ROCKSDB_NAMESPACE {
class CloudStorageReadableFileImpl : public CloudStorageReadableFile {
 public:
  CloudStorageReadableFileImpl(Logger* info_log, std::string const& bucket,
                               std::string const& fname, uint64_t size);
  // sequential access, read data at current offset in file
  IOStatus Read(size_t n, IOOptions const& options, Slice* result,
                char* scratch, IODebugContext* dbg) override;

  // random access, read data from specified offset in file
  IOStatus Read(uint64_t offset, size_t n, IOOptions const& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override;

  IOStatus Skip(uint64_t n) override;

 protected:
  virtual IOStatus DoCloudRead(uint64_t offset, size_t n,
                               IOOptions const& options, char* scratch,
                               uint64_t* bytes_read,
                               IODebugContext* dbg) const = 0;

  Logger* info_log_;
  std::string bucket_;
  std::string fname_;
  uint64_t offset_;
  uint64_t file_size_;
};

// Appends to a file in S3.
class CloudStorageWritableFileImpl : public CloudStorageWritableFile {
 protected:
  CloudFileSystem* cfs_;
  char const* class_;
  std::string fname_;
  std::string tmp_file_;
  IOStatus status_;
  std::unique_ptr<FSWritableFile> local_file_;
  std::string bucket_;
  std::string cloud_fname_;
  bool is_manifest_;

 public:
  CloudStorageWritableFileImpl(CloudFileSystem* fs,
                               std::string const& local_fname,
                               std::string const& bucket,
                               std::string const& cloud_fname,
                               FileOptions const& file_opts);

  virtual ~CloudStorageWritableFileImpl();
  using CloudStorageWritableFile::Append;
  IOStatus Append(Slice const& data, IOOptions const& opts,
                  IODebugContext* dbg) override {
    assert(status_.ok());
    // write to temporary file
    return local_file_->Append(data, opts, dbg);
  }

  using CloudStorageWritableFile::PositionedAppend;
  IOStatus PositionedAppend(Slice const& data, uint64_t offset,
                            IOOptions const& opts,
                            IODebugContext* dbg) override {
    return local_file_->PositionedAppend(data, offset, opts, dbg);
  }
  IOStatus Truncate(uint64_t size, IOOptions const& opts,
                    IODebugContext* dbg) override {
    return local_file_->Truncate(size, opts, dbg);
  }
  IOStatus Fsync(IOOptions const& opts, IODebugContext* dbg) override {
    return local_file_->Fsync(opts, dbg);
  }
  bool IsSyncThreadSafe() const override {
    return local_file_->IsSyncThreadSafe();
  }
  bool use_direct_io() const override { return local_file_->use_direct_io(); }
  size_t GetRequiredBufferAlignment() const override {
    return local_file_->GetRequiredBufferAlignment();
  }
  uint64_t GetFileSize(IOOptions const& opts, IODebugContext* dbg) override {
    return local_file_->GetFileSize(opts, dbg);
  }
  size_t GetUniqueId(char* id, size_t max_size) const override {
    return local_file_->GetUniqueId(id, max_size);
  }
  IOStatus InvalidateCache(size_t offset, size_t length) override {
    return local_file_->InvalidateCache(offset, length);
  }
  IOStatus RangeSync(uint64_t offset, uint64_t nbytes, IOOptions const& opts,
                     IODebugContext* dbg) override {
    return local_file_->RangeSync(offset, nbytes, opts, dbg);
  }
  IOStatus Allocate(uint64_t offset, uint64_t len, IOOptions const& opts,
                    IODebugContext* dbg) override {
    return local_file_->Allocate(offset, len, opts, dbg);
  }

  IOStatus Flush(IOOptions const& opts, IODebugContext* dbg) override {
    assert(status_.ok());
    return local_file_->Flush(opts, dbg);
  }
  IOStatus status() override { return status_; }
  IOStatus Sync(IOOptions const& opts, IODebugContext* dbg) override;
  IOStatus Close(IOOptions const& opts, IODebugContext* dbg) override;
};

// All writes to this DB can be configured to be persisted
// in cloud storage.
//
class Random64;
class CloudStorageProviderImpl : public CloudStorageProvider {
 public:
  static Status CreateS3Provider(std::unique_ptr<CloudStorageProvider>* result);
  static Status CreateGcsProvider(
      std::unique_ptr<CloudStorageProvider>* result);
  static char const* kS3() { return "s3"; }
  static char const* kGcs() { return "gcs"; }

  CloudStorageProviderImpl();
  virtual ~CloudStorageProviderImpl();
  IOStatus GetCloudObject(std::string const& bucket_name,
                          std::string const& object_path,
                          std::string const& local_destination) override;
  IOStatus PutCloudObject(std::string const& local_file,
                          std::string const& bucket_name,
                          std::string const& object_path) override;
  IOStatus NewCloudReadableFile(
      std::string const& bucket, std::string const& fname,
      FileOptions const& options,
      std::unique_ptr<CloudStorageReadableFile>* result,
      IODebugContext* dbg) override;
  Status PrepareOptions(ConfigOptions const& options) override;

 protected:
  std::unique_ptr<Random64> rng_;
  virtual IOStatus DoNewCloudReadableFile(
      std::string const& bucket, std::string const& fname, uint64_t fsize,
      std::string const& content_hash, FileOptions const& options,
      std::unique_ptr<CloudStorageReadableFile>* result,
      IODebugContext* dbg) = 0;

  // Downloads object from the cloud into a local directory
  virtual IOStatus DoGetCloudObject(std::string const& bucket_name,
                                    std::string const& object_path,
                                    std::string const& local_path,
                                    uint64_t* remote_size) = 0;
  virtual IOStatus DoPutCloudObject(std::string const& local_file,
                                    std::string const& object_path,
                                    std::string const& bucket_name,
                                    uint64_t file_size) = 0;

  CloudFileSystem* cfs_;
  Status status_;
};
}  // namespace ROCKSDB_NAMESPACE
