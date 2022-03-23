//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//

#include "cloud/mock_cloud_storage_provider.h"

#include "cloud/cloud_storage_provider_impl.h"
#include "cloud/filename.h"
#include "file/file_util.h"
#include "options/options_helper.h"
#include "rocksdb/cloud/cloud_env_options.h"
#include "rocksdb/convenience.h"
#include "rocksdb/file_system.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
Status MockCloudStorageProvider::PrepareOptions(const ConfigOptions& options) {
  cenv_ = static_cast<CloudEnv*>(options.env);
  fs_ = cenv_->GetBaseEnv()->GetFileSystem();
  Status s = fs_->GetTestDirectory(IOOptions(), &root_, nullptr);
  if (s.ok()) {
    s = CloudStorageProvider::PrepareOptions(options);
  }
  if (s.ok() && cenv_->HasDestBucket()) {
    s = CreateLocalPath(cenv_->GetDestBucketName());
    if (s.ok()) {
      s = CreateLocalPath(cenv_->GetDestBucketName() + "/" +
                          cenv_->GetDestObjectPath());
    }
  }
  return s;
}

std::string MockCloudStorageProvider::GetLocalPath(
    const std::string& bucket) const {
  return NormalizePath(root_ + "/" + bucket);
}

std::string MockCloudStorageProvider::GetLocalPath(
    const std::string& bucket, const std::string& prefix) const {
  if (prefix.empty()) {
    return GetLocalPath(bucket);
  } else if (StartsWith(prefix, root_)) {
    return GetLocalPath(bucket + "/" + prefix.substr(root_.size()));
  } else {
    return GetLocalPath(bucket + "/" + prefix);
  }
}

Status MockCloudStorageProvider::CreateLocalPath(const std::string& path) {
  Status s;
  auto dir = NormalizePath(path);
  if (paths_.find(dir) == paths_.end()) {
    // Build all subdirectories as necessary
    for (auto pos = dir.find('/'); pos != std::string::npos;
         pos = dir.find('/', pos + 1)) {
      auto subdir = dir.substr(0, pos);
      if (paths_.find(subdir) == paths_.end()) {
        s = fs_->CreateDirIfMissing(GetLocalPath(subdir), IOOptions(), nullptr);
        if (s.ok()) {
          paths_.insert(subdir);
        } else {
          break;
        }
      }
    }
    if (s.ok() && paths_.find(dir) == paths_.end()) {
      s = fs_->CreateDirIfMissing(GetLocalPath(dir), IOOptions(), nullptr);
      if (s.ok()) {
        paths_.insert(dir);
      }
    }
  }
  return s;
}

Status MockCloudStorageProvider::CreateBucket(const std::string& bucket_name) {
  std::string path = GetLocalPath(bucket_name);
  Status s = fs_->CreateDir(path, IOOptions(), nullptr);
  return s;
}

Status MockCloudStorageProvider::ExistsBucket(const std::string& bucket_name) {
  std::string path = GetLocalPath(bucket_name);
  Status s = fs_->FileExists(path, IOOptions(), nullptr);
  return s;
}

Status MockCloudStorageProvider::EmptyBucket(const std::string& bucket_name,
                                             const std::string& object_path) {
  std::string path = GetLocalPath(bucket_name, object_path);
  Status s = fs_->FileExists(path, IOOptions(), nullptr);
  if (s.ok()) {
    s = DestroyDir(cenv_->GetBaseEnv(), path);
  }
  return s;
}

Status MockCloudStorageProvider::DeleteCloudObject(
    const std::string& bucket_name, const std::string& object_path) {
  std::string path = GetLocalPath(bucket_name, object_path);
  Status s = fs_->DeleteFile(path, IOOptions(), nullptr);
  return s;
}

Status MockCloudStorageProvider::ListCloudObjects(
    const std::string& bucket_name, const std::string& object_path,
    std::vector<std::string>* results) {
  std::string path = GetLocalPath(bucket_name, object_path);
  std::vector<std::string> children;
  Status s = fs_->GetChildren(path, IOOptions(), &children, nullptr);
  if (s.ok()) {
    for (const auto& c : children) {
      if (c == "." || c == "..") {
        continue;
      } else {
        results->push_back(c);
      }
    }
  }
  return s;
}

Status MockCloudStorageProvider::ExistsCloudObject(
    const std::string& bucket_name, const std::string& object_path) {
  std::string path = GetLocalPath(bucket_name, object_path);
  Status s = fs_->FileExists(path, IOOptions(), nullptr);
  return s;
}

Status MockCloudStorageProvider::GetCloudObjectSize(
    const std::string& bucket_name, const std::string& object_path,
    uint64_t* filesize) {
  std::string path = GetLocalPath(bucket_name, object_path);
  Status s = fs_->GetFileSize(path, IOOptions(), filesize, nullptr);
  if (!s.ok()) {
    Status st = ExistsCloudObject(bucket_name, object_path);
    if (st.IsNotFound()) {
      s = st;
    }
  }
  return s;
}

// Get the modification time of the object in cloud storage
Status MockCloudStorageProvider::GetCloudObjectModificationTime(
    const std::string& bucket_name, const std::string& object_path,
    uint64_t* time) {
  std::string path = GetLocalPath(bucket_name, object_path);
  Status s = fs_->GetFileModificationTime(path, IOOptions(), time, nullptr);
  if (!s.ok()) {
    Status st = ExistsCloudObject(bucket_name, object_path);
    if (st.IsNotFound()) {
      s = st;
    }
  }
  return s;
}

Status MockCloudStorageProvider::GetCloudObjectMetadata(
    const std::string& bucket_name, const std::string& object_path,
    CloudObjectInformation* info) {
  std::string path = GetLocalPath(bucket_name, object_path);

  info->content_hash.clear();  // No hash
  std::unique_ptr<FSSequentialFile> file;
  std::string data;
  Status s = GetCloudObjectSize(bucket_name, object_path, &info->size);
  if (s.ok()) {
    s = GetCloudObjectModificationTime(bucket_name, object_path,
                                       &info->modification_time);
  }
  if (s.ok()) {
    s = ReadFileToString(fs_.get(), path, &data);
  }
  if (s.ok()) {
    s = StringToMap(data, &info->metadata);
  }
  return s;
}

Status MockCloudStorageProvider::PutCloudObjectMetadata(
    const std::string& bucket_name, const std::string& object_path,
    const std::unordered_map<std::string, std::string>& metadata) {
  Status s = CreateLocalPath(dirname(bucket_name + "/" + object_path));
  if (s.ok()) {
    std::string data;
    for (const auto& iter : metadata) {
      data.append(iter.first).append("=").append(iter.second).append("; ");
    }
    std::string path = GetLocalPath(bucket_name, object_path);
    s = WriteStringToFile(fs_.get(), data, path);
  }
  return s;
}

Status MockCloudStorageProvider::CopyCloudObject(
    const std::string& src_bucket_name, const std::string& src_object_path,
    const std::string& dest_bucket_name, const std::string& dest_object_path) {
  std::string from_path = GetLocalPath(src_bucket_name, src_object_path);
  std::string to_path = GetLocalPath(dest_bucket_name, dest_object_path);
  Status s = CopyFile(fs_.get(), from_path, to_path, 0, true);
  return s;
}

// Downloads object from the cloud into a local directory
Status MockCloudStorageProvider::GetCloudObject(const std::string& bucket_name,
                                                const std::string& object_path,
                                                const std::string& local_path) {
  std::string from_path = GetLocalPath(bucket_name, object_path);
  Status s = fs_->FileExists(from_path, IOOptions(), nullptr);
  if (s.ok()) {
    s = CopyFile(fs_.get(), from_path, local_path, 0, true);
  }
  return s;
}

class MockCloudStorageReadableFile : public CloudStorageReadableFile {
 private:
  std::unique_ptr<FSRandomAccessFile> target_;
  mutable uint64_t offset_;

 public:
  MockCloudStorageReadableFile(std::unique_ptr<FSRandomAccessFile>&& t)
      : target_(std::move(t)), offset_(0) {}
  virtual const char* Type() const {
    return MockCloudStorageProvider::kClassName();
  }
  Status Skip(uint64_t n) override {
    offset_ += n;
    return Status::OK();
  }
  bool use_direct_io() const override {
    return false;
  }  // target_->use_direct_io(); }
  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  Status InvalidateCache(size_t offset, size_t length) override {
    return target_->InvalidateCache(offset, length);
  }
  Status PositionedRead(uint64_t offset, size_t n, Slice* result,
                        char* scratch) override {
    return Read(offset, n, result, scratch);
  }
  Status Read(size_t n, Slice* result, char* scratch) override {
    return Read(offset_, n, result, scratch);
  }
  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    IOOptions io_opts;
    IODebugContext dbg;
    Status s = target_->Read(offset, n, io_opts, result, scratch, &dbg);
    if (s.ok()) {
      offset_ = offset + result->size();
    }
    return s;
  }
};

class MockCloudStorageWritableFile : public CloudStorageWritableFileImpl {
 public:
  MockCloudStorageWritableFile(CloudEnv* env, const std::string& local_fname,
                               const std::string& bucket,
                               const std::string& cloud_fname,
                               const EnvOptions& options)
      : CloudStorageWritableFileImpl(env, local_fname, bucket, cloud_fname,
                                     options) {}
  virtual const char* Name() const override {
    return MockCloudStorageProvider::kClassName();
  }
};

Status MockCloudStorageProvider::NewCloudReadableFile(
    const std::string& bucket, const std::string& fname,
    std::unique_ptr<CloudStorageReadableFile>* result,
    const EnvOptions& options) {
  std::string path = GetLocalPath(bucket, fname);
  std::unique_ptr<FSRandomAccessFile> file;
  Status s = fs_->NewRandomAccessFile(path, options, &file, nullptr);
  if (s.ok()) {
    result->reset(new MockCloudStorageReadableFile(std::move(file)));
  }
  return s;
}

Status MockCloudStorageProvider::NewCloudWritableFile(
    const std::string& local_path, const std::string& bucket_name,
    const std::string& object_path,
    std::unique_ptr<CloudStorageWritableFile>* result,
    const EnvOptions& options) {
  result->reset(new MockCloudStorageWritableFile(cenv_, local_path, bucket_name,
                                                 object_path, options));
  return (*result)->status();
}

Status MockCloudStorageProvider::PutCloudObject(
    const std::string& local_file, const std::string& bucket_name,
    const std::string& object_path) {
  std::string to_path = GetLocalPath(bucket_name, object_path);
  Status s = CopyFile(fs_.get(), local_file, to_path, 0, true);
  return s;
}
}  // namespace ROCKSDB_NAMESPACE
