
#include <cloud/filename.h>
#include <include/rocksdb/remote_file_system.h>
#include <rocksdb/env.h>

#include "env/composite_env_wrapper.h"
#include "rocksdb/io_status.h"

namespace rocksdb {

IOStatus RemoteFileSystem::NewSequentialFile(
    const std::string& f, const FileOptions& file_opts,
    std::unique_ptr<FSSequentialFile>* r, IODebugContext* dbg) {
  r->reset();

  auto baseFileName = cloud_env_->RemapFilename(f);

  auto fType = GetFileType(baseFileName);
  if (fType != RocksDBFileType::kSstFile) {
    return FileSystemWrapper::NewSequentialFile(f, file_opts, r, dbg);
  }

  std::unique_ptr<CloudStorageReadableFile> cloud_file;
  auto st = cloud_storage_provider_->NewCloudReadableFile(
      cloud_env_->GetSrcBucketName(), f, &cloud_file, EnvOptions());

  std::unique_ptr<SequentialFile> result;
  if (st.ok()) {
    result.reset(cloud_file.release());
    r->reset(NewLegacySequentialFileWrapper(result).release());
    return IOStatus::OK();
  }
  return status_to_io_status(std::move(st));
}

IOStatus RemoteFileSystem::NewRandomAccessFile(
    const std::string& f, const FileOptions& file_opts,
    std::unique_ptr<FSRandomAccessFile>* r, IODebugContext* dbg) {
  r->reset();

  auto baseFileName = cloud_env_->RemapFilename(f);

  auto fType = GetFileType(baseFileName);
  if (fType != RocksDBFileType::kSstFile) {
    return FileSystemWrapper::NewRandomAccessFile(f, file_opts, r, dbg);
  }

  std::unique_ptr<CloudStorageReadableFile> cloud_file;
  auto st = cloud_storage_provider_->NewCloudReadableFile(
      cloud_env_->GetSrcBucketName(), f, &cloud_file, EnvOptions());

  std::unique_ptr<RandomAccessFile> result;
  if (st.ok()) {
    result.reset(cloud_file.release());
    r->reset(NewLegacyRandomAccessFileWrapper(result).release());
    return IOStatus::OK();
  }

  return status_to_io_status(std::move(st));
}

std::string RemoteFileSystem::destname(const std::string& localname) {
  assert(cloud_env_.dest_bucket.IsValid());
  return cloud_env_->GetDestObjectPath() + "/" + basename(localname);
}

bool RemoteFileSystem::isSstFile(const std::string& f) {
  auto baseFileName = cloud_env_->RemapFilename(f);
  return GetFileType(baseFileName) == RocksDBFileType::kSstFile;
}

IOStatus RemoteFileSystem::NewWritableFile(const std::string& f,
                                           const FileOptions& file_opts,
                                           std::unique_ptr<FSWritableFile>* r,
                                           IODebugContext* dbg) {
  r->reset();

  if (!isSstFile(f)) {
    return FileSystemWrapper::NewWritableFile(f, file_opts, r, dbg);
  }

  std::unique_ptr<CloudStorageWritableFile> cloud_file;
  auto st = cloud_storage_provider_->NewCloudWritableFile(
      f, cloud_env_->GetDestBucketName(), destname(f), &cloud_file,
      EnvOptions());

  std::unique_ptr<WritableFile> result;
  if (st.ok()) {
    result.reset(cloud_file.release());
    r->reset(NewLegacyWritableFileWrapper(std::move(result)).release());
    return IOStatus::OK();
  }

  return status_to_io_status(std::move(st));
}
}  // namespace rocksdb
