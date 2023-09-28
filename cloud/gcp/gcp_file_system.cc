#ifndef ROCKSDB_LITE

#include <string>

#include "rocksdb/convenience.h"
#include "rocksdb/utilities/object_registry.h"

#include "cloud/gcp/gcp_file_system.h"
#include "cloud/cloud_storage_provider_impl.h"

#ifdef USE_GCP

namespace ROCKSDB_NAMESPACE {
GcpFileSystem::GcpFileSystem(std::shared_ptr<FileSystem> const& underlying_fs,
                             CloudFileSystemOptions const& cloud_options,
                             std::shared_ptr<Logger> const& info_log)
    : CloudFileSystemImpl(cloud_options, underlying_fs, info_log) {}

Status GcpFileSystem::NewGcpFileSystem(
    std::shared_ptr<FileSystem> const& base_fs,
    CloudFileSystemOptions const& cloud_options,
    std::shared_ptr<Logger> const& info_log, CloudFileSystem** cfs) {
  Status status;
  *cfs = nullptr;
  auto fs = base_fs;
  if (!fs) {
    fs = FileSystem::Default();
  }
  std::unique_ptr<GcpFileSystem> gfs(
      new GcpFileSystem(fs, cloud_options, info_log));
  auto env = gfs->NewCompositeEnvFromThis(Env::Default());
  ConfigOptions config_options;
  config_options.env = env.get();
  status = gfs->PrepareOptions(config_options);
  if (status.ok()) {
    *cfs = gfs.release();
  }
  return status;
}

Status GcpFileSystem::NewGcpFileSystem(std::shared_ptr<FileSystem> const& fs,
                                       std::unique_ptr<CloudFileSystem>* cfs) {
  cfs->reset(new GcpFileSystem(fs, CloudFileSystemOptions()));
  return Status::OK();
}

Status GcpFileSystem::PrepareOptions(ConfigOptions const& options) {
  if (cloud_fs_options.src_bucket.GetRegion().empty() ||
      cloud_fs_options.dest_bucket.GetRegion().empty()) {
    std::string region;
    if (!CloudFileSystemOptions::GetNameFromEnvironment(
            "GCP_DEFAULT_REGION", "gcp_default_region", &region)) {
      region = default_region;
    }
    if (cloud_fs_options.src_bucket.GetRegion().empty()) {
      cloud_fs_options.src_bucket.SetRegion(region);
    }
    if (cloud_fs_options.dest_bucket.GetRegion().empty()) {
      cloud_fs_options.dest_bucket.SetRegion(region);
    }
  }
  if (cloud_fs_options.storage_provider == nullptr) {
    // If the user has not specified a storage provider, then use the default
    // provider for this CloudType
    Status s = CloudStorageProvider::CreateFromString(
        options, CloudStorageProviderImpl::kGcs(),
        &cloud_fs_options.storage_provider);
    if (!s.ok()) {
      return s;
    }
  }
  return CloudFileSystemImpl::PrepareOptions(options);
}

int CloudFileSystemImpl::RegisterGcpObjects(ObjectLibrary& library,
                                            std::string const& /*arg*/) {
  int count = 0;
#ifdef USE_GCP
  library.AddFactory<FileSystem>(
      CloudFileSystemImpl::kGcp(),
      [](std::string const& /*uri*/, std::unique_ptr<FileSystem>* guard,
         std::string* errmsg) {
        std::unique_ptr<CloudFileSystem> cguard;
        Status s =
            GcpFileSystem::NewGcpFileSystem(FileSystem::Default(), &cguard);
        if (s.ok()) {
          guard->reset(cguard.release());
          return guard->get();
        } else {
          *errmsg = s.ToString();
          return static_cast<FileSystem*>(nullptr);
        }
      });
  count++;
#endif
  library.AddFactory<CloudStorageProvider>(
      CloudStorageProviderImpl::kGcs(),
      [](std::string const& /*uri*/,
         std::unique_ptr<CloudStorageProvider>* guard, std::string* errmsg) {
        Status s = CloudStorageProviderImpl::CreateGcsProvider(guard);
        if (!s.ok()) {
          *errmsg = s.ToString();
        }
        return guard->get();
      });
  count++;
  return count;
}
}  // namespace ROCKSDB_NAMESPACE
#endif  // USE_GCP
#endif  // ROCKSDB_LITE