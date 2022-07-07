// Copyright (c) 2017 Rockset.
#include <memory>
#include "cloud/cloud_storage_provider_impl.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#ifndef ROCKSDB_LITE

#include <unordered_map>
#include "cloud/manifest_reader.h"

#include "cloud/cloud_manifest.h"
#include "cloud/db_cloud_impl.h"
#include "cloud/filename.h"
#include "db/version_set.h"
#include "env/composite_env_wrapper.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

LocalManifestReader::LocalManifestReader(std::shared_ptr<Logger> info_log,
                                         CloudEnv* cenv)
    : info_log_(info_log), cenv_(cenv) {}

Status LocalManifestReader::GetLiveFilesLocally(
    const std::string& local_dbname, std::set<uint64_t>* list,
    std::string* manifest_file_version) const {
  auto cenv_impl = static_cast<CloudEnvImpl*>(cenv_);
  assert(cenv_impl);
  // cloud manifest should be set in CloudEnv, and it should map to local
  // CloudManifest
  assert(cenv_impl->GetCloudManifest());
  auto cloud_manifest = cenv_impl->GetCloudManifest();
  auto current_epoch = cloud_manifest->GetCurrentEpoch().ToString();

  std::unique_ptr<SequentialFileReader> manifest_file_reader;
  Status s;
  {
    auto cloud_storage_provider =
        std::dynamic_pointer_cast<CloudStorageProviderImpl>(
            cenv_impl->GetStorageProvider());
    assert(cloud_storage_provider);
    // file name here doesn't matter, it will always be mapped to the correct Manifest file.
    // use empty epoch here so that it will be recognized as manifest file type
    auto local_manifest_file = cenv_impl->RemapFilename(
        ManifestFileWithEpoch(local_dbname, "" /* epoch */));

    if (manifest_file_version != nullptr) {
      // Only fetch the latest Manifest file from cloud when we ask for the version number.
      auto remote_manifest_file =
          cenv_impl->GetSrcObjectPath() + "/" + basename(local_manifest_file);

      s = cloud_storage_provider->GetCloudObjectAndVersion(
          cenv_impl->GetSrcBucketName(), remote_manifest_file,
          local_manifest_file, manifest_file_version);
      if (!s.ok()) {
        return s;
      }

      // Assuming that we are running on versioned s3 when asking for version
      assert(!manifest_file_version->empty());
    }

    std::unique_ptr<SequentialFile> file;
    s = cenv_impl->NewSequentialFile(local_manifest_file, &file, EnvOptions());
    if (!s.ok()) {
      return s;
    }
    manifest_file_reader.reset(new SequentialFileReader(
        NewLegacySequentialFileWrapper(file), local_manifest_file));
  }

  return GetLiveFilesFromFileReader(std::move(manifest_file_reader), list);
}

Status LocalManifestReader::GetLiveFilesFromFileReader(
    std::unique_ptr<SequentialFileReader> file_reader,
    std::set<uint64_t>* list) const {
  Status s;
  // create a callback that gets invoked whil looping through the log records
  VersionSet::LogReporter reporter;
  reporter.status = &s;
  log::Reader reader(nullptr, std::move(file_reader), &reporter,
                     true /*checksum*/, 0);

  Slice record;
  std::string scratch;

  // keep track of each CF's live files on each level
  std::unordered_map<uint32_t,                // CF id
                     std::unordered_map<int,  // level
                                        std::unordered_set<uint64_t>>>
      cf_live_files;

  while (reader.ReadRecord(&record, &scratch) && s.ok()) {
    VersionEdit edit;
    s = edit.DecodeFrom(record);
    if (!s.ok()) {
      break;
    }

    // add the files that are added by this transaction
    std::vector<std::pair<int, FileMetaData>> new_files = edit.GetNewFiles();
    for (auto& one : new_files) {
      uint64_t num = one.second.fd.GetNumber();
      cf_live_files[edit.GetColumnFamily()][one.first].insert(num);
    }
    // delete the files that are removed by this transaction
    std::set<std::pair<int, uint64_t>> deleted_files = edit.GetDeletedFiles();
    for (auto& one : deleted_files) {
      int level = one.first;
      uint64_t num = one.second;
      // Deleted files should belong to some CF
      auto it = cf_live_files.find(edit.GetColumnFamily());
      if ((it == cf_live_files.end()) ||
          (it->second.count(level) == 0) ||
          (it->second[level].count(num) == 0)) {
        return Status::Corruption(
            "Corrupted Manifest file with unrecognized deleted file: " +
            std::to_string(level) + "," + std::to_string(num));
      }
      it->second[level].erase(num);
    }

    // Removing the files from dropped CF, since we don't mark the files as
    // deleted in Manifest when a CF is dropped,
    if (edit.IsColumnFamilyDrop()) {
      cf_live_files.erase(edit.GetColumnFamily());
    }
  }

  for (auto& [cf_id, live_files] : cf_live_files) {
    for (auto& [level, level_live_files]: live_files) {
      list->insert(level_live_files.begin(), level_live_files.end());
    }
  }

  file_reader.reset();
  return s;
}

ManifestReader::ManifestReader(std::shared_ptr<Logger> info_log, CloudEnv* cenv,
                               const std::string& bucket_prefix)
    : LocalManifestReader(info_log, cenv), bucket_prefix_(bucket_prefix) {}

//
// Extract all the live files needed by this MANIFEST file and corresponding
// cloud_manifest object
//
Status ManifestReader::GetLiveFiles(const std::string& bucket_path,
                                    std::set<uint64_t>* list) const {
  Status s;
  std::unique_ptr<CloudManifest> cloud_manifest;
  {
    std::unique_ptr<SequentialFile> file;
    auto cloudManifestFile = CloudManifestFile(bucket_path);
    s = cenv_->NewSequentialFileCloud(bucket_prefix_, cloudManifestFile, &file,
                                      EnvOptions());
    if (!s.ok()) {
      return s;
    }
    s = CloudManifest::LoadFromLog(
        std::unique_ptr<SequentialFileReader>(new SequentialFileReader(
            NewLegacySequentialFileWrapper(file), cloudManifestFile)),
        &cloud_manifest);
    if (!s.ok()) {
      return s;
    }
  }
  std::unique_ptr<SequentialFileReader> file_reader;
  {
    auto manifestFile = ManifestFileWithEpoch(
        bucket_path, cloud_manifest->GetCurrentEpoch().ToString());
    std::unique_ptr<SequentialFile> file;
    s = cenv_->NewSequentialFileCloud(bucket_prefix_, manifestFile, &file,
                                      EnvOptions());
    if (!s.ok()) {
      return s;
    }
    file_reader.reset(new SequentialFileReader(
        NewLegacySequentialFileWrapper(file), manifestFile));
  }

  return GetLiveFilesFromFileReader(std::move(file_reader), list);
}

Status ManifestReader::GetMaxFileNumberFromManifest(Env* env,
                                                    const std::string& fname,
                                                    uint64_t* maxFileNumber) {
  // We check if the file exists to return IsNotFound() error status if it does
  // (NewSequentialFile) doesn't have the same behavior on file not existing --
  // it returns IOError instead.
  auto s = env->FileExists(fname);
  if (!s.ok()) {
    return s;
  }
  std::unique_ptr<SequentialFile> file;
  s = env->NewSequentialFile(fname, &file, EnvOptions());
  if (!s.ok()) {
    return s;
  }

  VersionSet::LogReporter reporter;
  reporter.status = &s;
  log::Reader reader(
      NULL,
      std::unique_ptr<SequentialFileReader>(new SequentialFileReader(
          NewLegacySequentialFileWrapper(file), fname)),
      &reporter, true /*checksum*/, 0);

  Slice record;
  std::string scratch;

  *maxFileNumber = 0;
  while (reader.ReadRecord(&record, &scratch) && s.ok()) {
    VersionEdit edit;
    s = edit.DecodeFrom(record);
    if (!s.ok()) {
      break;
    }
    uint64_t f;
    if (edit.GetNextFileNumber(&f)) {
      assert(*maxFileNumber <= f);
      *maxFileNumber = f;
    }
  }
  return s;
}
}  // namespace ROCKSDB_NAMESPACE
#endif /* ROCKSDB_LITE */
