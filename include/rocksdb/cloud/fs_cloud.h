//  Copyright (c) 2022-present, Rockset, Inc.  All rights reserved.
//
#pragma once

#include <memory>
// #include <unordered_map>

#include "rocksdb/file_system.h"

#include "rocksdb/cloud/cloud_env_options.h"
// #include "rocksdb/configurable.h"
// #include "rocksdb/cache.h"
// #include "rocksdb/env.h"
// #include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
  
// TODO(estalgo): Inherit from FileSystem directly when we have an override for all methods
// TODO(estalgo): Change most/all return values to IOStatus ?
class CloudFileSystem : FileSystemWrapper {
 protected:
  CloudEnvOptions cloud_env_options_;

  std::shared_ptr<FileSystem> base_fs_;
  // TODO(esteban) Remove this, once all cals use base_fs_
  Env* base_env_;  // The underlying env

  CloudFileSystem(const CloudEnvOptions& options, const std::shared_ptr<FileSystem>& base_fs, Env* base_env, const std::shared_ptr<Logger>& l);

public:
  mutable std::shared_ptr<Logger> info_log_;  // informational messages

  static std::shared_ptr<FileSystem> CreateCloudFileSystem(const CloudEnvOptions& options, const std::shared_ptr<FileSystem>& base_fs,
                                                           Env* base_env, const std::shared_ptr<Logger>& l);

  const char* Name() const override { return "CloudFileSystem"; }

  // Returns the underlying env
  Env* GetBaseEnv() const { return base_env_; }
  virtual Status PreloadCloudManifest(const std::string& local_dbname) = 0;
  // This method will migrate the database that is using pure RocksDB into
  // RocksDB-Cloud. Call this before opening the database with RocksDB-Cloud.
  virtual Status MigrateFromPureRocksDB(const std::string& local_dbname) = 0;

  // Reads a file from the cloud
  virtual IOStatus NewSequentialFileCloud(const std::string& bucket_prefix,
                                          const std::string& fname,  const FileOptions& file_opts,
                                        std::unique_ptr<FSSequentialFile>* result,
                                        IODebugContext* dbg) = 0;

  // Saves and retrieves the dbid->dirname mapping in cloud storage
  /* TODO consider moving here rather than keeping these in CloudEnv
  virtual Status SaveDbid(const std::string& bucket_name,
                          const std::string& dbid,
                          const std::string& dirname) = 0;
  virtual Status GetPathForDbid(const std::string& bucket_prefix,
                                const std::string& dbid,
                                std::string* dirname) = 0;
  virtual Status GetDbidList(const std::string& bucket_prefix,
                             DbidList* dblist) = 0;
  virtual Status DeleteDbid(const std::string& bucket_prefix,
                            const std::string& dbid) = 0;
  */

  Logger* GetLogger() const { return info_log_.get(); }
  const std::shared_ptr<CloudStorageProvider>&  GetStorageProvider() const {
    return cloud_env_options_.storage_provider;
  }
  
  const std::shared_ptr<CloudLogController>& GetLogController() const {
    return cloud_env_options_.cloud_log_controller;
  }
  
  // The SrcBucketName identifies the cloud storage bucket and
  // GetSrcObjectPath specifies the path inside that bucket
  // where data files reside. The specified bucket is used in
  // a readonly mode by the associated DBCloud instance.
  const std::string& GetSrcBucketName() const {
    return cloud_env_options_.src_bucket.GetBucketName();
  }
  const std::string& GetSrcObjectPath() const {
    return cloud_env_options_.src_bucket.GetObjectPath();
  }
  bool HasSrcBucket() const { return cloud_env_options_.src_bucket.IsValid(); }

  // The DestBucketName identifies the cloud storage bucket and
  // GetDestObjectPath specifies the path inside that bucket
  // where data files reside. The associated DBCloud instance
  // writes newly created files to this bucket.
  const std::string& GetDestBucketName() const {
    return cloud_env_options_.dest_bucket.GetBucketName();
  }
  const std::string& GetDestObjectPath() const {
    return cloud_env_options_.dest_bucket.GetObjectPath();
  }

  bool HasDestBucket() const { return cloud_env_options_.dest_bucket.IsValid(); }
  bool SrcMatchesDest() const {
    if (HasSrcBucket() && HasDestBucket()) {
      return cloud_env_options_.src_bucket == cloud_env_options_.dest_bucket;
    } else {
      return false;
    }
  }

  // returns the options used to create this env
  const CloudEnvOptions& GetCloudEnvOptions() const {
    return cloud_env_options_;
  }

  // Deletes file from a destination bucket.
  virtual IOStatus DeleteCloudFileFromDest(const std::string& fname) = 0;
  // Copies a local file to a destination bucket.
  virtual IOStatus CopyLocalFileToDest(const std::string& local_name,
                                     const std::string& cloud_name) = 0;

  // Transfers the filename from RocksDB's domain to the physical domain, based
  // on information stored in CLOUDMANIFEST.
  // For example, it will map 00010.sst to 00010.sst-[epoch] where [epoch] is
  // an epoch during which that file was created.
  // Files both in S3 and in the local directory have this [epoch] suffix.
  virtual std::string RemapFilename(const std::string& logical_name) const = 0;

  // Find the list of live files based on CloudManifest and Manifest in local db
  //
  // For the returned filepath in `live_sst_files` and `manifest_file`, we only
  // include the basename of the filepath but not the directory prefix to the
  // file
  virtual IOStatus FindAllLiveFiles(const std::string& local_dbname,
                                  std::vector<std::string>* live_sst_files,
                                  std::string* manifest_file) = 0;
  // Find the list of live files based on CloudManifest and Manifest in local
  // db. Also, fetch the current Manifest file before reading.
  //
  // Besides returning live file paths, it also return the version of the fetched
  // manfiest file, if the cloud storage has versioning enabled
  //
  // NOTE: be careful with using this function since it will override the local
  // Manifest file in `local_dbname`!
  // 
  // REQUIRES: cloud storage has versioning enabled
  virtual IOStatus FindAllLiveFilesAndFetchManifest(
      const std::string& local_dbname, std::vector<std::string>* live_sst_files,
      std::string* manifest_file, std::string* manifest_file_version) = 0;

  // Apply cloud manifest delta to in-memory cloud manifest. Does not change the
  // on-disk state.
  //
  // If delta has already been applied in cloud manifest, delta_applied would be
  // `false`
  virtual Status ApplyCloudManifestDelta(const CloudManifestDelta& delta,
                                         bool* delta_applied) = 0;

  // This function does several things:
  // * Writes CLOUDMANIFEST-cookie file based on existing in-memory
  // CLOUDMANIFEST and provided CloudManifestDelta.
  // * Copies MANIFEST-epoch into MANIFEST-delta.epoch
  // * Uploads both CLOUDMANIFEST-cookie and MANIFEST-delta.epoch into cloud
  // storage (if dest bucket is given).
  //
  // Return InvalidArgument status if the delta has been applied in current
  // CloudManifest
  virtual Status RollNewCookie(const std::string& local_dbname,
                               const std::string& cookie,
                               const CloudManifestDelta& delta) const = 0;

  virtual Status DeleteCloudInvisibleFiles(
      const std::vector<std::string>& active_cookies) = 0;


};


}  // namespace ROCKSDB_NAMESPACE
