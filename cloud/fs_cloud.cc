//  Copyright (c) 2022-present, Rockset, Inc.  All rights reserved.
#ifndef ROCKSDB_LITE

#include "rocksdb/cloud/fs_cloud.h"

#include "cloud/cloud_env_impl.h"
#include "cloud/filename.h"
#include "rocksdb/cloud/cloud_log_controller.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {

std::shared_ptr<FileSystem> CloudFileSystem::CreateCloudFileSystem(
    CloudEnvImpl* cloud_env) {
  FileSystem* fs = new CloudFileSystem(cloud_env);
  return std::shared_ptr<FileSystem>(fs);
}

CloudFileSystem::CloudFileSystem(CloudEnvImpl* cloud_env)
    : FileSystemWrapper(NewLegacyFileSystemWrapper(cloud_env)),
      cloud_env_(cloud_env) {}

IOStatus CloudFileSystem::NewRandomAccessFile(
    const std::string& logical_fname, const FileOptions& file_opts,
    std::unique_ptr<FSRandomAccessFile>* result, IODebugContext* dbg) {
  result->reset();

  auto fname = cloud_env_->RemapFilename(logical_fname);
  auto file_type = GetFileType(fname);
  bool sstfile = (file_type == RocksDBFileType::kSstFile),
       manifest = (file_type == RocksDBFileType::kManifestFile),
       identity = (file_type == RocksDBFileType::kIdentityFile),
       logfile = (file_type == RocksDBFileType::kLogFile);

  // Validate options
  auto st = status_to_io_status(cloud_env_->CheckOption(file_opts));
  if (!st.ok()) {
    return st;
  }

  auto* base_fs = cloud_env_->GetBaseEnv()->GetFileSystem().get();
  if (sstfile || manifest || identity) {
    if (cloud_env_->GetCloudEnvOptions().keep_local_sst_files ||
        cloud_env_->GetCloudEnvOptions().hasSstFileCache() || !sstfile) {
      // Read from local storage and then from cloud storage.
      st = base_fs->NewRandomAccessFile(fname, file_opts, result, dbg);

      // Found in local storage. Update LRU cache.
      // There is a loose coupling between the sst_file_cache and the files on
      // local storage. The sst_file_cache is only used for accounting of sst
      // files. We do not keep a reference to the LRU cache handle when the sst
      // file remains open by the db. If the LRU policy causes the file to be
      // evicted, it will be deleted from local storage, but because the db
      // already has an open file handle to it, it can continue to occupy local
      // storage space until the time the db decides to close the sst file.
      if (sstfile && st.ok()) {
        cloud_env_->FileCacheAccess(fname);
      }

      if (!st.ok() &&
          !base_fs->FileExists(fname, IOOptions(), dbg).IsNotFound()) {
        // if status is not OK, but file does exist locally, something is wrong
        return st;
      }

      if (!st.ok()) {
        // copy the file to the local storage
        st = status_to_io_status(cloud_env_->GetCloudObject(fname));
        if (st.ok()) {
          // we successfully copied the file, try opening it locally now
          st = base_fs->NewRandomAccessFile(fname, file_opts, result, dbg);
        }
        // Update the size of our local sst file cache
        if (st.ok() && sstfile &&
            cloud_env_->GetCloudEnvOptions().hasSstFileCache()) {
          uint64_t local_size;
          Status statx =
              base_fs->GetFileSize(fname, IOOptions(), &local_size, dbg);
          if (statx.ok()) {
            cloud_env_->FileCacheInsert(fname, local_size);
          }
        }
      }
      // If we are being paranoic, then we validate that our file size is
      // the same as in cloud storage.
      if (st.ok() && sstfile &&
          cloud_env_->GetCloudEnvOptions().validate_filesize) {
        uint64_t remote_size = 0;
        uint64_t local_size = 0;
        auto stax = base_fs->GetFileSize(fname, IOOptions(), &local_size, dbg);
        if (!stax.ok()) {
          return stax;
        }
        stax = IOStatus::NotFound();
        if (cloud_env_->HasDestBucket()) {
          stax = status_to_io_status(
              cloud_env_->GetCloudObjectSize(fname, &remote_size));
        }
        if (stax.IsNotFound() && !cloud_env_->HasDestBucket()) {
          // It is legal for file to not be present in storage provider if
          // destination bucket is not set.
        } else if (!stax.ok() || remote_size != local_size) {
          std::string msg = std::string("[") + Name() + "] HeadObject src " +
                            fname + " local size " +
                            std::to_string(local_size) + " cloud size " +
                            std::to_string(remote_size) + " " + stax.ToString();
          Log(InfoLogLevel::ERROR_LEVEL, cloud_env_->GetLogger(), "%s",
              msg.c_str());
          return IOStatus::IOError(msg);
        }
      }
    } else {
      // Only execute this code path if files are not cached locally
      std::unique_ptr<FSCloudStorageReadableFile> file;
      st = NewFSCloudReadableFile(fname, file_opts, &file, dbg);
      if (st.ok()) {
        result->reset(file.release());
      }
    }
    Log(InfoLogLevel::DEBUG_LEVEL, cloud_env_->GetLogger(),
        "[%s] NewRandomAccessFile file %s %s", Name(), fname.c_str(),
        st.ToString().c_str());
    return st;

  } else if (logfile &&
             !cloud_env_->GetCloudEnvOptions().keep_local_log_files) {
    // read from LogController
    st = cloud_env_->GetCloudEnvOptions()
             .cloud_log_controller->NewRandomAccessFile(fname, file_opts,
                                                        result, dbg);
    return st;
  }

  // This is neither a sst file or a log file. Read from default env.
  return base_fs->NewRandomAccessFile(fname, file_opts, result, dbg);
}

IOStatus CloudFileSystem::NewSequentialFile(
    const std::string& logical_fname, const FileOptions& file_opts,
    std::unique_ptr<FSSequentialFile>* result, IODebugContext* dbg) {
  result->reset();

  auto fname = cloud_env_->RemapFilename(logical_fname);
  auto file_type = GetFileType(fname);
  bool sstfile = (file_type == RocksDBFileType::kSstFile),
       manifest = (file_type == RocksDBFileType::kManifestFile),
       identity = (file_type == RocksDBFileType::kIdentityFile),
       logfile = (file_type == RocksDBFileType::kLogFile);

  auto st = status_to_io_status(cloud_env_->CheckOption(file_opts));
  if (!st.ok()) {
    return st;
  }

  auto* base_fs = cloud_env_->GetBaseEnv()->GetFileSystem().get();
  if (sstfile || manifest || identity) {
    if (cloud_env_->GetCloudEnvOptions().keep_local_sst_files || !sstfile) {
      // We read first from local storage and then from cloud storage.
      st = base_fs->NewSequentialFile(fname, file_opts, result, dbg);
      if (!st.ok()) {
        // copy the file to the local storage if keep_local_sst_files is true
        st = status_to_io_status(cloud_env_->GetCloudObject(fname));
        if (st.ok()) {
          // we successfully copied the file, try opening it locally now
          st = base_fs->NewSequentialFile(fname, file_opts, result, dbg);
        }
      }
    } else {
      std::unique_ptr<FSCloudStorageReadableFile> file;
      st = NewFSCloudReadableFile(fname, file_opts, &file, dbg);
      if (st.ok()) {
        result->reset(file.release());
      }
    }
    // Do not update the sst_file_cache for sequential read patterns.
    // These are mostly used by compaction.
    Log(InfoLogLevel::DEBUG_LEVEL, cloud_env_->GetLogger(),
        "[%s] NewSequentialFile file %s %s", Name(), fname.c_str(),
        st.ToString().c_str());
    return st;

  } else if (logfile &&
             !cloud_env_->GetCloudEnvOptions().keep_local_log_files) {
    return cloud_env_->GetCloudEnvOptions()
        .cloud_log_controller->NewSequentialFile(fname, file_opts, result, dbg);
  }

  // This is neither a sst file or a log file. Read from default env.
  return base_fs->NewSequentialFile(fname, file_opts, result, dbg);
}

IOStatus CloudFileSystem::NewFSCloudReadableFile(
    const std::string& fname, const FileOptions& file_opts,
    std::unique_ptr<FSCloudStorageReadableFile>* result, IODebugContext* dbg) {
  IOStatus st = IOStatus::NotFound();
  if (cloud_env_->HasDestBucket()) {
    // read from destination
    st = cloud_env_->GetStorageProvider()->NewFSCloudReadableFile(
        cloud_env_->GetDestBucketName(), cloud_env_->destname(fname), file_opts,
        result, dbg);
    if (st.ok()) {
      return st;
    }
  }
  if (cloud_env_->HasSrcBucket() && !cloud_env_->SrcMatchesDest()) {
    // read from src bucket
    st = cloud_env_->GetStorageProvider()->NewFSCloudReadableFile(
        cloud_env_->GetSrcBucketName(), cloud_env_->srcname(fname), file_opts,
        result, dbg);
  }
  return st;
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
