//  Copyright (c) 2022-present, Rockset, Inc.  All rights reserved.

#ifndef ROCKSDB_LITE

#include "cloud/fs_cloud_impl.h"

#include <cinttypes>

#include "cloud/cloud_env_wrapper.h"
#include "cloud/cloud_log_controller_impl.h"
#include "cloud/cloud_manifest.h"
#include "cloud/cloud_scheduler.h"
#include "cloud/filename.h"
#include "cloud/manifest_reader.h"
#include "db/db_impl/replication_codec.h"
#include "env/composite_env_wrapper.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "file/writable_file_writer.h"
#include "port/likely.h"
#include "rocksdb/cloud/cloud_log_controller.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "test_util/sync_point.h"
#include "util/xxhash.h"
#include "cloud/cloud_file_deletion_scheduler.h"

namespace ROCKSDB_NAMESPACE {


CloudFileSystemImpl::CloudFileSystemImpl(const CloudEnvOptions& opts, const std::shared_ptr<FileSystem>& base_fs, Env* base_env,
                           const std::shared_ptr<Logger>& l)
  : CloudFileSystem(opts, base_fs, base_env, l), purger_is_running_(true) {
  scheduler_ = CloudScheduler::Get();
  cloud_file_deletion_scheduler_ =
      CloudFileDeletionScheduler::Create(scheduler_);
}

CloudFileSystemImpl::~CloudFileSystemImpl() {
  // remove items from the file cache
  FileCachePurge();

  if (cloud_env_options_.cloud_log_controller) {
    cloud_env_options_.cloud_log_controller->StopTailingStream();
  }
  StopPurger();
}

IOStatus CloudFileSystemImpl::ExistsCloudObject(const std::string& fname) {
  Status st = Status::NotFound();
  if (HasDestBucket()) {
    st = GetStorageProvider()->ExistsCloudObject(GetDestBucketName(),
                                                 destname(fname));
  }
  if (st.IsNotFound() && HasSrcBucket() && !SrcMatchesDest()) {
    st = GetStorageProvider()->ExistsCloudObject(GetSrcBucketName(),
                                                 srcname(fname));
  }
  return status_to_io_status(std::move(st));
}
IOStatus CloudFileSystemImpl::GetCloudObject(const std::string& fname) {
  Status st = Status::NotFound();
  if (HasDestBucket()) {
    st = GetStorageProvider()->GetCloudObject(GetDestBucketName(),
                                              destname(fname), fname);
  }
  if (st.IsNotFound() && HasSrcBucket() && !SrcMatchesDest()) {
    st = GetStorageProvider()->GetCloudObject(GetSrcBucketName(),
                                              srcname(fname), fname);
  }
  return status_to_io_status(std::move(st));
}

IOStatus CloudFileSystemImpl::GetCloudObjectSize(const std::string& fname,
                                        uint64_t* remote_size) {
  Status st = Status::NotFound();
  if (HasDestBucket()) {
    st = GetStorageProvider()->GetCloudObjectSize(GetDestBucketName(),
                                                  destname(fname), remote_size);
  }
  if (st.IsNotFound() && HasSrcBucket() && !SrcMatchesDest()) {
    st = GetStorageProvider()->GetCloudObjectSize(GetSrcBucketName(),
                                                  srcname(fname), remote_size);
  }
  return status_to_io_status(std::move(st));
}

IOStatus CloudFileSystemImpl::GetCloudObjectModificationTime(const std::string& fname,
                                                    uint64_t* time) {
  Status st = Status::NotFound();
  if (HasDestBucket()) {
    st = GetStorageProvider()->GetCloudObjectModificationTime(
        GetDestBucketName(), destname(fname), time);
  }
  if (st.IsNotFound() && HasSrcBucket() && !SrcMatchesDest()) {
    st = GetStorageProvider()->GetCloudObjectModificationTime(
        GetSrcBucketName(), srcname(fname), time);
  }
  return status_to_io_status(std::move(st));
}

IOStatus CloudFileSystemImpl::ListCloudObjects(const std::string& path,
                                      std::vector<std::string>* result) {
  Status st;
  // Fetch the list of children from both cloud buckets
  if (HasSrcBucket()) {
    st = GetStorageProvider()->ListCloudObjects(GetSrcBucketName(),
                                                GetSrcObjectPath(), result);
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[%s] GetChildren src bucket %s %s error from %s %s", Name(),
          GetSrcBucketName().c_str(), path.c_str(),
          GetStorageProvider()->Name(), st.ToString().c_str());
      return status_to_io_status(std::move(st));
    }
  }
  if (HasDestBucket() && !SrcMatchesDest()) {
    st = GetStorageProvider()->ListCloudObjects(GetDestBucketName(),
                                                GetDestObjectPath(), result);
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[%s] GetChildren dest bucket %s %s error from %s %s", Name(),
          GetDestBucketName().c_str(), path.c_str(),
          GetStorageProvider()->Name(), st.ToString().c_str());
    }
  }
  return status_to_io_status(std::move(st));
}

IOStatus CloudFileSystemImpl::NewCloudReadableFile(
                                                 const std::string& fname, const FileOptions& file_opts, std::unique_ptr<FSCloudStorageReadableFile>* result,
    IODebugContext* dbg) {
  auto st = IOStatus::NotFound();
  if (HasDestBucket()) {  // read from destination
    st = GetStorageProvider()->NewFSCloudReadableFile(
                                                    GetDestBucketName(), destname(fname), file_opts, result, dbg);
    if (st.ok()) {
      return st;
    }
  }
  if (HasSrcBucket() && !SrcMatchesDest()) {  // read from src bucket
    st = GetStorageProvider()->NewFSCloudReadableFile(
                                                    GetSrcBucketName(), srcname(fname), file_opts, result, dbg);
  }
  return st;
}

// open a file for sequential reading
IOStatus CloudFileSystemImpl::NewSequentialFile(const std::string& logical_fname,
			     const FileOptions& file_opts,
			     std::unique_ptr<FSSequentialFile>* result,
                                                IODebugContext* dbg) {
  result->reset();

  auto fname = RemapFilename(logical_fname);
  auto file_type = GetFileType(fname);
  bool sstfile = (file_type == RocksDBFileType::kSstFile),
       manifest = (file_type == RocksDBFileType::kManifestFile),
       identity = (file_type == RocksDBFileType::kIdentityFile),
       logfile = (file_type == RocksDBFileType::kLogFile);

  auto st = CheckOption(file_opts);
  if (!st.ok()) {
    return st;
  }

  if (sstfile || manifest || identity) {
    if (cloud_env_options_.keep_local_sst_files || !sstfile) {
      // We read first from local storage and then from cloud storage.
      st = base_fs_->NewSequentialFile(fname, file_opts, result, dbg);
      if (!st.ok()) {
        // copy the file to the local storage if keep_local_sst_files is true
        st = GetCloudObject(fname);
        if (st.ok()) {
          // we successfully copied the file, try opening it locally now
          st = base_fs_->NewSequentialFile(fname, file_opts, result, dbg);
        }
      }
    } else {
      std::unique_ptr<FSCloudStorageReadableFile> file;
      st = NewCloudReadableFile(fname, file_opts, &file, dbg);
      if (st.ok()) {
        result->reset(file.release());
      }
    }
    // Do not update the sst_file_cache for sequential read patterns.
    // These are mostly used by compaction.
    Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
        "[%s] NewSequentialFile file %s %s", Name(), fname.c_str(),
        st.ToString().c_str());
    return st;

  } else if (logfile && !cloud_env_options_.keep_local_log_files) {
    //////////////////////////////////////////////////////
    // TODO(estalgo): Update CloudLogController interface
    #if 0
    return cloud_env_options_.cloud_log_controller->NewSequentialFile(
        fname, result, options);
    #endif
  }

  // This is neither a sst file or a log file. Read from default env.
  return base_fs_->NewSequentialFile(fname, file_opts, result, dbg);
}

// Ability to read a file directly from cloud storage
IOStatus CloudFileSystemImpl::NewSequentialFileCloud(
                                                     const std::string& bucket, const std::string& fname, const FileOptions& file_opts,
    std::unique_ptr<FSSequentialFile>* result,
    IODebugContext* dbg) {
  std::unique_ptr<FSCloudStorageReadableFile> file;
  auto st =
    GetStorageProvider()->NewFSCloudReadableFile(bucket, fname, file_opts, &file, dbg);
  if (!st.ok()) {
    return st;
  }

  result->reset(file.release());
  return st;
}

// open a file for random reading
IOStatus CloudFileSystemImpl::NewRandomAccessFile(
                                                const std::string& logical_fname, const FileOptions& file_opts, std::unique_ptr<FSRandomAccessFile>* result,
    IODebugContext* dbg) {
  result->reset();

  auto fname = RemapFilename(logical_fname);
  auto file_type = GetFileType(fname);
  bool sstfile = (file_type == RocksDBFileType::kSstFile),
       manifest = (file_type == RocksDBFileType::kManifestFile),
       identity = (file_type == RocksDBFileType::kIdentityFile),
       logfile = (file_type == RocksDBFileType::kLogFile);

  // Validate options
  auto st = CheckOption(file_opts);
  if (!st.ok()) {
    return st;
  }

  if (sstfile || manifest || identity) {
    if (cloud_env_options_.keep_local_sst_files ||
        cloud_env_options_.hasSstFileCache() || !sstfile) {
      // Read from local storage and then from cloud storage.
      st = base_fs_->NewRandomAccessFile(fname, file_opts, result, dbg);

      // Found in local storage. Update LRU cache.
      // There is a loose coupling between the sst_file_cache and the files on
      // local storage. The sst_file_cache is only used for accounting of sst
      // files. We do not keep a reference to the LRU cache handle when the sst
      // file remains open by the db. If the LRU policy causes the file to be
      // evicted, it will be deleted from local storage, but because the db
      // already has an open file handle to it, it can continue to occupy local
      // storage space until the time the db decides to close the sst file.
      if (sstfile && st.ok()) {
        FileCacheAccess(fname);
      }

      if (!st.ok() && !base_fs_->FileExists(fname, IOOptions(), dbg).IsNotFound()) {
        // if status is not OK, but file does exist locally, something is wrong
        return st;
      }

      if (!st.ok()) {
        // copy the file to the local storage
        st = GetCloudObject(fname);
        if (st.ok()) {
          // we successfully copied the file, try opening it locally now
          st = base_fs_->NewRandomAccessFile(fname, file_opts, result, dbg);
        }
        // Update the size of our local sst file cache
        if (st.ok() && sstfile && cloud_env_options_.hasSstFileCache()) {
          uint64_t local_size;
          auto statx = base_fs_->GetFileSize(fname, IOOptions(), &local_size, dbg);
          if (statx.ok()) {
            FileCacheInsert(fname, local_size);
          }
        }
      }
      // If we are being paranoid, then we validate that our file size is
      // the same as in cloud storage.
      if (st.ok() && sstfile && cloud_env_options_.validate_filesize) {
        uint64_t remote_size = 0;
        uint64_t local_size = 0;
        auto stax = base_fs_->GetFileSize(fname, IOOptions(), &local_size, dbg);
        if (!stax.ok()) {
          return stax;
        }
        stax = IOStatus::NotFound();
        if (HasDestBucket()) {
          stax = GetCloudObjectSize(fname, &remote_size);
        }
        if (stax.IsNotFound() && !HasDestBucket()) {
          // It is legal for file to not be present in storage provider if
          // destination bucket is not set.
        } else if (!stax.ok() || remote_size != local_size) {
          std::string msg = std::string("[") + Name() + "] HeadObject src " +
                            fname + " local size " +
                            std::to_string(local_size) + " cloud size " +
                            std::to_string(remote_size) + " " + stax.ToString();
          Log(InfoLogLevel::ERROR_LEVEL, info_log_, "%s", msg.c_str());
          return IOStatus::IOError(msg);
        }
      }
    } else {
      // Only execute this code path if files are not cached locally
      std::unique_ptr<FSCloudStorageReadableFile> file;
      st = NewCloudReadableFile(fname, file_opts, &file, dbg);
      if (st.ok()) {
        result->reset(file.release());
      }
    }
    Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
        "[%s] NewRandomAccessFile file %s %s", Name(), fname.c_str(),
        st.ToString().c_str());
    return st;

  } else if (logfile && !cloud_env_options_.keep_local_log_files) {
    // read from LogController
    //////////////////////////////////////////////////////
    // TODO(estalgo): Update CloudLogController interface
    #if 0
    st = cloud_env_options_.cloud_log_controller->NewRandomAccessFile(
        fname, result, options);
    return st;
    #endif
  }

  // This is neither a sst file or a log file. Read from default env.
  return base_fs_->NewRandomAccessFile(fname, file_opts, result, dbg);
}

// create a new file for writing
IOStatus CloudFileSystemImpl::NewWritableFile(const std::string& logical_fname, const FileOptions& file_opts,
                                     std::unique_ptr<FSWritableFile>* result,
                                     IODebugContext* dbg) {
  result->reset();

  auto fname = RemapFilename(logical_fname);
  auto file_type = GetFileType(fname);
  bool sstfile = (file_type == RocksDBFileType::kSstFile),
       manifest = (file_type == RocksDBFileType::kManifestFile),
       identity = (file_type == RocksDBFileType::kIdentityFile),
       logfile = (file_type == RocksDBFileType::kLogFile);

  IOStatus s;

  if (HasDestBucket() && (sstfile || identity || manifest)) {
    std::unique_ptr<FSCloudStorageWritableFile> f;
    s = GetStorageProvider()->NewFSCloudWritableFile(fname, GetDestBucketName(),
                                               destname(fname),  file_opts, &f, dbg);
    if (!s.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[%s] NewWritableFile fails while NewCloudWritableFile, src %s %s",
          Name(), fname.c_str(), s.ToString().c_str());
      return s;
    }
    s = f->status();
    if (!s.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[%s] NewWritableFile fails; unexpected WritableFile Status, src %s "
          "%s",
          Name(), fname.c_str(), s.ToString().c_str());
      return s;
    }
    result->reset(f.release());
  } else if (logfile && !cloud_env_options_.keep_local_log_files) {
    //////////////////////////////////////////////////////
    // TODO(estalgo): Update CloudLogController interface
    #if 0
    std::unique_ptr<CloudLogWritableFile> f(
        cloud_env_options_.cloud_log_controller->CreateWritableFile(fname,
                                                                   options));
    if (!f || !f->status().ok()) {
      std::string msg = std::string("[") + Name() + "] NewWritableFile";
      s = Status::IOError(msg, fname.c_str());
      Log(InfoLogLevel::ERROR_LEVEL, info_log_, "%s src %s %s", msg.c_str(),
          fname.c_str(), s.ToString().c_str());
      return s;
    }
    result->reset(f.release());
    #endif
  } else {
    s = base_fs_->NewWritableFile(fname, file_opts, result, dbg);
  }
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[%s] NewWritableFile src %s %s",
      Name(), fname.c_str(), s.ToString().c_str());
  return s;
}

IOStatus CloudFileSystemImpl::ReopenWritableFile(const std::string& fname, const FileOptions& file_opts,
                                        std::unique_ptr<FSWritableFile>* result,
                                        IODebugContext* dbg) {
  // This is not accurately correct because there is no wasy way to open
  // an provider file in append mode. We still need to support this because
  // rocksdb's ExternalSstFileIngestionJob invokes this api to reopen
  // a pre-created file to flush/sync it.
  return base_fs_->ReopenWritableFile(fname, file_opts, result, dbg);
}

//
// Check if the specified filename exists.
//
IOStatus CloudFileSystemImpl::FileExists(const std::string& logical_fname, const IOOptions& options,
                                                                       IODebugContext* dbg) {
  IOStatus st;

  auto fname = RemapFilename(logical_fname);
  auto file_type = GetFileType(fname);
  bool sstfile = (file_type == RocksDBFileType::kSstFile),
       manifest = (file_type == RocksDBFileType::kManifestFile),
       identity = (file_type == RocksDBFileType::kIdentityFile),
       logfile = (file_type == RocksDBFileType::kLogFile);

  if (sstfile || manifest || identity) {
    // We read first from local storage and then from cloud storage.
    st = base_fs_->FileExists(fname, options, dbg);
    if (st.IsNotFound()) {
      st = ExistsCloudObject(fname);
    }
  } else if (logfile && !cloud_env_options_.keep_local_log_files) {
    // read from controller
    //////////////////////////////////////////////////////
    // TODO(estalgo): Update CloudLogController interface
    #if 0
    st = cloud_env_options_.cloud_log_controller->FileExists(fname);
    #endif
  } else {
    st = base_fs_->FileExists(fname, options, dbg);
  }
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[%s] FileExists path '%s' %s",
      Name(), fname.c_str(), st.ToString().c_str());
  return st;
}

IOStatus CloudFileSystemImpl::GetChildren(const std::string& path, const IOOptions& options,
                                          std::vector<std::string>* result, IODebugContext* dbg) {
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[%s] GetChildren path '%s' ",
      Name(), path.c_str());
  result->clear();

  IOStatus st;
  if (!cloud_env_options_.skip_cloud_files_in_getchildren) {
    // Fetch the list of children from the cloud
    st = ListCloudObjects(path, result);
    if (!st.ok()) {
      return st;
    }
  }

  // fetch all files that exist in the local posix directory
  std::vector<std::string> local_files;
  st = base_fs_->GetChildren(path, options, &local_files, dbg);
  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[%s] GetChildren %s error on local dir", Name(), path.c_str());
    return st;
  }


  *result = std::move(local_files);

  // Remove all results that are not supposed to be visible.
  result->erase(
      std::remove_if(result->begin(), result->end(),
                     [&](const std::string& f) {
                       auto noepoch = RemoveEpoch(f);
                       if (!IsSstFile(noepoch) && !IsManifestFile(noepoch)) {
                         return false;
                       }
                       return RemapFilename(noepoch) != f;
                     }),
      result->end());

  // Remove the epoch, remap into RocksDB's domain
  for (size_t i = 0; i < result->size(); ++i) {
    auto noepoch = RemoveEpoch(result->at(i));
    if (IsSstFile(noepoch) || IsManifestFile(noepoch)) {
      // remap sst and manifest files
      result->at(i) = noepoch;
    }
  }
  // remove duplicates
  std::sort(result->begin(), result->end());
  result->erase(std::unique(result->begin(), result->end()), result->end());

  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[%s] GetChildren %s successfully returned %" ROCKSDB_PRIszt " files",
      Name(), path.c_str(), result->size());
  return IOStatus::OK();
}

IOStatus CloudFileSystemImpl::GetFileSize(const std::string& logical_fname, const IOOptions& options,
                                          uint64_t* size, IODebugContext* dbg) {
  *size = 0L;

  auto fname = RemapFilename(logical_fname);
  auto file_type = GetFileType(fname);
  bool sstfile = (file_type == RocksDBFileType::kSstFile),
       logfile = (file_type == RocksDBFileType::kLogFile);

  IOStatus st;
  if (sstfile) {
    if (base_fs_->FileExists(fname, options, dbg).ok()) {
      st = base_fs_->GetFileSize(fname, options, size, dbg);
    } else {
      st = GetCloudObjectSize(fname, size);
    }
  } else if (logfile && !cloud_env_options_.keep_local_log_files) {
    ///////////////////
    ///  TODO(estalgo): Update CloudLogController interface
    #if 0
    st = cloud_env_options_.cloud_log_controller->GetFileSize(fname, size);
    #endif
  } else {
    st = base_fs_->GetFileSize(fname, options, size, dbg);
  }
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[%s] GetFileSize src '%s' %s %" PRIu64, Name(), fname.c_str(),
      st.ToString().c_str(), *size);
  return st;
}

IOStatus CloudFileSystemImpl::GetFileModificationTime(const std::string& logical_fname,
                                                     const IOOptions& options,
                                                     uint64_t* file_mtime,
                                                     IODebugContext* dbg) {
  *file_mtime = 0;

  auto fname = RemapFilename(logical_fname);
  auto file_type = GetFileType(fname);
  bool sstfile = (file_type == RocksDBFileType::kSstFile),
       logfile = (file_type == RocksDBFileType::kLogFile);

  IOStatus st;
  if (sstfile) {
    if (base_fs_->FileExists(fname, options, dbg).ok()) {
      st = base_fs_->GetFileModificationTime(fname, options, file_mtime, dbg);
    } else {
      st = GetCloudObjectModificationTime(fname, file_mtime);
    }
  } else if (logfile && !cloud_env_options_.keep_local_log_files) {
    //////////////////////////////////////////////////////
    // TODO(estalgo): Update CloudLogController interface
    #if 0
    st = cloud_env_options_.cloud_log_controller->GetFileModificationTime(fname,
                                                                         time);
    #endif
  } else {
    st = base_fs_->GetFileModificationTime(fname, options, file_mtime, dbg);
  }
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[%s] GetFileModificationTime src '%s' %s", Name(), fname.c_str(),
      st.ToString().c_str());
  return st;
}

// The rename may not be atomic. Some cloud vendords do not support renaming
// natively. Copy file to a new object and then delete original object.
IOStatus CloudFileSystemImpl::RenameFile(const std::string& logical_src,
                                         const std::string& logical_target,
                                         const IOOptions& options,
                                                             IODebugContext* dbg) {
  auto src = RemapFilename(logical_src);
  auto target = RemapFilename(logical_target);
  // Get file type of target
  auto file_type = GetFileType(target);
  bool sstfile = (file_type == RocksDBFileType::kSstFile),
       manifest = (file_type == RocksDBFileType::kManifestFile),
       identity = (file_type == RocksDBFileType::kIdentityFile),
       logfile = (file_type == RocksDBFileType::kLogFile);

  // Rename should never be called on sst files.
  if (sstfile) {
    Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
        "[%s] RenameFile source sstfile %s %s is not supported", Name(),
        src.c_str(), target.c_str());
    assert(0);
    return IOStatus::NotSupported(Slice(src), Slice(target));
  } else if (logfile) {
    // Rename should never be called on log files as well
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[%s] RenameFile source logfile %s %s is not supported", Name(),
        src.c_str(), target.c_str());
    assert(0);
    return IOStatus::NotSupported(Slice(src), Slice(target));
  } else if (manifest) {
    // Rename should never be called on manifest files as well
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[%s] RenameFile source manifest %s %s is not supported", Name(),
        src.c_str(), target.c_str());
    assert(0);
    return IOStatus::NotSupported(Slice(src), Slice(target));

  } else if (!identity || !HasDestBucket()) {
    return base_fs_->RenameFile(src, target, options, dbg);
  }
  // Only ID file should come here
  assert(identity);
  assert(HasDestBucket());
  assert(basename(target) == "IDENTITY");

  // Save Identity to Cloud
  auto st = SaveIdentityToCloud(src, destname(target));

  // Do the rename on local filesystem too
  if (st.ok()) {
    st = base_fs_->RenameFile(src, target, options, dbg);
  }
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[%s] RenameFile src %s target %s: %s", Name(), src.c_str(),
      target.c_str(), st.ToString().c_str());
  return st;
}

IOStatus CloudFileSystemImpl::LinkFile(const std::string& src,
                                       const std::string& target, const IOOptions& options, IODebugContext* dbg) {
  // We only know how to link file if both src and dest buckets are empty
  if (HasDestBucket() || HasSrcBucket()) {
    return IOStatus::NotSupported();
  }
  auto src_remapped = RemapFilename(src);
  auto target_remapped = RemapFilename(target);
  return base_fs_->LinkFile(src_remapped, target_remapped, options, dbg);
}

class CloudDirectory : public FSDirectory {
 public:
  explicit CloudDirectory(FileSystem* base_fs, const std::string& name, const IOOptions& options, IODebugContext* dbg) {
    status_ = base_fs->NewDirectory(name, options, &posixDir_, dbg);
  }

  ~CloudDirectory() {}

  IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) override {
    if (!status_.ok()) {
      return status_;
    }
    return posixDir_->Fsync(options, dbg);
  }

  IOStatus status() { return status_; }

 private:
  IOStatus status_;
  std::unique_ptr<FSDirectory> posixDir_;
};

//  Returns success only if the directory-bucket exists in the
//  StorageProvider and the posixEnv local directory exists as well.
IOStatus CloudFileSystemImpl::NewDirectory(const std::string& name,const IOOptions& io_opts,
                                         std::unique_ptr<FSDirectory>* result, IODebugContext* dbg) {
  result->reset(nullptr);

  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[%s] NewDirectory name '%s'",
      Name(), name.c_str());

  // create new object.
  std::unique_ptr<CloudDirectory> d(new CloudDirectory(base_fs_.get(), name, io_opts, dbg));

  // Check if the path exists in local dir
  if (!d->status().ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[%s] NewDirectory name %s unable to create local dir", Name(),
        name.c_str());
    return d->status();
  }
  result->reset(d.release());
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[%s] NewDirectory name %s ok",
      Name(), name.c_str());
  return IOStatus::OK();
}

// Cloud storage providers have no concepts of directories,
// so we just have to forward the request to the base_env_
IOStatus CloudFileSystemImpl::CreateDir(const std::string& dirname, const IOOptions& options, IODebugContext* dbg) {
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[%s] CreateDir dir '%s'", Name(),
      dirname.c_str());

  // create local dir
  auto st = base_fs_->CreateDir(dirname, options, dbg);

  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[%s] CreateDir dir %s %s", Name(),
      dirname.c_str(), st.ToString().c_str());
  return st;
};

// Cloud storage providers have no concepts of directories,
// so we just have to forward the request to the base_env_
IOStatus CloudFileSystemImpl::CreateDirIfMissing(const std::string& dirname, const IOOptions& options, IODebugContext* dbg) {
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[%s] CreateDirIfMissing dir '%s'",
      Name(), dirname.c_str());

  // create directory in base_fs_
  auto st = base_fs_->CreateDirIfMissing(dirname, options, dbg);

  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[%s] CreateDirIfMissing created dir %s %s", Name(), dirname.c_str(),
      st.ToString().c_str());
  return st;
};

// Cloud storage providers have no concepts of directories,
// so we just have to forward the request to the base_env_
IOStatus CloudFileSystemImpl::DeleteDir(const std::string& dirname, const IOOptions& options, IODebugContext* dbg) {
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[%s] DeleteDir src '%s'", Name(),
      dirname.c_str());
  auto st = base_fs_->DeleteDir(dirname, options, dbg);
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[%s] DeleteDir dir %s %s", Name(),
      dirname.c_str(), st.ToString().c_str());
  return st;
};

IOStatus CloudFileSystemImpl::DeleteFile(const std::string& logical_fname, const IOOptions& options, IODebugContext* dbg) {
  auto fname = RemapFilename(logical_fname);
  auto file_type = GetFileType(fname);
  bool sstfile = (file_type == RocksDBFileType::kSstFile),
       manifest = (file_type == RocksDBFileType::kManifestFile),
       identity = (file_type == RocksDBFileType::kIdentityFile),
       logfile = (file_type == RocksDBFileType::kLogFile);

  if (manifest) {
    // We don't delete manifest files. The reason for this is that even though
    // RocksDB creates manifest with different names (like MANIFEST-00001,
    // MANIFEST-00008) we actually map all of them to the same filename
    // MANIFEST-[epoch].
    // When RocksDB wants to roll the MANIFEST (let's say from 1 to 8) it does
    // the following:
    // 1. Create a new MANIFEST-8
    // 2. Write everything into MANIFEST-8
    // 3. Sync MANIFEST-8
    // 4. Store "MANIFEST-8" in CURRENT file
    // 5. Delete MANIFEST-1
    //
    // What RocksDB cloud does behind the scenes (the numbers match the list
    // above):
    // 1. Create manifest file MANIFEST-[epoch].tmp
    // 2. Forward RocksDB writes to the file created in the first step
    // 3. Atomic rename from MANIFEST-[epoch].tmp to MANIFEST-[epoch]. The old
    // file with the same file name is overwritten.
    // 4. Nothing. Whatever the contents of CURRENT file, we don't care, we
    // always remap MANIFEST files to the correct with the latest epoch.
    // 5. Also nothing. There is no file to delete, because we have overwritten
    // it in the third step.
    return IOStatus::OK();
  }

  IOStatus st;
  // Delete from destination bucket and local dir
  if (sstfile || manifest || identity) {
    if (HasDestBucket()) {
      // add the remote file deletion to the queue
      st = DeleteCloudFileFromDest(basename(fname));
    }
    // delete from local, too. Ignore the result, though. The file might not be
    // there locally.
    base_fs_->DeleteFile(fname, options, dbg);

    // remove from sst_file_cache
    if (sstfile) {
      FileCacheErase(fname);
    }
  } else if (logfile && !cloud_env_options_.keep_local_log_files) {
    // read from Log Controller
    ////////////////////////////////////
    /// TODO(estalgo) Change log controller status return value
    #if 0
    st = cloud_env_options_.cloud_log_controller->status();
    #endif
    if (st.ok()) {
      // Log a Delete record to controller stream
      std::unique_ptr<CloudLogWritableFile> f(
          cloud_env_options_.cloud_log_controller->CreateWritableFile(
              fname, EnvOptions()));
      if (!f || !f->status().ok()) {
        std::string msg =
            "[" + std::string(cloud_env_options_.cloud_log_controller->Name()) +
            "] DeleteFile";
        st = IOStatus::IOError(msg, fname.c_str());
      } else {
        ////////////////////////////////////
        /// TODO(estalgo) Change log controller status return value
        #if 0
        st = f->LogDelete();
        #endif
      }
    }
  } else {
    st = base_fs_->DeleteFile(fname, options, dbg);
  }
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[%s] DeleteFile file %s %s",
      Name(), fname.c_str(), st.ToString().c_str());
  return st;
}

void CloudFileSystemImpl::RemoveFileFromDeletionQueue(const std::string& filename) {
  cloud_file_deletion_scheduler_->UnscheduleFileDeletion(filename);
}

void CloudFileSystemImpl::TEST_SetFileDeletionDelay(std::chrono::seconds delay) {
  cloud_file_deletion_scheduler_->TEST_SetFileDeletionDelay(delay);
}

IOStatus CloudFileSystemImpl::CopyLocalFileToDest(const std::string& local_name,
                                         const std::string& dest_name) {
  RemoveFileFromDeletionQueue(basename(local_name));
  return status_to_io_status(GetStorageProvider()->PutCloudObject(local_name, GetDestBucketName(),
                                                                  dest_name));
}

IOStatus CloudFileSystemImpl::DeleteCloudFileFromDest(const std::string& fname) {
  assert(HasDestBucket());
  auto base = basename(fname);
  auto path = GetDestObjectPath() + pathsep + base;
  auto bucket = GetDestBucketName();
  std::weak_ptr<Logger> info_log_wp = info_log_;
  std::weak_ptr<CloudStorageProvider> storage_provider_wp = GetStorageProvider();
  auto file_deletion_runnable =
      [path = std::move(path), bucket = std::move(bucket),
       info_log_wp = std::move(info_log_wp),
       storage_provider_wp = std::move(storage_provider_wp)]() {
        auto storage_provider = storage_provider_wp.lock();
        auto info_log = info_log_wp.lock();
        if (!storage_provider || !info_log) {
          return;
        }
        auto st = storage_provider->DeleteCloudObject(bucket, path);
        if (!st.ok() && !st.IsNotFound()) {
          Log(InfoLogLevel::ERROR_LEVEL, info_log,
              "[CloudEnvImpl] DeleteFile file %s error %s", path.c_str(),
              st.ToString().c_str());
        }
      };
  return status_to_io_status(cloud_file_deletion_scheduler_->ScheduleFileDeletion(
                                                                                  base, std::move(file_deletion_runnable)));
}

void CloudFileSystemImpl::StopPurger() {
  {
    std::lock_guard<std::mutex> lk(purger_lock_);
    purger_is_running_ = false;
    purger_cv_.notify_one();
  }

  // wait for the purger to stop
  if (purge_thread_.joinable()) {
    purge_thread_.join();
  }
}

Status CloudFileSystemImpl::LoadLocalCloudManifest(const std::string& dbname) {
  return LoadLocalCloudManifest(dbname, cloud_env_options_.cookie_on_open);
}

Status CloudFileSystemImpl::LoadLocalCloudManifest(const std::string& dbname, const std::string& cookie) {
  if (cloud_manifest_) {
    cloud_manifest_.reset();
  }
  return CloudFileSystemImpl::LoadLocalCloudManifest(
      dbname, GetBaseEnv(), cookie, &cloud_manifest_);
}

Status CloudFileSystemImpl::LoadLocalCloudManifest(
    const std::string& dbname, Env* base_env, const std::string& cookie,
    std::unique_ptr<CloudManifest>* cloud_manifest) {
  std::unique_ptr<SequentialFileReader> reader;
  auto cloud_manifest_file_name = MakeCloudManifestFile(dbname, cookie);
  Status s = SequentialFileReader::Create(base_env->GetFileSystem(),
                                          cloud_manifest_file_name,
                                          FileOptions(), &reader, nullptr);
  if (s.ok()) {
    s = CloudManifest::LoadFromLog(std::move(reader), cloud_manifest);
  }
  return s;
}

std::string RemapFilenameWithCloudManifest(const std::string& logical_path,
                                           CloudManifest* cloud_manifest) {
  auto file_name = basename(logical_path);
  uint64_t fileNumber;
  FileType type;
  WalFileType walType;
  if (file_name == "MANIFEST") {
    type = kDescriptorFile;
  } else {
    bool ok = ParseFileName(file_name, &fileNumber, &type, &walType);
    if (!ok) {
      return logical_path;
    }
  }
  std::string epoch;
  switch (type) {
    case kTableFile:
      // We should not be accessing sst files before CLOUDMANIFEST is loaded
      assert(cloud_manifest);
      epoch = cloud_manifest->GetEpoch(fileNumber);
      break;
    case kDescriptorFile:
      // We should not be accessing MANIFEST files before CLOUDMANIFEST is
      // loaded
      // Even though logical file might say MANIFEST-000001, we cut the number
      // suffix and store MANIFEST-[epoch] in the cloud and locally.
      file_name = "MANIFEST";
      assert(cloud_manifest);
      epoch = cloud_manifest->GetCurrentEpoch();
      break;
    default:
      return logical_path;
  };
  auto dir = dirname(logical_path);
  return dir + (dir.empty() ? "" : "/") + file_name +
         (epoch.empty() ? "" : ("-" + epoch));
}

std::string CloudFileSystemImpl::RemapFilename(const std::string& logical_path) const {
  if (UNLIKELY(test_disable_cloud_manifest_)) {
    return logical_path;
  }
  return RemapFilenameWithCloudManifest(logical_path, cloud_manifest_.get());
}

Status CloudFileSystemImpl::DeleteCloudInvisibleFiles(
    const std::vector<std::string>& active_cookies) {
  assert(HasDestBucket());
  std::vector<std::string> pathnames;
  auto s = GetStorageProvider()->ListCloudObjects(GetDestBucketName(),
                                             GetDestObjectPath(), &pathnames);
  if (!s.ok()) {
    Log(InfoLogLevel::WARN_LEVEL, info_log_,
        "Files in cloud are not scheduled to be deleted since listing cloud "
        "object fails: %s",
        s.ToString().c_str());
    return s;
  }

  for (auto& fname : pathnames) {
    if (IsFileInvisible(active_cookies, fname)) {
      // Ignore returned status on purpose.
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "DeleteCloudInvisibleFiles deleting %s from destination bucket",
          fname.c_str());
      DeleteCloudFileFromDest(fname);
    }
  }
  return s;
}

Status CloudFileSystemImpl::DeleteLocalInvisibleFiles(
    const std::string& dbname, const std::vector<std::string>& active_cookies) {
  std::vector<std::string> children;
  auto s = GetBaseEnv()->GetChildren(dbname, &children);
  TEST_SYNC_POINT_CALLBACK("CloudFileSystemImpl::DeleteLocalInvisibleFiles:AfterListLocalFiles", &s);
  if (!s.ok()) {
    Log(InfoLogLevel::WARN_LEVEL, info_log_,
        "Local files are not deleted since listing local files fails: %s",
        s.ToString().c_str());
    return s;
  }
  for (auto& fname : children) {
    if (IsFileInvisible(active_cookies, fname)) {
        // Ignore returned status on purpose.
        Log(InfoLogLevel::INFO_LEVEL, info_log_,
            "DeleteLocalInvisibleFiles deleting file %s from local dir",
            fname.c_str());
        GetBaseEnv()->DeleteFile(dbname + "/" + fname);
    }
  }
  return s;
}

bool CloudFileSystemImpl::IsFileInvisible(
    const std::vector<std::string>& active_cookies,
    const std::string& fname) const {
  if (IsCloudManifestFile(fname)) {
    auto fname_cookie = GetCookie(fname);

    // empty cookie CM file is never deleted
    // TODO(wei): we can remove this assumption once L/F is fully rolled out
    if (fname_cookie.empty()) {
      return false;
    }

    bool is_active = false;
    for (auto& c: active_cookies) {
      if (c == fname_cookie) {
        is_active = true;
        break;
      }
    }

    return !is_active;
  } else {
    auto noepoch = RemoveEpoch(fname);
    if ((IsSstFile(noepoch) || IsManifestFile(noepoch)) &&
        (RemapFilename(noepoch) != fname)) {
      return true;
    }
  }
  return false;
}

void CloudFileSystemImpl::TEST_InitEmptyCloudManifest() {
  CloudManifest::CreateForEmptyDatabase("", &cloud_manifest_);
}

Status CloudFileSystemImpl::writeCloudManifest(CloudManifest* manifest,
                                        const std::string& fname) const {
  Env* local_env = GetBaseEnv();
  // Write to tmp file and atomically rename later. This helps if we crash
  // mid-write :)
  auto tmp_fname = fname + ".tmp";
  std::unique_ptr<WritableFileWriter> writer;
  Status s = WritableFileWriter::Create(local_env->GetFileSystem(), tmp_fname,
                                        FileOptions(), &writer, nullptr);
  if (s.ok()) {
    s = manifest->WriteToLog(std::move(writer));
  }
  if (s.ok()) {
    s = local_env->RenameFile(tmp_fname, fname);
  }
  return s;
}

// we map a longer string given by env->GenerateUniqueId() into 16-byte string
std::string CloudFileSystemImpl::generateNewEpochId() {
  auto uniqueId = base_env_->GenerateUniqueId();
  size_t split = uniqueId.size() / 2;
  auto low = uniqueId.substr(0, split);
  auto hi = uniqueId.substr(split);
  uint64_t hash =
      XXH32(low.data(), static_cast<int>(low.size()), 0) +
      (static_cast<uint64_t>(XXH32(hi.data(), static_cast<int>(hi.size()), 0))
       << 32);
  char buf[17];
  snprintf(buf, sizeof buf, "%0" PRIx64, hash);
  return buf;
}

// Check if options are compatible with the cloud storage system
IOStatus CloudFileSystemImpl::CheckOption(const FileOptions& options) {
  // Cannot mmap files that reside on cloud storage, unless the file is also
  // local
  if (options.use_mmap_reads && !cloud_env_options_.keep_local_sst_files) {
    std::string msg = "Mmap only if keep_local_sst_files is set";
    return IOStatus::InvalidArgument(msg);
  }
  if (cloud_env_options_.hasSstFileCache() &&
      cloud_env_options_.keep_local_sst_files) {
    std::string msg =
        "Only one of sst_file_cache or keep_local_sst_files can be set";
    return IOStatus::InvalidArgument(msg);
  }
  return IOStatus::OK();
}

//
// prepends the configured src object path name
//
std::string CloudFileSystemImpl::srcname(const std::string& localname) {
  assert(cloud_env_options_.src_bucket.IsValid());
  return cloud_env_options_.src_bucket.GetObjectPath() + "/" +
         basename(localname);
}

//
// prepends the configured dest object path name
//
std::string CloudFileSystemImpl::destname(const std::string& localname) {
  assert(cloud_env_options_.dest_bucket.IsValid());
  return cloud_env_options_.dest_bucket.GetObjectPath() + "/" +
         basename(localname);
}

Status CloudFileSystemImpl::MigrateFromPureRocksDB(const std::string& local_dbname) {
  std::unique_ptr<CloudManifest> manifest;
  CloudManifest::CreateForEmptyDatabase("", &manifest);
  auto st = writeCloudManifest(manifest.get(), CloudManifestFile(local_dbname));
  if (!st.ok()) {
    return st;
  }
  st = LoadLocalCloudManifest(local_dbname);
  if (!st.ok()) {
    return st;
  }

  std::string manifest_filename;
  Env* local_env = GetBaseEnv();
  st = local_env->FileExists(CurrentFileName(local_dbname));
  if (st.IsNotFound()) {
    // No need to migrate
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[cloud_env_impl] MigrateFromPureRocksDB: No need to migrate %s",
        CurrentFileName(local_dbname).c_str());
    return Status::OK();
  }
  if (!st.ok()) {
    return st;
  }
  st = ReadFileToString(local_env, CurrentFileName(local_dbname),
                        &manifest_filename);
  if (!st.ok()) {
    return st;
  }
  // Note: This rename is important for migration. If we are just starting on
  // an old database, our local MANIFEST filename will be something like
  // MANIFEST-00001 instead of MANIFEST. If we don't do the rename we'll
  // download MANIFEST file from the cloud, which might not be what we want do
  // to (especially for databases which don't have a destination bucket
  // specified).
  manifest_filename = local_dbname + "/" + rtrim_if(manifest_filename, '\n');
  if (local_env->FileExists(manifest_filename).IsNotFound()) {
    // manifest doesn't exist, shrug
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[cloud_env_impl] MigrateFromPureRocksDB: Manifest %s does not exist",
        manifest_filename.c_str());
    return Status::OK();
  }
  st = local_env->RenameFile(manifest_filename, local_dbname + "/MANIFEST");
  if (st.ok() && cloud_env_options_.roll_cloud_manifest_on_open) {
      st = RollNewEpoch(local_dbname);
  }

  return st;
}

Status CloudFileSystemImpl::PreloadCloudManifest(const std::string& local_dbname) {
  Env* local_env = GetBaseEnv();
  local_env->CreateDirIfMissing(local_dbname);
  // Init cloud manifest
  auto st = FetchCloudManifest(local_dbname);
  if (st.ok()) {
    // Inits CloudFileSystemImpl::cloud_manifest_, which will enable us to read files
    // from the cloud
    st = LoadLocalCloudManifest(local_dbname);
  }
  return st;
}

Status CloudFileSystemImpl::LoadCloudManifest(const std::string& local_dbname,
                                       bool read_only) {
  // Init cloud manifest
  auto st = FetchCloudManifest(local_dbname);
  if (st.ok()) {
    // Inits CloudFileSystemImpl::cloud_manifest_, which will enable us to read files
    // from the cloud
    st = LoadLocalCloudManifest(local_dbname);
  }

  if (st.ok() && cloud_env_options_.resync_on_open &&
      cloud_env_options_.resync_manifest_on_open) {
    auto epoch = cloud_manifest_->GetCurrentEpoch();
    st = FetchManifest(local_dbname, epoch);
    if (st.IsNotFound()) {
      // We always upload MANIFEST first before uploading CLOUDMANIFEST. So it's
      // not expected to have CLOUDMANIFEST in s3 which points to MANIFEST file
      // that doesn't exist.
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[CloudEnvImpl] CLOUDMANIFEST-%s points to MANIFEST-%s that doesn't "
          "exist in s3",
          cloud_env_options_.cookie_on_open.c_str(), epoch.c_str());
      st = Status::Corruption(
          "CLOUDMANIFEST points to MANIFEST that doesn't exist in s3");
    }
  }

  // Do the cleanup, but don't fail if the cleanup fails.
  // We only cleanup files which don't belong to cookie_on_open. Also, we do it
  // before rolling the epoch, so that newly generated CM/M files won't be
  // cleaned up.
  if (st.ok() && !read_only) {
    std::vector<std::string> active_cookies{
        cloud_env_options_.cookie_on_open, cloud_env_options_.new_cookie_on_open};
    st = DeleteLocalInvisibleFiles(local_dbname, active_cookies);
    if (st.ok() && cloud_env_options_.delete_cloud_invisible_files_on_open &&
        HasDestBucket()) {
      st = DeleteCloudInvisibleFiles(active_cookies);
    }
    if (!st.ok()) {
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "Failed to delete invisible files: %s", st.ToString().c_str());
      // Ignore the fail
      st = Status::OK();
    }
  }

  if (st.ok() && cloud_env_options_.roll_cloud_manifest_on_open) {
    // Rolls the new epoch in CLOUDMANIFEST (only for existing databases)
    st = RollNewEpoch(local_dbname);
    if (st.IsNotFound()) {
      st = Status::Corruption(
          "CLOUDMANIFEST points to MANIFEST that doesn't exist");
    }
  }
  if (!st.ok()) {
    cloud_manifest_.reset();
    return st;
  }

  IODebugContext* dbg = nullptr;
  IOOptions io_opts;
  if (FileExists(CurrentFileName(local_dbname), io_opts, dbg).IsNotFound()) {
    // Create dummy CURRENT file to point to the dummy manifest (cloud env
    // will remap the filename appropriately, this is just to fool the
    // underyling RocksDB)
    std::unique_ptr<FSWritableFile> destfile;
    st =
      NewWritableFile(CurrentFileName(local_dbname), FileOptions(), &destfile, dbg);
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "Unable to create local CURRENT file to %s %s",
          CurrentFileName(local_dbname).c_str(), st.ToString().c_str());
      return st;
    }
    Log(InfoLogLevel::INFO_LEVEL, info_log_, "Creating a dummy CURRENT file.");
    st = destfile->Append(Slice("MANIFEST-000001\n"), io_opts, dbg);
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "Unable to write local CURRENT file to %s %s",
          CurrentFileName(local_dbname).c_str(), st.ToString().c_str());
      return st;
    }
  }

  return st;
}

Status CloudFileSystemImpl::FetchCloudManifest(const std::string& local_dbname) {
  return FetchCloudManifest(local_dbname, cloud_env_options_.cookie_on_open);
}

Status CloudFileSystemImpl::FetchCloudManifest(const std::string& local_dbname, const std::string& cookie) {
  std::string cloudmanifest = MakeCloudManifestFile(local_dbname, cookie);
  // TODO(wei): following check is to make sure we maintain the same behavior
  // as before. Once we double check every service has right resync_on_open set,
  // we should remove.
  bool resync_on_open = cloud_env_options_.resync_on_open;
  if (SrcMatchesDest() && !resync_on_open) {
    Log(InfoLogLevel::WARN_LEVEL, info_log_,
        "[cloud_env_impl] FetchCloudManifest: Src bucket matches dest bucket "
        "but resync_on_open not enabled. Force enabling it for now");
    resync_on_open = true;
  }

  // If resync_on_open is false and we have a local cloud manifest, do nothing.
  if (!resync_on_open && GetBaseEnv()->FileExists(cloudmanifest).ok()) {

    // nothing to do here, we have our cloud manifest
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[cloud_env_impl] FetchCloudManifest: Nothing to do, %s exists and "
        "resync_on_open is false",
        cloudmanifest.c_str());
    return Status::OK();
  }
  // first try to get cloudmanifest from dest
  if (HasDestBucket()) {
    Status st = GetStorageProvider()->GetCloudObject(
        GetDestBucketName(), MakeCloudManifestFile(GetDestObjectPath(), cookie),
        cloudmanifest);
    if (!st.ok() && !st.IsNotFound()) {
      // something went wrong, bail out
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "[cloud_env_impl] FetchCloudManifest: Failed to fetch "
          " cloud manifest %s from dest %s",
          cloudmanifest.c_str(), GetDestBucketName().c_str());
      return st;
    }
    if (st.ok()) {
      // found it!
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "[cloud_env_impl] FetchCloudManifest: Fetched"
          " cloud manifest %s from dest %s",
          cloudmanifest.c_str(), GetDestBucketName().c_str());
      return st;
    }
  }
  // we couldn't get cloud manifest from dest, need to try from src?
  if (HasSrcBucket() && !SrcMatchesDest()) {
    Status st = GetStorageProvider()->GetCloudObject(
        GetSrcBucketName(), MakeCloudManifestFile(GetSrcObjectPath(), cookie),
        cloudmanifest);
    if (!st.ok() && !st.IsNotFound()) {
      // something went wrong, bail out
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "[cloud_env_impl] FetchCloudManifest: Failed to fetch "
          " cloud manifest %s from src %s",
          cloudmanifest.c_str(), GetSrcBucketName().c_str());
      return st;
    }
    if (st.ok()) {
      // found it!
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "[cloud_env_impl] FetchCloudManifest: Fetched"
          " cloud manifest %s from src %s",
          cloudmanifest.c_str(), GetSrcBucketName().c_str());
      return st;
    }
  }
  Log(InfoLogLevel::INFO_LEVEL, info_log_,
      "[cloud_env_impl] FetchCloudManifest: No cloud manifest");
  return Status::NotFound();
}

Status CloudFileSystemImpl::FetchManifest(const std::string& local_dbname,
                                   const std::string& epoch) {
  auto local_manifest_file = ManifestFileWithEpoch(local_dbname, epoch);
  if (HasDestBucket()) {
    Status st = GetStorageProvider()->GetCloudObject(
        GetDestBucketName(), ManifestFileWithEpoch(GetDestObjectPath(), epoch),
        local_manifest_file);
    if (!st.ok() && !st.IsNotFound()) {
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "[cloud_env_impl] FetchManifest: Failed to fetch manifest %s from "
          "dest %s",
          local_manifest_file.c_str(), GetDestBucketName().c_str());
    }

    if (st.ok()) {
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "[cloud_env_impl] FetchManifest: Fetched manifest %s from dest: %s",
          local_manifest_file.c_str(), GetDestBucketName().c_str());
      return st;
    }
  }

  if (HasSrcBucket() && !SrcMatchesDest()) {
    Status st = GetStorageProvider()->GetCloudObject(
        GetSrcBucketName(), ManifestFileWithEpoch(GetSrcObjectPath(), epoch),
        local_manifest_file);
    if (!st.ok() && !st.IsNotFound()) {
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "[cloud_env_impl] FetchManifest: Failed to fetch manifest %s from "
          "src %s",
          local_manifest_file.c_str(), GetSrcBucketName().c_str());
    }
    if (st.ok()) {
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "[cloud_env_impl] FetchManifest: Fetched manifest %s from "
          "src %s",
          local_manifest_file.c_str(), GetSrcBucketName().c_str());

      return st;
    }
  }

  Log(InfoLogLevel::INFO_LEVEL, info_log_,
      "[cloud_env_impl] FetchManifest: not manifest");
  return Status::NotFound();
}

Status CloudFileSystemImpl::CreateCloudManifest(const std::string& local_dbname) {
  return CreateCloudManifest(local_dbname, cloud_env_options_.cookie_on_open);
}

Status CloudFileSystemImpl::CreateCloudManifest(const std::string& local_dbname, const std::string& cookie) {
  // No cloud manifest, create an empty one
  std::unique_ptr<CloudManifest> manifest;
  CloudManifest::CreateForEmptyDatabase(generateNewEpochId(), &manifest);
  auto st = writeCloudManifest(manifest.get(), MakeCloudManifestFile(local_dbname, cookie));
  if (st.ok()) {
    st = LoadLocalCloudManifest(local_dbname, cookie);
  }
  return st;
}

// REQ: This is an existing database.
Status CloudFileSystemImpl::RollNewEpoch(const std::string& local_dbname) {
  assert(cloud_env_options_.roll_cloud_manifest_on_open);
  // Find next file number. We use dummy MANIFEST filename, which should get
  // remapped into the correct MANIFEST filename through CloudManifest.
  // After this call we should also have a local file named
  // MANIFEST-<current_epoch> (unless st.IsNotFound()).
  uint64_t maxFileNumber;
  auto st = ManifestReader::GetMaxFileNumberFromManifest(
      base_env_, local_dbname + "/MANIFEST-000001", &maxFileNumber);
  if (!st.ok()) {
    // uh oh
    return st;
  }
  // roll new epoch
  auto newEpoch = generateNewEpochId();
  // To make sure `RollNewEpoch` is backwards compatible, we don't change
  // the cookie when applying CM delta
  auto newCookie = cloud_env_options_.new_cookie_on_open;
  auto cloudManifestDelta = CloudManifestDelta{maxFileNumber, newEpoch};

  st = RollNewCookie(local_dbname, newCookie, cloudManifestDelta);
  if (st.ok()) {
    // Apply the delta to our in-memory state, too.
    bool updateApplied = true;
    st = ApplyCloudManifestDelta(cloudManifestDelta, &updateApplied);
    // We know for sure that <maxFileNumber, newEpoch> hasn't been applied
    // in current CLOUDMANFIEST yet since maxFileNumber >= filenumber in
    // CLOUDMANIFEST and epoch is generated randomly
    assert(updateApplied);
  }

  return st;
}

Status CloudFileSystemImpl::UploadManifest(
    const std::string& local_dbname, const std::string& epoch) const {
  if (!HasDestBucket()) {
    return Status::InvalidArgument(
        "Dest bucket has to be specified when uploading manifest files");
  }

  auto st = GetStorageProvider()->PutCloudObject(
      ManifestFileWithEpoch(local_dbname, epoch), GetDestBucketName(),
      ManifestFileWithEpoch(GetDestObjectPath(), epoch));

  TEST_SYNC_POINT_CALLBACK(
      "CloudFileSystemImpl::UploadManifest:AfterUploadManifest",
      &st);
  return st;
}

Status CloudFileSystemImpl::UploadCloudManifest(const std::string& local_dbname,
                                              const std::string& cookie) const {
  if (!HasDestBucket()) {
    return Status::InvalidArgument(
        "Dest bucket has to be specified when uploading CloudManifest files");
  }
   // upload the cloud manifest file corresponds to cookie (i.e., CLOUDMANIFEST-cookie)
  Status st = GetStorageProvider()->PutCloudObject(
      MakeCloudManifestFile(local_dbname, cookie), GetDestBucketName(),
      MakeCloudManifestFile(GetDestObjectPath(), cookie));
  if (!st.ok()) {
    return st;
  }

  return st;
}

size_t CloudFileSystemImpl::TEST_NumScheduledJobs() const {
  return scheduler_->TEST_NumScheduledJobs();
};

Status CloudFileSystemImpl::ApplyCloudManifestDelta(const CloudManifestDelta& delta,
                                             bool* delta_applied) {
  *delta_applied = cloud_manifest_->AddEpoch(delta.file_num, delta.epoch);
  return Status::OK();
}

Status CloudFileSystemImpl::RollNewCookie(const std::string& local_dbname,
                                   const std::string& cookie,
                                   const CloudManifestDelta& delta) const {
  auto newCloudManifest = cloud_manifest_->clone();
  Status st;
  std::string old_epoch = newCloudManifest->GetCurrentEpoch();
  if (!newCloudManifest->AddEpoch(delta.file_num, delta.epoch)) {
    return Status::InvalidArgument("Delta already applied in cloud manifest");
  }

  const auto& fs = GetBaseEnv()->GetFileSystem();
  Log(InfoLogLevel::INFO_LEVEL, info_log_,
      "Rolling new CLOUDMANIFEST from file number %lu, renaming MANIFEST-%s to "
      "MANIFEST-%s, new cookie: %s",
      delta.file_num, old_epoch.c_str(), delta.epoch.c_str(),
      cookie.c_str());
  // ManifestFileWithEpoch(local_dbname, oldEpoch) should exist locally.
  // We have to move our old manifest to the new filename.
  // However, we don't move here, we copy. If we moved and crashed immediately
  // after (before writing CLOUDMANIFEST), we'd corrupt our database. The old
  // MANIFEST file will be cleaned up in DeleteInvisibleFiles().
  st = CopyFile(fs.get(), ManifestFileWithEpoch(local_dbname, old_epoch),
                ManifestFileWithEpoch(local_dbname, delta.epoch), 0 /* size */,
                true /* use_fsync */, nullptr /* io_tracer */,
                Temperature::kUnknown);
  if (!st.ok()) {
    return st;
  }

  // TODO(igor): Compact cloud manifest by looking at live files in the database
  // and removing epochs that don't contain any live files.


  TEST_SYNC_POINT_CALLBACK(
      "CloudFileSystemImpl::RollNewCookie:AfterManifestCopy", &st);
  if (!st.ok()) {
    return st;
  }

  // Dump cloud_manifest into the CLOUDMANIFEST-cookie file
  st = writeCloudManifest(newCloudManifest.get(),
                          MakeCloudManifestFile(local_dbname, cookie));
  if (!st.ok()) {
    return st;
  }

  if (HasDestBucket()) {
    // We have to upload the manifest file first. Otherwise, if the process
    // crashed in the middle, we'll endup with a CLOUDMANIFEST file pointing to
    // MANIFEST file which doesn't exist in s3
    st = UploadManifest(local_dbname, delta.epoch);
    if (!st.ok()) {
      return st;
    }
    st = UploadCloudManifest(local_dbname, cookie);
    if (!st.ok()) {
      return st;
    }
  }
  return Status::OK();
}

  IOStatus CloudFileSystemImpl::LockFile(const std::string& /*fname*/, const IOOptions& /*options*/, FileLock** lock, IODebugContext* /*dbg*/) {
  // there isn's a very good way to atomically check and create cloud file
  *lock = nullptr;
  return IOStatus::OK();
}

IOStatus CloudFileSystemImpl::UnlockFile(FileLock* /*lock*/, const IOOptions& /*options*/, IODebugContext* /*dbg*/) { return IOStatus::OK(); }

std::string CloudFileSystemImpl::GetWALCacheDir() {
  return cloud_env_options_.cloud_log_controller->GetCacheDir();
}

Status CloudFileSystemImpl::PrepareOptions(const ConfigOptions& options) {
  Status status;
  if (!cloud_env_options_.cloud_log_controller &&
      !cloud_env_options_.keep_local_log_files) {
    if (cloud_env_options_.log_type == LogType::kLogKinesis) {
      status = CloudLogController::CreateFromString(
          options, CloudLogControllerImpl::kKinesis(),
          &cloud_env_options_.cloud_log_controller);
    } else if (cloud_env_options_.log_type == LogType::kLogKafka) {
      status = CloudLogController::CreateFromString(
          options, CloudLogControllerImpl::kKafka(),
          &cloud_env_options_.cloud_log_controller);
    } else {
      status = Status::NotSupported("Unsupported log controller type");
    }
    if (!status.ok()) {
      return status;
    }
  }

  status = CheckValidity();
  if (!status.ok()) {
    return status;
  }
  // start the purge thread only if there is a destination bucket
  if (cloud_env_options_.dest_bucket.IsValid() && cloud_env_options_.run_purger) {
    auto* cloud = this;
    purge_thread_ = std::thread([cloud] { cloud->Purger(); });
  }
  return status;
}

IOStatus CloudFileSystemImpl::ValidateOptions(const DBOptions& db_opts,
                                     const ColumnFamilyOptions& cf_opts) const {
  if (info_log_ == nullptr) {
    info_log_ = db_opts.info_log;
  }
  return CheckValidity();
}

Status CloudFileSystemImpl::CheckValidity() const {
  if (cloud_env_options_.src_bucket.GetBucketName().empty() !=
      cloud_env_options_.src_bucket.GetObjectPath().empty()) {
    return Status::InvalidArgument(
        "Must specify both src bucket name and path");
  } else if (cloud_env_options_.dest_bucket.GetBucketName().empty() !=
             cloud_env_options_.dest_bucket.GetObjectPath().empty()) {
    return Status::InvalidArgument(
        "Must specify both dest bucket name and path");
  } else if (!cloud_env_options_.storage_provider) {
    return Status::InvalidArgument(
        "Cloud environment requires a storage provider");
  } else if (!cloud_env_options_.keep_local_log_files &&
             !cloud_env_options_.cloud_log_controller) {
    return Status::InvalidArgument(
        "Log controller required for remote log files");
  } else {
    return Status::OK();
  }
}

std::string CloudFileSystemImpl::CloudManifestFile(const std::string& dbname) {
  return MakeCloudManifestFile(dbname, cloud_env_options_.cookie_on_open);
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
