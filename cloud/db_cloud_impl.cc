// Copyright (c) 2017 Rockset.
#ifndef ROCKSDB_LITE
#include "cloud/db_cloud_impl.h"

#include "cloud/db_cloud_plugin.h"
#include "cloud/filename.h"
#include "cloud/manifest_reader.h"
#include "env/composite_env_wrapper.h"
#include "file/file_util.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "util/xxhash.h"

namespace rocksdb {

DBCloudImpl::DBCloudImpl(DB* db) : DBCloud(db), cenv_(nullptr) {}

DBCloudImpl::~DBCloudImpl() {}

Status DBCloud::Open(const Options& options, const std::string& dbname,
                     const std::string& persistent_cache_path,
                     const uint64_t persistent_cache_size_gb, DBCloud** dbptr,
                     bool read_only) {
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
  std::vector<ColumnFamilyHandle*> handles;
  DBCloud* dbcloud = nullptr;
  Status s =
      DBCloud::Open(options, dbname, column_families, persistent_cache_path,
                    persistent_cache_size_gb, &handles, &dbcloud, read_only);
  if (s.ok()) {
    assert(handles.size() == 1);
    // i can delete the handle since DBImpl is always holding a reference to
    // default column family
    delete handles[0];
    *dbptr = dbcloud;
  }
  return s;
}

Status DBCloud::Open(const Options& opt, const std::string& local_dbname,
                     const std::vector<ColumnFamilyDescriptor>& column_families,
                     const std::string& persistent_cache_path,
                     const uint64_t persistent_cache_size_gb,
                     std::vector<ColumnFamilyHandle*>* handles, DBCloud** dbptr,
                     bool read_only) {
  Status st;
  Options options = opt;
  if (DBPlugin::Find(CloudOptionNames::kNameCloud, options.plugins) ==
      nullptr) {
    options.plugins.push_back(std::make_shared<cloud::CloudDBPlugin>(
        persistent_cache_path, persistent_cache_size_gb));
  }

  DB* db = nullptr;
  if (read_only) {
    st = DB::OpenForReadOnly(options, local_dbname, column_families, handles,
                             &db);
  } else {
    st = DB::Open(options, local_dbname, column_families, handles, &db);
  }

  if (st.ok()) {
    std::string dbid;
    db->GetDbIdentity(dbid);
    Log(InfoLogLevel::INFO_LEVEL, options.info_log,
        "Opened cloud db with local dir %s dbid %s", local_dbname.c_str(),
        dbid.c_str());
    *dbptr = static_cast<DBCloud*>(db);
  } else {
    Log(InfoLogLevel::INFO_LEVEL, options.info_log,
        "Failed Opening cloud db with local dir %s: %s", local_dbname.c_str(),
        st.ToString().c_str());
    *dbptr = nullptr;
  }
  return st;
}

Status DBCloudImpl::Savepoint() {
  std::string dbid;
  Options default_options = GetOptions();
  Status st = GetDbIdentity(dbid);
  if (!st.ok()) {
    Log(InfoLogLevel::INFO_LEVEL, default_options.info_log,
        "Savepoint could not get dbid %s", st.ToString().c_str());
    return st;
  }
  CloudEnvImpl* cenv = static_cast<CloudEnvImpl*>(GetEnv());

  // If there is no destination bucket, then nothing to do
  if (!cenv->HasDestBucket()) {
    Log(InfoLogLevel::INFO_LEVEL, default_options.info_log,
        "Savepoint on cloud dbid %s has no destination bucket, nothing to do.",
        dbid.c_str());
    return st;
  }

  Log(InfoLogLevel::INFO_LEVEL, default_options.info_log,
      "Savepoint on cloud dbid  %s", dbid.c_str());

  // find all sst files in the db
  std::vector<LiveFileMetaData> live_files;
  GetLiveFilesMetaData(&live_files);

  auto& provider = cenv->GetCloudEnvOptions().storage_provider;
  // If an sst file does not exist in the destination path, then remember it
  std::vector<std::string> to_copy;
  for (auto onefile : live_files) {
    auto remapped_fname = cenv->RemapFilename(onefile.name);
    std::string destpath = cenv->GetDestObjectPath() + "/" + remapped_fname;
    if (!provider->ExistsObject(cenv->GetDestBucketName(), destpath).ok()) {
      to_copy.push_back(remapped_fname);
    }
  }

  // copy all files in parallel
  std::atomic<size_t> next_file_meta_idx(0);
  int max_threads = default_options.max_file_opening_threads;

  std::function<void()> load_handlers_func = [&]() {
    while (true) {
      size_t idx = next_file_meta_idx.fetch_add(1);
      if (idx >= to_copy.size()) {
        break;
      }
      auto& onefile = to_copy[idx];
      Status s = provider->CopyObject(
          cenv->GetSrcBucketName(), cenv->GetSrcObjectPath() + "/" + onefile,
          cenv->GetDestBucketName(), cenv->GetDestObjectPath() + "/" + onefile);
      if (!s.ok()) {
        Log(InfoLogLevel::INFO_LEVEL, default_options.info_log,
            "Savepoint on cloud dbid  %s error in copying srcbucket %s srcpath "
            "%s dest bucket %s dest path %s. %s",
            dbid.c_str(), cenv->GetSrcBucketName().c_str(),
            cenv->GetSrcObjectPath().c_str(), cenv->GetDestBucketName().c_str(),
            cenv->GetDestObjectPath().c_str(), s.ToString().c_str());
        if (st.ok()) {
          st = s;  // save at least one error
        }
        break;
      }
    }
  };

  if (max_threads <= 1) {
    load_handlers_func();
  } else {
    std::vector<port::Thread> threads;
    for (int i = 0; i < max_threads; i++) {
      threads.emplace_back(load_handlers_func);
    }
    for (auto& t : threads) {
      t.join();
    }
  }
  return st;
}

Status DBCloudImpl::CheckpointToCloud(const BucketOptions& destination,
                                      const CheckpointToCloudOptions& options) {
  DisableFileDeletions();
  auto st = DoCheckpointToCloud(destination, options);
  EnableFileDeletions(false);
  return st;
}

Status DBCloudImpl::DoCheckpointToCloud(
    const BucketOptions& destination, const CheckpointToCloudOptions& options) {
  std::vector<std::string> live_files;
  uint64_t manifest_file_size{0};
  auto cenv = static_cast<CloudEnvImpl*>(GetEnv());
  auto base_env = cenv->GetBaseEnv();

  auto st =
      GetLiveFiles(live_files, &manifest_file_size, options.flush_memtable);
  if (!st.ok()) {
    return st;
  }

  std::vector<std::pair<std::string, std::string>> files_to_copy;
  for (auto& f : live_files) {
    uint64_t number = 0;
    FileType type;
    auto ok = ParseFileName(f, &number, &type);
    if (!ok) {
      return Status::InvalidArgument("Unknown file " + f);
    }
    if (type != kTableFile) {
      // ignore
      continue;
    }
    auto remapped_fname = cenv->RemapFilename(f);
    files_to_copy.emplace_back(remapped_fname, remapped_fname);
  }

  // IDENTITY file
  std::string dbid;
  st = ReadFileToString(cenv, IdentityFileName(GetName()), &dbid);
  if (!st.ok()) {
    return st;
  }
  dbid = rtrim_if(trim(dbid), '\n');
  files_to_copy.emplace_back(IdentityFileName(""), IdentityFileName(""));

  // MANIFEST file
  auto current_epoch = cenv->GetCloudManifest()->GetCurrentEpoch().ToString();
  auto manifest_fname = ManifestFileWithEpoch("", current_epoch);
  auto tmp_manifest_fname = manifest_fname + ".tmp";
  LegacyFileSystemWrapper fs(base_env);    
  st = CopyFile(&fs, GetName() + "/" + manifest_fname,
                GetName() + "/" + tmp_manifest_fname, manifest_file_size, false);
  if (!st.ok()) {
    return st;
  }
  files_to_copy.emplace_back(tmp_manifest_fname, std::move(manifest_fname));

  // CLOUDMANIFEST file
  files_to_copy.emplace_back(CloudManifestFile(""), CloudManifestFile(""));

  std::atomic<size_t> next_file_to_copy{0};
  int thread_count = std::max(1, options.thread_count);
  std::vector<Status> thread_statuses;
  thread_statuses.resize(thread_count);

  auto do_copy = [&](size_t threadId) {
    auto& provider = cenv->GetCloudEnvOptions().storage_provider;
    while (true) {
      size_t idx = next_file_to_copy.fetch_add(1);
      if (idx >= files_to_copy.size()) {
        break;
      }

      auto& f = files_to_copy[idx];
      auto copy_st = provider->PutObject(
          GetName() + "/" + f.first, destination.GetBucketName(),
          destination.GetObjectPath() + "/" + f.second);
      if (!copy_st.ok()) {
        thread_statuses[threadId] = std::move(copy_st);
        break;
      }
    }
  };

  if (thread_count == 1) {
    do_copy(0);
  } else {
    std::vector<std::thread> threads;
    for (int i = 0; i < thread_count; ++i) {
      threads.emplace_back([&, i]() { do_copy(i); });
    }
    for (auto& t : threads) {
      t.join();
    }
  }

  for (auto& s : thread_statuses) {
    if (!s.ok()) {
      st = s;
      break;
    }
  }

  if (!st.ok()) {
      return st;
  }

  // Ignore errors
  base_env->DeleteFile(tmp_manifest_fname);

  st = cenv->SaveDbid(destination.GetBucketName(), dbid,
                      destination.GetObjectPath());
  return st;
}

Status DBCloudImpl::ExecuteRemoteCompactionRequest(
      const PluggableCompactionParam& inputParams,
      PluggableCompactionResult* result ,
      bool doSanitize) {
  auto cenv = static_cast<CloudEnvImpl*>(GetEnv());

  // run the compaction request on the underlying local database
  Status status = GetBaseDB()->ExecuteRemoteCompactionRequest(inputParams,
		  result, doSanitize);
  if (!status.ok()) {
    return status;
  }

  // convert the local pathnames to the cloud pathnames
  for (unsigned int i = 0; i < result->output_files.size(); i++) {
      OutputFile* outfile = &result->output_files[i];
      outfile->pathname = cenv->RemapFilename(outfile->pathname);
  }
  return Status::OK();
}

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
