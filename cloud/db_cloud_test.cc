// Copyright (c) 2017 Rockset

#ifndef ROCKSDB_LITE

#ifdef USE_AWS

#include "rocksdb/cloud/db_cloud.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cinttypes>

#include "cloud/cloud_env_impl.h"
#include "cloud/cloud_storage_provider_impl.h"
#include "cloud/db_cloud_impl.h"
#include "cloud/filename.h"
#include "cloud/manifest_reader.h"
#include "db/db_impl/db_impl.h"
#include "file/filename.h"
#include "logging/logging.h"
#include "rocksdb/cloud/cloud_env_options.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/random.h"
#include "util/string_util.h"
#ifndef OS_WIN
#include <unistd.h>
#endif

namespace ROCKSDB_NAMESPACE {

class CloudTest : public testing::Test {
 public:
  CloudTest() {
    Random64 rng(time(nullptr));
    test_id_ = std::to_string(rng.Next());
    fprintf(stderr, "Test ID: %s\n", test_id_.c_str());

    base_env_ = Env::Default();
    dbname_ = test::TmpDir() + "/db_cloud-" + test_id_;
    clone_dir_ = test::TmpDir() + "/ctest-" + test_id_;
    cloud_env_options_.TEST_Initialize("dbcloudtest.", dbname_);
    cloud_env_options_.resync_manifest_on_open = true;

    options_.create_if_missing = true;
    options_.stats_dump_period_sec = 0;
    options_.stats_persist_period_sec = 0;
    persistent_cache_path_ = "";
    persistent_cache_size_gb_ = 0;
    db_ = nullptr;

    DestroyDir(dbname_);
    base_env_->CreateDirIfMissing(dbname_);
    base_env_->NewLogger(test::TmpDir(base_env_) + "/rocksdb-cloud.log",
                         &options_.info_log);
    options_.info_log->SetInfoLogLevel(InfoLogLevel::DEBUG_LEVEL);

    Cleanup();
  }

  void Cleanup() {
    ASSERT_TRUE(!aenv_);

    // check cloud credentials
    ASSERT_TRUE(cloud_env_options_.credentials.HasValid().ok());

    CloudEnv* aenv;
    // create a dummy aws env
    ASSERT_OK(CloudEnv::NewAwsEnv(base_env_, cloud_env_options_,
                                  options_.info_log, &aenv));
    ASSERT_NE(aenv, nullptr);
    aenv_.reset(aenv);
    // delete all pre-existing contents from the bucket
    Status st = aenv_->GetStorageProvider()->EmptyBucket(
        aenv_->GetSrcBucketName(), dbname_);
    ASSERT_TRUE(st.ok() || st.IsNotFound());
    aenv_.reset();

    DestroyDir(clone_dir_);
    ASSERT_OK(base_env_->CreateDir(clone_dir_));
  }

  std::set<std::string> GetSSTFiles(std::string name) {
    std::vector<std::string> files;
    aenv_->GetBaseEnv()->GetChildren(name, &files);
    std::set<std::string> sst_files;
    for (auto& f : files) {
      if (IsSstFile(RemoveEpoch(f))) {
        sst_files.insert(f);
      }
    }
    return sst_files;
  }

  // Return total size of all sst files available locally
  void GetSSTFilesTotalSize(std::string name, uint64_t* total_size) {
    std::vector<std::string> files;
    aenv_->GetBaseEnv()->GetChildren(name, &files);
    std::set<std::string> sst_files;
    uint64_t local_size = 0;
    for (auto& f : files) {
      if (IsSstFile(RemoveEpoch(f))) {
        sst_files.insert(f);
        std::string lpath = dbname_ + "/" + f;
        ASSERT_OK(aenv_->GetBaseEnv()->GetFileSize(lpath, &local_size));
        (*total_size) += local_size;
      }
    }
  }

  std::set<std::string> GetSSTFilesClone(std::string name) {
    std::string cname = clone_dir_ + "/" + name;
    return GetSSTFiles(cname);
  }

  void DestroyDir(const std::string& dir) {
    std::string cmd = "rm -rf " + dir;
    int rc = system(cmd.c_str());
    ASSERT_EQ(rc, 0);
  }

  virtual ~CloudTest() {
    // Cleanup the cloud bucket
    if (!cloud_env_options_.src_bucket.GetBucketName().empty()) {
      CloudEnv* aenv;
      Status st = CloudEnv::NewAwsEnv(base_env_, cloud_env_options_,
                                      options_.info_log, &aenv);
      if (st.ok()) {
        aenv->GetStorageProvider()->EmptyBucket(aenv->GetSrcBucketName(),
                                                dbname_);
        delete aenv;
      }
    }

    CloseDB();
  }

  void CreateCloudEnv() {
    CloudEnv* cenv;
    cloud_env_options_.use_aws_transfer_manager = true;
    ASSERT_OK(CloudEnv::NewAwsEnv(base_env_, cloud_env_options_,
                                  options_.info_log, &cenv));
    // To catch any possible file deletion bugs, we set file deletion delay to
    // smallest possible
    CloudEnvImpl* cimpl = static_cast<CloudEnvImpl*>(cenv);
    cimpl->TEST_SetFileDeletionDelay(std::chrono::seconds(0));
    aenv_.reset(cenv);
  }

  // Open database via the cloud interface
  void OpenDB() {
    std::vector<ColumnFamilyHandle*> handles;
    OpenDB(&handles);
    // Delete the handle for the default column family because the DBImpl
    // always holds a reference to it.
    ASSERT_TRUE(handles.size() > 0);
    delete handles[0];
  }

  // Open database via the cloud interface
  void OpenDB(std::vector<ColumnFamilyHandle*>* handles) {
    // default column family
    OpenWithColumnFamilies({kDefaultColumnFamilyName}, handles);
  }

  void OpenWithColumnFamilies(const std::vector<std::string>& cfs,
                              std::vector<ColumnFamilyHandle*>* handles) {
    ASSERT_TRUE(cloud_env_options_.credentials.HasValid().ok());

    // Create new AWS env
    CreateCloudEnv();
    options_.env = aenv_.get();
    // Sleep for a second because S3 is eventual consistency.
    std::this_thread::sleep_for(std::chrono::seconds(1));

    ASSERT_TRUE(db_ == nullptr);
    std::vector<ColumnFamilyDescriptor> column_families;
    for (size_t i = 0; i < cfs.size(); ++i) {
      column_families.emplace_back(cfs[i], options_);
    }
    ASSERT_OK(DBCloud::Open(options_, dbname_, column_families,
                            persistent_cache_path_, persistent_cache_size_gb_,
                            handles, &db_));
    ASSERT_OK(db_->GetDbIdentity(dbid_));
  }

  // Try to open and return status
  Status checkOpen() {
    // Create new AWS env
    CreateCloudEnv();
    options_.env = aenv_.get();
    // Sleep for a second because S3 is eventual consistency.
    std::this_thread::sleep_for(std::chrono::seconds(1));

    return DBCloud::Open(options_, dbname_, persistent_cache_path_,
                         persistent_cache_size_gb_, &db_);
  }

  void CreateColumnFamilies(const std::vector<std::string>& cfs,
                            std::vector<ColumnFamilyHandle*>* handles) {
    ASSERT_NE(db_, nullptr);
    size_t cfi = handles->size();
    handles->resize(cfi + cfs.size());
    for (auto cf : cfs) {
      ASSERT_OK(db_->CreateColumnFamily(options_, cf, &handles->at(cfi++)));
    }
  }

  // Creates and Opens a clone
  Status CloneDB(const std::string& clone_name,
                 const std::string& dest_bucket_name,
                 const std::string& dest_object_path,
                 std::unique_ptr<DBCloud>* cloud_db,
                 std::unique_ptr<CloudEnv>* cloud_env,
                 bool force_keep_local_on_invalid_dest_bucket = true) {
    // The local directory where the clone resides
    std::string cname = clone_dir_ + "/" + clone_name;

    CloudEnv* cenv;
    DBCloud* clone_db;

    // If there is no destination bucket, then the clone needs to copy
    // all sst fies from source bucket to local dir
    CloudEnvOptions copt = cloud_env_options_;
    if (dest_bucket_name == copt.src_bucket.GetBucketName()) {
      copt.dest_bucket = copt.src_bucket;
    } else {
      copt.dest_bucket.SetBucketName(dest_bucket_name);
    }
    copt.dest_bucket.SetObjectPath(dest_object_path);
    if (!copt.dest_bucket.IsValid() &&
        force_keep_local_on_invalid_dest_bucket) {
      copt.keep_local_sst_files = true;
    }
    // Create new AWS env
    Status st = CloudEnv::NewAwsEnv(base_env_, copt, options_.info_log, &cenv);
    if (!st.ok()) {
      return st;
    }

    // To catch any possible file deletion bugs, we set file deletion delay to
    // smallest possible
    CloudEnvImpl* cimpl = static_cast<CloudEnvImpl*>(cenv);
    cimpl->TEST_SetFileDeletionDelay(std::chrono::seconds(0));
    // sets the cloud env to be used by the env wrapper
    options_.env = cenv;

    // Returns the cloud env that was created
    cloud_env->reset(cenv);

    // default column family
    ColumnFamilyOptions cfopt = options_;

    std::vector<ColumnFamilyDescriptor> column_families;
    column_families.emplace_back(
        ColumnFamilyDescriptor(kDefaultColumnFamilyName, cfopt));
    std::vector<ColumnFamilyHandle*> handles;

    st = DBCloud::Open(options_, cname, column_families, persistent_cache_path_,
                       persistent_cache_size_gb_, &handles, &clone_db);
    if (!st.ok()) {
      return st;
    }

    cloud_db->reset(clone_db);

    // Delete the handle for the default column family because the DBImpl
    // always holds a reference to it.
    assert(handles.size() > 0);
    delete handles[0];

    return st;
  }

  void CloseDB(std::vector<ColumnFamilyHandle*>* handles) {
    for (auto h : *handles) {
      delete h;
    }
    handles->clear();
    CloseDB();
  }

  void CloseDB() {
    if (db_) {
      db_->Flush(FlushOptions());  // convert pending writes to sst files
      delete db_;
      db_ = nullptr;
    }
  }

  void SetPersistentCache(const std::string& path, uint64_t size_gb) {
    persistent_cache_path_ = path;
    persistent_cache_size_gb_ = size_gb;
  }

  Status GetCloudLiveFilesSrc(std::set<uint64_t>* list) {
    std::unique_ptr<ManifestReader> manifest(new ManifestReader(
        options_.info_log, aenv_.get(), aenv_->GetSrcBucketName()));
    return manifest->GetLiveFiles(aenv_->GetSrcObjectPath(), list);
  }

  // Verify that local files are the same as cloud files in src bucket path
  void ValidateCloudLiveFilesSrcSize() {
    // Loop though all the files in the cloud manifest
    std::set<uint64_t> cloud_files;
    ASSERT_OK(GetCloudLiveFilesSrc(&cloud_files));
    for (uint64_t num : cloud_files) {
      std::string pathname = MakeTableFileName(dbname_, num);
      Log(options_.info_log, "cloud file list  %s\n", pathname.c_str());
    }

    std::set<std::string> localFiles = GetSSTFiles(dbname_);
    uint64_t cloudSize = 0;
    uint64_t localSize = 0;

    // loop through all the local files and validate
    for (std::string path : localFiles) {
      std::string cpath = aenv_->GetSrcObjectPath() + "/" + path;
      ASSERT_OK(aenv_->GetStorageProvider()->GetCloudObjectSize(
          aenv_->GetSrcBucketName(), cpath, &cloudSize));

      // find the size of the file on local storage
      std::string lpath = dbname_ + "/" + path;
      ASSERT_OK(aenv_->GetBaseEnv()->GetFileSize(lpath, &localSize));
      ASSERT_TRUE(localSize == cloudSize);
      Log(options_.info_log, "local file %s size %" PRIu64 "\n", lpath.c_str(),
          localSize);
      Log(options_.info_log, "cloud file %s size %" PRIu64 "\n", cpath.c_str(),
          cloudSize);
      printf("local file %s size %" PRIu64 "\n", lpath.c_str(), localSize);
      printf("cloud file %s size %" PRIu64 "\n", cpath.c_str(), cloudSize);
    }
  }

  CloudEnvImpl* GetCloudEnvImpl() const {
    return static_cast<CloudEnvImpl*>(aenv_.get());
  }

  DBImpl* GetDBImpl() const {
    return static_cast<DBImpl*>(db_->GetBaseDB());
  }

  void SwitchToNewCookie(std::string new_cookie) {
    CloudManifestDelta delta{
      db_->GetNextFileNumber(),
      new_cookie
    };
    ASSERT_OK(aenv_->RollNewCookie(dbname_, new_cookie, delta));
    ASSERT_OK(aenv_->ApplyCloudManifestDelta(delta));
    db_->NewManifestOnNextUpdate();
  }

 protected:
  void WaitUntilNoScheduledJobs() {
    while (true) {
      auto num = GetCloudEnvImpl()->TEST_NumScheduledJobs();
      if (num > 0) {
        usleep(100);
      } else {
        return;
      }
    }
  }

  std::vector<Env::FileAttributes> GetAllLocalFiles() {
    std::vector<Env::FileAttributes> local_files;
    assert(base_env_->GetChildrenFileAttributes(dbname_, &local_files).ok());
    return local_files;
  }

  std::string test_id_;
  Env* base_env_;
  Options options_;
  std::string dbname_;
  std::string clone_dir_;
  CloudEnvOptions cloud_env_options_;
  std::string dbid_;
  std::string persistent_cache_path_;
  uint64_t persistent_cache_size_gb_;
  DBCloud* db_;
  std::unique_ptr<CloudEnv> aenv_;
};

//
// Most basic test. Create DB, write one key, close it and then check to see
// that the key exists.
//
TEST_F(CloudTest, BasicTest) {
  // Put one key-value
  OpenDB();
  std::string value;
  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "World"));
  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_TRUE(value.compare("World") == 0);
  CloseDB();
  value.clear();

  // Reopen and validate
  OpenDB();
  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_EQ(value, "World");

  std::set<uint64_t> live_files;
  ASSERT_OK(GetCloudLiveFilesSrc(&live_files));
  ASSERT_GT(live_files.size(), 0);
  CloseDB();
}

TEST_F(CloudTest, FindAllLiveFilesTest) {
  OpenDB();
  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "World"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // wait until files are persisted into s3
  GetDBImpl()->TEST_WaitForBackgroundWork();

  CloseDB();

  std::vector<std::string> tablefiles;
  std::string manifest;
  std::string manifest_file_version;

  // fetch latest manifest to local
  ASSERT_OK(aenv_->FindAllLiveFilesAndFetchManifest(
      dbname_, &tablefiles, &manifest, &manifest_file_version));
  EXPECT_EQ(tablefiles.size(), 1);
  EXPECT_FALSE(manifest_file_version.empty());

  for (auto name: tablefiles) {
    EXPECT_EQ(GetFileType(name), RocksDBFileType::kSstFile);
    // verify that the sst file indeed exists in cloud
    EXPECT_OK(
        aenv_->GetStorageProvider()
            ->ExistsCloudObject(aenv_->GetSrcBucketName(),
                                aenv_->GetSrcObjectPath() + pathsep + name));
  }

  EXPECT_EQ(GetFileType(manifest), RocksDBFileType::kManifestFile);
  // verify that manifest file indeed exists in cloud
  auto cloud_storage = std::dynamic_pointer_cast<CloudStorageProviderImpl>(
      aenv_->GetStorageProvider());
  EXPECT_OK(cloud_storage->TEST_ExistsCloudObject(
      aenv_->GetSrcBucketName(), aenv_->GetSrcObjectPath() + pathsep + manifest,
      manifest_file_version));
}

// Files of dropped CF should not be included in live files
TEST_F(CloudTest, LiveFilesOfDroppedCFTest) {
  std::vector<ColumnFamilyHandle*> handles;
  OpenDB(&handles);

  std::vector<std::string> tablefiles;
  std::string manifest;
  ASSERT_OK(aenv_->FindAllLiveFiles(dbname_, &tablefiles, &manifest));

  EXPECT_TRUE(tablefiles.empty());
  CreateColumnFamilies({"cf1"}, &handles);

  // write to CF
  ASSERT_OK(db_->Put(WriteOptions(), handles[1], "hello", "world"));
  // flush cf1
  ASSERT_OK(db_->Flush({}, handles[1]));

  tablefiles.clear();
  ASSERT_OK(aenv_->FindAllLiveFiles(dbname_, &tablefiles, &manifest));
  EXPECT_TRUE(tablefiles.size() == 1);

  // Drop the CF
  ASSERT_OK(db_->DropColumnFamily(handles[1]));
  tablefiles.clear();
  // make sure that files are not listed as live for dropped CF
  ASSERT_OK(aenv_->FindAllLiveFiles(dbname_, &tablefiles, &manifest));
  EXPECT_TRUE(tablefiles.empty());
  CloseDB(&handles);
}

// Verifies that when we move files across levels, the files are still listed as
// live files
TEST_F(CloudTest, LiveFilesAfterChangingLevelTest) {
  options_.num_levels = 3;
  OpenDB();
  ASSERT_OK(db_->Put(WriteOptions(), "a", "1"));
  ASSERT_OK(db_->Put(WriteOptions(), "b", "2"));
  ASSERT_OK(db_->Flush({}));
  auto db_impl = GetDBImpl();

  std::vector<std::string> tablefiles_before_move;
  std::string manifest;
  ASSERT_OK(
      aenv_->FindAllLiveFiles(dbname_, &tablefiles_before_move, &manifest));
  EXPECT_EQ(tablefiles_before_move.size(), 1);

  CompactRangeOptions cro;
  cro.change_level = true;
  cro.target_level = 2;
  // Move the sst files to another level by compacting entire range
  ASSERT_OK(db_->CompactRange(cro, nullptr /* begin */, nullptr /* end */));

  ASSERT_OK(db_impl->TEST_WaitForBackgroundWork());

  std::vector<std::string> tablefiles_after_move;
  ASSERT_OK(aenv_->FindAllLiveFiles(dbname_, &tablefiles_after_move, &manifest));
  EXPECT_EQ(tablefiles_before_move, tablefiles_after_move);
}

TEST_F(CloudTest, GetChildrenTest) {
  // Create some objects in S3
  OpenDB();
  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "World"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  CloseDB();
  DestroyDir(dbname_);
  OpenDB();

  std::vector<std::string> children;
  ASSERT_OK(aenv_->GetChildren(dbname_, &children));
  int sst_files = 0;
  for (auto c : children) {
    if (IsSstFile(c)) {
      sst_files++;
    }
  }
  // This verifies that GetChildren() works on S3. We deleted the S3 file
  // locally, so the only way to actually get it through GetChildren() if
  // listing S3 buckets works.
  EXPECT_EQ(sst_files, 1);
}

//
// Create and read from a clone.
//
TEST_F(CloudTest, Newdb) {
  std::string master_dbid;
  std::string newdb1_dbid;
  std::string newdb2_dbid;

  // Put one key-value
  OpenDB();
  std::string value;
  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "World"));
  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_TRUE(value.compare("World") == 0);
  ASSERT_OK(db_->GetDbIdentity(master_dbid));
  CloseDB();
  value.clear();

  {
    // Create and Open a new ephemeral instance
    std::unique_ptr<CloudEnv> cloud_env;
    std::unique_ptr<DBCloud> cloud_db;
    CloneDB("newdb1", "", "", &cloud_db, &cloud_env);

    // Retrieve the id of the first reopen
    ASSERT_OK(cloud_db->GetDbIdentity(newdb1_dbid));

    // This is an ephemeral clone. Its dbid is a prefix of the master's.
    ASSERT_NE(newdb1_dbid, master_dbid);
    auto res = std::mismatch(master_dbid.begin(), master_dbid.end(),
                             newdb1_dbid.begin());
    ASSERT_TRUE(res.first == master_dbid.end());

    ASSERT_OK(cloud_db->Get(ReadOptions(), "Hello", &value));
    ASSERT_TRUE(value.compare("World") == 0);

    // Open master and write one more kv to it. This is written to
    // src bucket as well.
    OpenDB();
    ASSERT_OK(db_->Put(WriteOptions(), "Dhruba", "Borthakur"));

    // check that the newly written kv exists
    value.clear();
    ASSERT_OK(db_->Get(ReadOptions(), "Dhruba", &value));
    ASSERT_TRUE(value.compare("Borthakur") == 0);

    // check that the earlier kv exists too
    value.clear();
    ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
    ASSERT_TRUE(value.compare("World") == 0);
    CloseDB();

    // Assert  that newdb1 cannot see the second kv because the second kv
    // was written to local dir only of the ephemeral clone.
    ASSERT_TRUE(cloud_db->Get(ReadOptions(), "Dhruba", &value).IsNotFound());
  }
  {
    // Create another ephemeral instance using a different local dir but the
    // same two buckets as newdb1. This should be identical in contents with
    // newdb1.
    std::unique_ptr<CloudEnv> cloud_env;
    std::unique_ptr<DBCloud> cloud_db;
    CloneDB("newdb2", "", "", &cloud_db, &cloud_env);

    // Retrieve the id of the second clone db
    ASSERT_OK(cloud_db->GetDbIdentity(newdb2_dbid));

    // Since we use two different local directories for the two ephemeral
    // clones, their dbids should be different from one another
    ASSERT_NE(newdb1_dbid, newdb2_dbid);

    // check that both the kvs appear in the new ephemeral clone
    value.clear();
    ASSERT_OK(cloud_db->Get(ReadOptions(), "Hello", &value));
    ASSERT_TRUE(value.compare("World") == 0);
    value.clear();
    ASSERT_OK(cloud_db->Get(ReadOptions(), "Dhruba", &value));
    ASSERT_TRUE(value.compare("Borthakur") == 0);
  }

  CloseDB();
}

TEST_F(CloudTest, ColumnFamilies) {
  std::vector<ColumnFamilyHandle*> handles;
  // Put one key-value
  OpenDB(&handles);

  CreateColumnFamilies({"cf1", "cf2"}, &handles);

  ASSERT_OK(db_->Put(WriteOptions(), handles[0], "hello", "a"));
  ASSERT_OK(db_->Put(WriteOptions(), handles[1], "hello", "b"));
  ASSERT_OK(db_->Put(WriteOptions(), handles[2], "hello", "c"));

  auto validate = [&]() {
    std::string value;
    ASSERT_OK(db_->Get(ReadOptions(), handles[0], "hello", &value));
    ASSERT_EQ(value, "a");
    ASSERT_OK(db_->Get(ReadOptions(), handles[1], "hello", &value));
    ASSERT_EQ(value, "b");
    ASSERT_OK(db_->Get(ReadOptions(), handles[2], "hello", &value));
    ASSERT_EQ(value, "c");
  };

  validate();

  CloseDB(&handles);
  OpenWithColumnFamilies({kDefaultColumnFamilyName, "cf1", "cf2"}, &handles);

  validate();
  CloseDB(&handles);

  // destory local state
  DestroyDir(dbname_);

  // new cloud env
  CreateCloudEnv();
  options_.env = aenv_.get();

  std::vector<std::string> families;
  ASSERT_OK(DBCloud::ListColumnFamilies(options_, dbname_, &families));
  std::sort(families.begin(), families.end());
  ASSERT_TRUE(families == std::vector<std::string>(
                              {"cf1", "cf2", kDefaultColumnFamilyName}));

  OpenWithColumnFamilies({kDefaultColumnFamilyName, "cf1", "cf2"}, &handles);
  validate();
  CloseDB(&handles);
}

//
// Create and read from a clone.
//
TEST_F(CloudTest, DISABLED_TrueClone) {
  std::string master_dbid;
  std::string newdb1_dbid;
  std::string newdb2_dbid;
  std::string newdb3_dbid;

  // Put one key-value
  OpenDB();
  std::string value;
  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "World"));
  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_TRUE(value.compare("World") == 0);
  ASSERT_OK(db_->GetDbIdentity(master_dbid));
  ASSERT_OK(db_->Flush(FlushOptions()));
  CloseDB();
  value.clear();
  auto clone_path1 = "clone1_path-" + test_id_;
  {
    // Create a new instance with different src and destination paths.
    // This is true clone and should have all the contents of the masterdb
    std::unique_ptr<CloudEnv> cloud_env;
    std::unique_ptr<DBCloud> cloud_db;
    CloneDB("localpath1", cloud_env_options_.src_bucket.GetBucketName(),
            clone_path1, &cloud_db, &cloud_env);

    // Retrieve the id of the clone db
    ASSERT_OK(cloud_db->GetDbIdentity(newdb1_dbid));

    // Since we used the different src and destination paths for both
    // the master and clone1, the clone should have its own identity.
    ASSERT_NE(master_dbid, newdb1_dbid);

    // check that the original kv appears in the clone
    value.clear();
    ASSERT_OK(cloud_db->Get(ReadOptions(), "Hello", &value));
    ASSERT_TRUE(value.compare("World") == 0);

    // write a new value to the clone
    ASSERT_OK(cloud_db->Put(WriteOptions(), "Hello", "Clone1"));
    value.clear();
    ASSERT_OK(cloud_db->Get(ReadOptions(), "Hello", &value));
    ASSERT_TRUE(value.compare("Clone1") == 0);
    ASSERT_OK(cloud_db->Flush(FlushOptions()));
  }
  {
    // Reopen clone1 with a different local path
    std::unique_ptr<CloudEnv> cloud_env;
    std::unique_ptr<DBCloud> cloud_db;
    CloneDB("localpath2", cloud_env_options_.src_bucket.GetBucketName(),
            clone_path1, &cloud_db, &cloud_env);

    // Retrieve the id of the clone db
    ASSERT_OK(cloud_db->GetDbIdentity(newdb2_dbid));
    ASSERT_EQ(newdb1_dbid, newdb2_dbid);
    value.clear();
    ASSERT_OK(cloud_db->Get(ReadOptions(), "Hello", &value));
    ASSERT_TRUE(value.compare("Clone1") == 0);
    ASSERT_OK(cloud_db->Flush(FlushOptions()));
  }
  {
    // Reopen clone1 with the same local path as above.
    std::unique_ptr<CloudEnv> cloud_env;
    std::unique_ptr<DBCloud> cloud_db;
    CloneDB("localpath2", cloud_env_options_.src_bucket.GetBucketName(),
            clone_path1, &cloud_db, &cloud_env);

    // Retrieve the id of the clone db
    ASSERT_OK(cloud_db->GetDbIdentity(newdb2_dbid));
    ASSERT_EQ(newdb1_dbid, newdb2_dbid);
    value.clear();
    ASSERT_OK(cloud_db->Get(ReadOptions(), "Hello", &value));
    ASSERT_TRUE(value.compare("Clone1") == 0);
    ASSERT_OK(cloud_db->Flush(FlushOptions()));
  }
  auto clone_path2 = "clone2_path-" + test_id_;
  {
    // Create clone2
    std::unique_ptr<CloudEnv> cloud_env;
    std::unique_ptr<DBCloud> cloud_db;
    CloneDB("localpath3",  // xxx try with localpath2
            cloud_env_options_.src_bucket.GetBucketName(), clone_path2,
            &cloud_db, &cloud_env);

    // Retrieve the id of the clone db
    ASSERT_OK(cloud_db->GetDbIdentity(newdb3_dbid));
    ASSERT_NE(newdb2_dbid, newdb3_dbid);

    // verify that data is still as it was in the original db.
    value.clear();
    ASSERT_OK(cloud_db->Get(ReadOptions(), "Hello", &value));
    ASSERT_TRUE(value.compare("World") == 0);

    // Assert that there are no redundant sst files
    CloudEnvImpl* env = static_cast<CloudEnvImpl*>(cloud_env.get());
    std::vector<std::string> to_be_deleted;
    ASSERT_OK(env->FindObsoleteFiles(env->GetSrcBucketName(), &to_be_deleted));
    // TODO(igor): Re-enable once purger code is fixed
    // ASSERT_EQ(to_be_deleted.size(), 0);

    // Assert that there are no redundant dbid
    ASSERT_OK(env->FindObsoleteDbid(env->GetSrcBucketName(), &to_be_deleted));
    // TODO(igor): Re-enable once purger code is fixed
    // ASSERT_EQ(to_be_deleted.size(), 0);
  }

  aenv_->GetStorageProvider()->EmptyBucket(aenv_->GetSrcBucketName(),
                                           clone_path1);
  aenv_->GetStorageProvider()->EmptyBucket(aenv_->GetSrcBucketName(),
                                           clone_path2);
}

//
// verify that dbid registry is appropriately handled
//
TEST_F(CloudTest, DbidRegistry) {
  // Put one key-value
  OpenDB();
  std::string value;
  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "World"));
  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_TRUE(value.compare("World") == 0);

  // Assert that there is one db in the registry
  DbidList dbs;
  ASSERT_OK(aenv_->GetDbidList(aenv_->GetSrcBucketName(), &dbs));
  ASSERT_GE(dbs.size(), 1);

  CloseDB();
}

TEST_F(CloudTest, KeepLocalFiles) {
  cloud_env_options_.keep_local_sst_files = true;
  for (int iter = 0; iter < 4; ++iter) {
    cloud_env_options_.use_direct_io_for_cloud_download =
        iter == 0 || iter == 1;
    cloud_env_options_.use_aws_transfer_manager = iter == 0 || iter == 3;
    // Create two files
    OpenDB();
    std::string value;
    ASSERT_OK(db_->Put(WriteOptions(), "Hello", "World"));
    ASSERT_OK(db_->Flush(FlushOptions()));
    ASSERT_OK(db_->Put(WriteOptions(), "Hello2", "World2"));
    ASSERT_OK(db_->Flush(FlushOptions()));

    CloseDB();
    DestroyDir(dbname_);
    OpenDB();

    std::vector<std::string> files;
    ASSERT_OK(Env::Default()->GetChildren(dbname_, &files));
    long sst_files =
        std::count_if(files.begin(), files.end(), [](const std::string& file) {
          return file.find("sst") != std::string::npos;
        });
    ASSERT_EQ(sst_files, 2);

    ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
    ASSERT_EQ(value, "World");
    ASSERT_OK(db_->Get(ReadOptions(), "Hello2", &value));
    ASSERT_EQ(value, "World2");

    CloseDB();
    ValidateCloudLiveFilesSrcSize();
    aenv_->GetStorageProvider()->EmptyBucket(aenv_->GetSrcBucketName(),
                                             dbname_);
    DestroyDir(dbname_);
  }
}

TEST_F(CloudTest, CopyToFromS3) {
  std::string fname = dbname_ + "/100000.sst";

  // iter 0 -- not using transfer manager
  // iter 1 -- using transfer manager
  for (int iter = 0; iter < 2; ++iter) {
    // Create aws env
    cloud_env_options_.keep_local_sst_files = true;
    cloud_env_options_.use_aws_transfer_manager = iter == 1;
    CreateCloudEnv();
    CloudEnvImpl* cimpl = static_cast<CloudEnvImpl*>(aenv_.get());
    cimpl->TEST_InitEmptyCloudManifest();
    char buffer[1 * 1024 * 1024];

    // create a 10 MB file and upload it to cloud
    {
      std::unique_ptr<WritableFile> writer;
      ASSERT_OK(aenv_->NewWritableFile(fname, &writer, EnvOptions()));

      for (int i = 0; i < 10; i++) {
        ASSERT_OK(writer->Append(Slice(buffer, sizeof(buffer))));
      }
      // sync and close file
    }

    // delete the file manually.
    ASSERT_OK(base_env_->DeleteFile(fname));

    // reopen file for reading. It should be refetched from cloud storage.
    {
      std::unique_ptr<RandomAccessFile> reader;
      ASSERT_OK(aenv_->NewRandomAccessFile(fname, &reader, EnvOptions()));

      uint64_t offset = 0;
      for (int i = 0; i < 10; i++) {
        Slice result;
        char* scratch = &buffer[0];
        ASSERT_OK(reader->Read(offset, sizeof(buffer), &result, scratch));
        ASSERT_EQ(result.size(), sizeof(buffer));
        offset += sizeof(buffer);
      }
    }
  }
}

TEST_F(CloudTest, DelayFileDeletion) {
  std::string fname = dbname_ + "/000010.sst";

  // Create aws env
  cloud_env_options_.keep_local_sst_files = true;
  CreateCloudEnv();
  CloudEnvImpl* cimpl = static_cast<CloudEnvImpl*>(aenv_.get());
  cimpl->TEST_InitEmptyCloudManifest();
  cimpl->TEST_SetFileDeletionDelay(std::chrono::seconds(2));

  auto createFile = [&]() {
    std::unique_ptr<WritableFile> writer;
    ASSERT_OK(aenv_->NewWritableFile(fname, &writer, EnvOptions()));

    for (int i = 0; i < 10; i++) {
      ASSERT_OK(writer->Append("igor"));
    }
    // sync and close file
  };

  for (int iter = 0; iter <= 1; ++iter) {
    createFile();
    // delete the file
    ASSERT_OK(aenv_->DeleteFile(fname));
    // file should still be there
    ASSERT_OK(aenv_->FileExists(fname));

    if (iter == 1) {
      // should prevent the deletion
      createFile();
    }

    std::this_thread::sleep_for(std::chrono::seconds(3));
    auto st = aenv_->FileExists(fname);
    if (iter == 0) {
      // in iter==0 file should be deleted after 2 seconds
      ASSERT_TRUE(st.IsNotFound());
    } else {
      // in iter==1 file should not be deleted because we wrote the new file
      ASSERT_OK(st);
    }
  }
}

// Verify that a savepoint copies all src files to destination
TEST_F(CloudTest, Savepoint) {
  // Put one key-value
  OpenDB();
  std::string value;
  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "World"));
  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_TRUE(value.compare("World") == 0);
  CloseDB();
  value.clear();
  std::string dest_path = "/clone2_path-" + test_id_;
  {
    // Create a new instance with different src and destination paths.
    // This is true clone and should have all the contents of the masterdb
    std::unique_ptr<CloudEnv> cloud_env;
    std::unique_ptr<DBCloud> cloud_db;
    CloneDB("localpath1", cloud_env_options_.src_bucket.GetBucketName(),
            dest_path, &cloud_db, &cloud_env);

    // check that the original kv appears in the clone
    value.clear();
    ASSERT_OK(cloud_db->Get(ReadOptions(), "Hello", &value));
    ASSERT_TRUE(value.compare("World") == 0);

    // there should be only one sst file
    std::vector<LiveFileMetaData> flist;
    cloud_db->GetLiveFilesMetaData(&flist);
    ASSERT_TRUE(flist.size() == 1);

    CloudEnvImpl* cimpl = static_cast<CloudEnvImpl*>(cloud_env.get());
    auto remapped_fname = cimpl->RemapFilename(flist[0].name);
    // source path
    std::string spath = cloud_env->GetSrcObjectPath() + "/" + remapped_fname;
    ASSERT_OK(cloud_env->GetStorageProvider()->ExistsCloudObject(
        cloud_env->GetSrcBucketName(), spath));

    // Verify that the destination path does not have any sst files
    std::string dpath = dest_path + "/" + remapped_fname;
    ASSERT_TRUE(cloud_env->GetStorageProvider()
                    ->ExistsCloudObject(cloud_env->GetSrcBucketName(), dpath)
                    .IsNotFound());

    // write a new value to the clone
    ASSERT_OK(cloud_db->Put(WriteOptions(), "Hell", "Done"));
    value.clear();
    ASSERT_OK(cloud_db->Get(ReadOptions(), "Hell", &value));
    ASSERT_TRUE(value.compare("Done") == 0);

    // Invoke savepoint to populate destination path from source path
    ASSERT_OK(cloud_db->Savepoint());

    // check that the sst file is copied to dest path
    ASSERT_OK(cloud_env->GetStorageProvider()->ExistsCloudObject(
        cloud_env->GetSrcBucketName(), dpath));
    ASSERT_OK(cloud_db->Flush(FlushOptions()));
  }
  {
    // Reopen the clone
    std::unique_ptr<CloudEnv> cloud_env;
    std::unique_ptr<DBCloud> cloud_db;
    CloneDB("localpath2", cloud_env_options_.src_bucket.GetBucketName(),
            dest_path, &cloud_db, &cloud_env);

    // check that the both kvs appears in the clone
    value.clear();
    ASSERT_OK(cloud_db->Get(ReadOptions(), "Hello", &value));
    ASSERT_TRUE(value.compare("World") == 0);
    value.clear();
    ASSERT_OK(cloud_db->Get(ReadOptions(), "Hell", &value));
    ASSERT_TRUE(value.compare("Done") == 0);
  }
  aenv_->GetStorageProvider()->EmptyBucket(aenv_->GetSrcBucketName(),
                                           dest_path);
}

TEST_F(CloudTest, Encryption) {
  // Create aws env
  cloud_env_options_.server_side_encryption = true;
  char* key_id = getenv("AWS_KMS_KEY_ID");
  if (key_id != nullptr) {
    cloud_env_options_.encryption_key_id = std::string(key_id);
    Log(options_.info_log, "Found encryption key id in env variable %s",
        key_id);
  }

  OpenDB();

  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "World"));
  // create a file
  ASSERT_OK(db_->Flush(FlushOptions()));
  CloseDB();

  OpenDB();
  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_EQ(value, "World");
  CloseDB();
}

TEST_F(CloudTest, DirectReads) {
  options_.use_direct_reads = true;
  options_.use_direct_io_for_flush_and_compaction = true;
  BlockBasedTableOptions bbto;
  bbto.no_block_cache = true;
  bbto.block_size = 1024;
  options_.table_factory.reset(NewBlockBasedTableFactory(bbto));

  OpenDB();

  for (int i = 0; i < 50; ++i) {
    ASSERT_OK(db_->Put(WriteOptions(), "Hello" + std::to_string(i), "World"));
  }
  // create a file
  ASSERT_OK(db_->Flush(FlushOptions()));

  std::string value;
  for (int i = 0; i < 50; ++i) {
    ASSERT_OK(db_->Get(ReadOptions(), "Hello" + std::to_string(i), &value));
    ASSERT_EQ(value, "World");
  }
  CloseDB();
}

#ifdef USE_KAFKA
TEST_F(CloudTest, KeepLocalLogKafka) {
  cloud_env_options_.keep_local_log_files = false;
  cloud_env_options_.log_type = LogType::kLogKafka;
  cloud_env_options_.kafka_log_options
      .client_config_params["metadata.broker.list"] = "localhost:9092";

  OpenDB();

  ASSERT_OK(db_->Put(WriteOptions(), "Franz", "Kafka"));

  // Destroy DB in memory and on local file system.
  delete db_;
  db_ = nullptr;
  aenv_.reset();
  DestroyDir(dbname_);
  DestroyDir("/tmp/ROCKSET");

  // Create new env.
  CreateCloudEnv();

  // Give env enough time to consume WALs
  std::this_thread::sleep_for(std::chrono::seconds(3));

  // Open DB.
  cloud_env_options_.keep_local_log_files = true;
  CloudEnvImpl* cimpl = static_cast<CloudEnvImpl*>(aenv_.get());
  options_.wal_dir = cimpl->GetWALCacheDir();
  OpenDB();

  // Test read.
  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), "Franz", &value));
  ASSERT_EQ(value, "Kafka");

  CloseDB();
}
#endif /* USE_KAFKA */

// TODO(igor): determine why this fails,
// https://github.com/rockset/rocksdb-cloud/issues/35
TEST_F(CloudTest, DISABLED_KeepLocalLogKinesis) {
  cloud_env_options_.keep_local_log_files = false;
  cloud_env_options_.log_type = LogType::kLogKinesis;

  OpenDB();

  // Test write.
  ASSERT_OK(db_->Put(WriteOptions(), "Tele", "Kinesis"));

  // Destroy DB in memory and on local file system.
  delete db_;
  db_ = nullptr;
  aenv_.reset();
  DestroyDir(dbname_);
  DestroyDir("/tmp/ROCKSET");

  // Create new env.
  CreateCloudEnv();

  // Give env enough time to consume WALs
  std::this_thread::sleep_for(std::chrono::seconds(3));

  // Open DB.
  cloud_env_options_.keep_local_log_files = true;
  CloudEnvImpl* cimpl = static_cast<CloudEnvImpl*>(aenv_.get());
  options_.wal_dir = cimpl->GetWALCacheDir();
  OpenDB();

  // Test read.
  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), "Tele", &value));
  ASSERT_EQ(value, "Kinesis");

  CloseDB();
}

// Test whether we are able to recover nicely from two different writers to the
// same S3 bucket. (The feature that was enabled by CLOUDMANIFEST)
TEST_F(CloudTest, TwoDBsOneBucket) {
  auto firstDB = dbname_;
  auto secondDB = dbname_ + "-1";
  cloud_env_options_.keep_local_sst_files = true;
  std::string value;

  cloud_env_options_.resync_on_open = true;
  OpenDB();
  CloudEnvImpl* cimpl = static_cast<CloudEnvImpl*>(aenv_.get());
  auto firstManifestFile =
      aenv_->GetDestObjectPath() + "/" + cimpl->RemapFilename("MANIFEST-1");
  EXPECT_OK(aenv_->GetStorageProvider()->ExistsCloudObject(
      aenv_->GetDestBucketName(), firstManifestFile));
  // Create two files
  ASSERT_OK(db_->Put(WriteOptions(), "First", "File"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Put(WriteOptions(), "Second", "File"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  auto files = GetSSTFiles(dbname_);
  EXPECT_EQ(files.size(), 2);
  CloseDB();

  cloud_env_options_.resync_on_open = false;
  // Open again, with no destination bucket
  cloud_env_options_.dest_bucket.SetBucketName("");
  cloud_env_options_.dest_bucket.SetObjectPath("");
  OpenDB();
  ASSERT_OK(db_->Put(WriteOptions(), "Third", "File"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  auto newFiles = GetSSTFiles(dbname_);
  EXPECT_EQ(newFiles.size(), 3);
  // Remember the third file we created
  std::vector<std::string> diff;
  std::set_difference(newFiles.begin(), newFiles.end(), files.begin(),
                      files.end(), std::inserter(diff, diff.begin()));
  ASSERT_EQ(diff.size(), 1);
  auto thirdFile = diff[0];
  CloseDB();

  // Open in a different directory with destination bucket set
  dbname_ = secondDB;
  cloud_env_options_.dest_bucket = cloud_env_options_.src_bucket;
  cloud_env_options_.resync_on_open = true;
  OpenDB();
  ASSERT_OK(db_->Put(WriteOptions(), "Third", "DifferentFile"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  CloseDB();

  // Open back in the first directory with no destination
  dbname_ = firstDB;
  cloud_env_options_.dest_bucket.SetBucketName("");
  cloud_env_options_.dest_bucket.SetObjectPath("");
  cloud_env_options_.resync_on_open = false;
  OpenDB();
  // Changes to the cloud database should make no difference for us. This is an
  // important check because we should not reinitialize from the cloud if we
  // have a valid local directory!
  ASSERT_OK(db_->Get(ReadOptions(), "Third", &value));
  EXPECT_EQ(value, "File");
  CloseDB();

  // Reopen in the first directory, this time with destination path
  dbname_ = firstDB;
  cloud_env_options_.dest_bucket = cloud_env_options_.src_bucket;
  cloud_env_options_.resync_on_open = true;
  OpenDB();
  // Changes to the cloud database should be pulled down now.
  ASSERT_OK(db_->Get(ReadOptions(), "Third", &value));
  EXPECT_EQ(value, "DifferentFile");
  files = GetSSTFiles(dbname_);
  // Should no longer be in my directory because it's not part of the new
  // MANIFEST.
  EXPECT_TRUE(files.find(thirdFile) == files.end());

  // We need to sleep a bit because file deletion happens in a different thread,
  // so it might not be immediately deleted.
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  EXPECT_TRUE(
      aenv_->GetStorageProvider()
          ->ExistsCloudObject(aenv_->GetDestBucketName(), firstManifestFile)
          .IsNotFound());
  CloseDB();
}

// This test is similar to TwoDBsOneBucket, but is much more chaotic and illegal
// -- it runs two databases on exact same S3 bucket. The work on CLOUDMANIFEST
// enables us to run in that configuration for extended amount of time (1 hour
// by default) without any issues -- the last CLOUDMANIFEST writer wins.
// This test only applies when cookie is empty. So whenever db is reopened, it
// always fetches the latest CM/M files from s3
TEST_F(CloudTest, TwoConcurrentWritersCookieEmpty) {
  cloud_env_options_.resync_on_open = true;
  auto firstDB = dbname_;
  auto secondDB = dbname_ + "-1";

  DBCloud *db1, *db2;
  CloudEnv *aenv1, *aenv2;

  auto openDB1 = [&] {
    dbname_ = firstDB;
    OpenDB();
    db1 = db_;
    db_ = nullptr;
    aenv1 = aenv_.release();
  };
  auto openDB2 = [&] {
    dbname_ = secondDB;
    OpenDB();
    db2 = db_;
    db_ = nullptr;
    aenv2 = aenv_.release();
  };
  auto closeDB1 = [&] {
    db_ = db1;
    aenv_.reset(aenv1);
    CloseDB();
  };
  auto closeDB2 = [&] {
    db_ = db2;
    aenv_.reset(aenv2);
    CloseDB();
  };

  openDB1();
  openDB2();

  // Create bunch of files, reopening the databases during
  for (int i = 0; i < 5; ++i) {
    closeDB1();
    if (i == 2) {
      DestroyDir(firstDB);
    }
    // opening the database makes me a master (i.e. CLOUDMANIFEST points to my
    // manifest), my writes are applied to the shared space!
    openDB1();
    for (int j = 0; j < 5; ++j) {
      auto key = std::to_string(i) + std::to_string(j) + "1";
      ASSERT_OK(db1->Put(WriteOptions(), key, "FirstDB"));
      ASSERT_OK(db1->Flush(FlushOptions()));
    }
    closeDB2();
    if (i == 2) {
      DestroyDir(secondDB);
    }
    // opening the database makes me a master (i.e. CLOUDMANIFEST points to my
    // manifest), my writes are applied to the shared space!
    openDB2();
    for (int j = 0; j < 5; ++j) {
      auto key = std::to_string(i) + std::to_string(j) + "2";
      ASSERT_OK(db2->Put(WriteOptions(), key, "SecondDB"));
      ASSERT_OK(db2->Flush(FlushOptions()));
    }
  }

  dbname_ = firstDB;
  // This write should not be applied, because DB2 is currently the owner of the
  // S3 bucket
  ASSERT_OK(db1->Put(WriteOptions(), "ShouldNotBeApplied", ""));
  ASSERT_OK(db1->Flush(FlushOptions()));
  closeDB1();
  closeDB2();

  openDB1();
  for (int i = 0; i < 5; ++i) {
    for (int j = 0; j < 5; ++j) {
      std::string val;
      auto key = std::to_string(i) + std::to_string(j);
      ASSERT_OK(db1->Get(ReadOptions(), key + "1", &val));
      EXPECT_EQ(val, "FirstDB");
      ASSERT_OK(db1->Get(ReadOptions(), key + "2", &val));
      EXPECT_EQ(val, "SecondDB");
    }
  }

  std::string v;
  ASSERT_TRUE(db1->Get(ReadOptions(), "ShouldNotBeApplied", &v).IsNotFound());
  closeDB1();
}

// Creates a pure RocksDB database and makes sure we can migrate to RocksDB
// Cloud
TEST_F(CloudTest, MigrateFromPureRocksDB) {
  {  // Create local RocksDB
    Options options;
    options.create_if_missing = true;
    DB* dbptr;
    std::unique_ptr<DB> db;
    ASSERT_OK(DB::Open(options, dbname_, &dbptr));
    db.reset(dbptr);
    // create 5 files
    for (int i = 0; i < 5; ++i) {
      auto key = "key" + std::to_string(i);
      ASSERT_OK(db->Put(WriteOptions(), key, key));
      ASSERT_OK(db->Flush(FlushOptions()));
    }
  }

  CreateCloudEnv();
  ASSERT_OK(aenv_->MigrateFromPureRocksDB(dbname_));

  // Now open RocksDB cloud
  // TODO(dhruba) Figure out how to make this work without skipping dbid
  // verification
  cloud_env_options_.skip_dbid_verification = true;
  cloud_env_options_.keep_local_sst_files = true;
  cloud_env_options_.validate_filesize = false;
  OpenDB();
  for (int i = 5; i < 10; ++i) {
    auto key = "key" + std::to_string(i);
    ASSERT_OK(db_->Put(WriteOptions(), key, key));
    ASSERT_OK(db_->Flush(FlushOptions()));
  }

  for (int i = 0; i < 10; ++i) {
    auto key = "key" + std::to_string(i);
    std::string value;
    ASSERT_OK(db_->Get(ReadOptions(), key, &value));
    ASSERT_EQ(value, key);
  }
  CloseDB();
}

// Tests that we can open cloud DB without destination and source bucket set.
// This is useful for tests.
TEST_F(CloudTest, NoDestOrSrc) {
  DestroyDir(dbname_);
  cloud_env_options_.keep_local_sst_files = true;
  cloud_env_options_.src_bucket.SetBucketName("");
  cloud_env_options_.src_bucket.SetObjectPath("");
  cloud_env_options_.dest_bucket.SetBucketName("");
  cloud_env_options_.dest_bucket.SetObjectPath("");
  OpenDB();
  ASSERT_OK(db_->Put(WriteOptions(), "key", "value"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), "key", &value));
  ASSERT_EQ(value, "value");
  CloseDB();
  OpenDB();
  ASSERT_OK(db_->Get(ReadOptions(), "key", &value));
  ASSERT_EQ(value, "value");
  CloseDB();
}

TEST_F(CloudTest, PreloadCloudManifest) {
  DestroyDir(dbname_);
  // Put one key-value
  OpenDB();
  std::string value;
  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "World"));
  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_TRUE(value.compare("World") == 0);
  CloseDB();
  value.clear();

  // Reopen and validate, preload cloud manifest
  aenv_->PreloadCloudManifest(dbname_);

  OpenDB();
  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_EQ(value, "World");
}

//
// Test Ephemeral mode. In this mode, the database is cloned
// from a cloud bucket but new writes are not propagated
// back to any cloud bucket. Once cloned, all updates are local.
//
TEST_F(CloudTest, Ephemeral) {
  cloud_env_options_.keep_local_sst_files = true;
  options_.level0_file_num_compaction_trigger = 100;  // never compact

  // Create a primary DB with two files
  OpenDB();
  std::string value;
  std::string newdb1_dbid;
  std::set<uint64_t> cloud_files;
  ASSERT_OK(db_->Put(WriteOptions(), "Name", "dhruba"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Put(WriteOptions(), "Hello2", "borthakur"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  CloseDB();
  ASSERT_EQ(2, GetSSTFiles(dbname_).size());

  // Reopen the same database in ephemeral mode by cloning the original.
  // Do not destroy the local dir. Writes to this db does not make it back
  // to any cloud storage.
  {
    std::unique_ptr<CloudEnv> cloud_env;
    std::unique_ptr<DBCloud> cloud_db;
    CloneDB("db_ephemeral", "", "", &cloud_db, &cloud_env);

    // Retrieve the id of the first reopen
    ASSERT_OK(cloud_db->GetDbIdentity(newdb1_dbid));

    // verify that we still have two sst files
    ASSERT_EQ(2, GetSSTFilesClone("db_ephemeral").size());

    ASSERT_OK(cloud_db->Get(ReadOptions(), "Name", &value));
    ASSERT_EQ(value, "dhruba");
    ASSERT_OK(cloud_db->Get(ReadOptions(), "Hello2", &value));
    ASSERT_EQ(value, "borthakur");

    // Write one more record.
    // There should be 3 local sst files in the ephemeral db.
    ASSERT_OK(cloud_db->Put(WriteOptions(), "zip", "94087"));
    ASSERT_OK(cloud_db->Flush(FlushOptions()));
    ASSERT_EQ(3, GetSSTFilesClone("db_ephemeral").size());

    // check that cloud files did not get updated
    ASSERT_OK(GetCloudLiveFilesSrc(&cloud_files));
    ASSERT_EQ(2, cloud_files.size());
    cloud_files.clear();
  }

  // reopen main db and write two more records to it
  OpenDB();
  ASSERT_EQ(2, GetSSTFiles(dbname_).size());

  // write two more records to it.
  ASSERT_OK(db_->Put(WriteOptions(), "Key1", "onlyInMainDB"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Put(WriteOptions(), "Key2", "onlyInMainDB"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(4, GetSSTFiles(dbname_).size());
  CloseDB();
  ASSERT_OK(GetCloudLiveFilesSrc(&cloud_files));
  ASSERT_EQ(4, cloud_files.size());
  cloud_files.clear();

  // At this point, the main db has 4 files while the ephemeral
  // database has diverged earlier with 3 local files. If we try
  // to reopen the ephemeral clone, it should not download new
  // files from the cloud
  {
    std::unique_ptr<CloudEnv> cloud_env;
    std::unique_ptr<DBCloud> cloud_db;
    std::string dbid;
    options_.info_log = nullptr;
    CreateLoggerFromOptions(clone_dir_ + "/db_ephemeral", options_,
                            &options_.info_log);

    CloneDB("db_ephemeral", "", "", &cloud_db, &cloud_env);

    // Retrieve the id of this clone. It should be same as before
    ASSERT_OK(cloud_db->GetDbIdentity(dbid));
    ASSERT_EQ(newdb1_dbid, dbid);

    ASSERT_EQ(3, GetSSTFilesClone("db_ephemeral").size());

    // verify that a key written to the ephemeral db still exists
    ASSERT_OK(cloud_db->Get(ReadOptions(), "zip", &value));
    ASSERT_EQ(value, "94087");

    // verify that keys written to the main db after the ephemeral
    // was clones do not appear in the ephemeral db.
    ASSERT_NOK(cloud_db->Get(ReadOptions(), "Key1", &value));
    ASSERT_NOK(cloud_db->Get(ReadOptions(), "Key2", &value));
  }
}

// This test is performed in a rare race condition where ephemral clone is
// started after durable clone upload its CLOUDMANIFEST but before it uploads
// one of the MANIFEST. In this case, we want to verify that ephemeral clone is
// able to reinitialize instead of crash looping.
TEST_F(CloudTest, EphemeralOnCorruptedDB) {
  cloud_env_options_.keep_local_sst_files = true;
  cloud_env_options_.resync_on_open = true;
  options_.level0_file_num_compaction_trigger = 100;  // never compact

  OpenDB();

  std::vector<std::string> files;
  base_env_->GetChildren(dbname_, &files);

  // Get the MANIFEST file
  std::string manifest_file_name;
  for (const auto& file_name : files) {
    if (file_name.rfind("MANIFEST", 0) == 0) {
      manifest_file_name = file_name;
      break;
    }
  }

  ASSERT_FALSE(manifest_file_name.empty());

  // Delete MANIFEST file from S3 bucket.
  // This is to simulate the scenario where CLOUDMANIFEST is uploaded, but
  // MANIFEST is not yet uploaded from the durable shard.
  ASSERT_NE(aenv_.get(), nullptr);
  aenv_->GetStorageProvider()->DeleteCloudObject(
      aenv_->GetSrcBucketName(),
      aenv_->GetSrcObjectPath() + "/" + manifest_file_name);

  // Ephemeral clone should fail.
  std::unique_ptr<DBCloud> clone_db;
  std::unique_ptr<CloudEnv> cenv;
  Status st = CloneDB("clone1", "", "", &clone_db, &cenv);
  ASSERT_TRUE(st.IsCorruption());

  // Put the MANIFEST file back
  aenv_->GetStorageProvider()->PutCloudObject(
      dbname_ + "/" + manifest_file_name, aenv_->GetSrcBucketName(),
      aenv_->GetSrcObjectPath() + "/" + manifest_file_name);

  // Try one more time. This time it should succeed.
  clone_db.reset();
  cenv.reset();
  st = CloneDB("clone1", "", "", &clone_db, &cenv);
  ASSERT_OK(st);

  clone_db->Close();
  CloseDB();
}

//
// Test Ephemeral clones with resyncOnOpen mode.
// In this mode, every open of the ephemeral clone db causes its
// data to be resynced with the master db.
//
TEST_F(CloudTest, EphemeralResync) {
  cloud_env_options_.keep_local_sst_files = true;
  cloud_env_options_.resync_on_open = true;
  options_.level0_file_num_compaction_trigger = 100;  // never compact

  // Create a primary DB with two files
  OpenDB();
  std::string value;
  std::string newdb1_dbid;
  std::set<uint64_t> cloud_files;
  ASSERT_OK(db_->Put(WriteOptions(), "Name", "dhruba"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Put(WriteOptions(), "Hello2", "borthakur"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  CloseDB();
  ASSERT_EQ(2, GetSSTFiles(dbname_).size());

  // Reopen the same database in ephemeral mode by cloning the original.
  // Do not destroy the local dir. Writes to this db does not make it back
  // to any cloud storage.
  {
    std::unique_ptr<CloudEnv> cloud_env;
    std::unique_ptr<DBCloud> cloud_db;
    CloneDB("db_ephemeral", "", "", &cloud_db, &cloud_env);

    // Retrieve the id of the first reopen
    ASSERT_OK(cloud_db->GetDbIdentity(newdb1_dbid));

    // verify that we still have two sst files
    ASSERT_EQ(2, GetSSTFilesClone("db_ephemeral").size());

    ASSERT_OK(cloud_db->Get(ReadOptions(), "Name", &value));
    ASSERT_EQ(value, "dhruba");
    ASSERT_OK(cloud_db->Get(ReadOptions(), "Hello2", &value));
    ASSERT_EQ(value, "borthakur");

    // Write one more record.
    // There should be 3 local sst files in the ephemeral db.
    ASSERT_OK(cloud_db->Put(WriteOptions(), "zip", "94087"));
    ASSERT_OK(cloud_db->Flush(FlushOptions()));
    ASSERT_EQ(3, GetSSTFilesClone("db_ephemeral").size());

    // check that cloud files did not get updated
    ASSERT_OK(GetCloudLiveFilesSrc(&cloud_files));
    ASSERT_EQ(2, cloud_files.size());
    cloud_files.clear();
  }

  // reopen main db and write two more records to it
  OpenDB();
  ASSERT_EQ(2, GetSSTFiles(dbname_).size());

  // write two more records to it.
  ASSERT_OK(db_->Put(WriteOptions(), "Key1", "onlyInMainDB"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Put(WriteOptions(), "Key2", "onlyInMainDB"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(4, GetSSTFiles(dbname_).size());
  CloseDB();
  ASSERT_OK(GetCloudLiveFilesSrc(&cloud_files));
  ASSERT_EQ(4, cloud_files.size());
  cloud_files.clear();

  // At this point, the main db has 4 files while the ephemeral
  // database has diverged earlier with 3 local files.
  // Reopen the ephemeral db with resync_on_open flag.
  // This means that earlier updates to the ephemeral db are lost.
  // It also means that the most latest updates in the master db
  // are reflected in the newly opened ephemeral database.
  {
    std::unique_ptr<CloudEnv> cloud_env;
    std::unique_ptr<DBCloud> cloud_db;
    std::string dbid;
    options_.info_log = nullptr;
    CreateLoggerFromOptions(clone_dir_ + "/db_ephemeral", options_,
                            &options_.info_log);

    CloneDB("db_ephemeral", "", "", &cloud_db, &cloud_env);

    // Retrieve the id of this clone. It should be same as before
    ASSERT_OK(cloud_db->GetDbIdentity(dbid));
    ASSERT_EQ(newdb1_dbid, dbid);

    // verify that a key written to the ephemeral db does not exist
    ASSERT_NOK(cloud_db->Get(ReadOptions(), "zip", &value));

    // verify that keys written to the main db after the ephemeral
    // was clones appear in the ephemeral db.
    ASSERT_OK(cloud_db->Get(ReadOptions(), "Key1", &value));
    ASSERT_EQ(value, "onlyInMainDB");
    ASSERT_OK(cloud_db->Get(ReadOptions(), "Key2", &value));
    ASSERT_EQ(value, "onlyInMainDB");
  }
}

TEST_F(CloudTest, CheckpointToCloud) {
  cloud_env_options_.keep_local_sst_files = true;
  options_.level0_file_num_compaction_trigger = 100;  // never compact

  // Pre-create the bucket.
  CreateCloudEnv();
  aenv_.reset();

  // S3 is eventual consistency.
  std::this_thread::sleep_for(std::chrono::seconds(1));

  auto checkpoint_bucket = cloud_env_options_.dest_bucket;

  cloud_env_options_.src_bucket = BucketOptions();
  cloud_env_options_.dest_bucket = BucketOptions();

  // Create a DB with two files
  OpenDB();
  ASSERT_OK(db_->Put(WriteOptions(), "a", "b"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Put(WriteOptions(), "c", "d"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  ASSERT_OK(
      db_->CheckpointToCloud(checkpoint_bucket, CheckpointToCloudOptions()));

  ASSERT_EQ(2, GetSSTFiles(dbname_).size());
  CloseDB();

  DestroyDir(dbname_);

  cloud_env_options_.src_bucket = checkpoint_bucket;

  OpenDB();
  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), "a", &value));
  ASSERT_EQ(value, "b");
  ASSERT_OK(db_->Get(ReadOptions(), "c", &value));
  ASSERT_EQ(value, "d");
  CloseDB();

  aenv_->GetStorageProvider()->EmptyBucket(checkpoint_bucket.GetBucketName(),
                                           checkpoint_bucket.GetObjectPath());
}

// Basic test to copy object within S3.
TEST_F(CloudTest, CopyObjectTest) {
  CreateCloudEnv();

  // We need to open an empty DB in order for epoch to work.
  OpenDB();

  std::string content = "This is a test file";
  std::string fname = dbname_ + "/100000.sst";
  std::string dst_fname = dbname_ + "/200000.sst";

  {
    std::unique_ptr<WritableFile> writableFile;
    aenv_->NewWritableFile(fname, &writableFile, EnvOptions());
    writableFile->Append(content);
    writableFile->Fsync();
  }

  Status st = aenv_->GetStorageProvider()->CopyCloudObject(
      aenv_->GetSrcBucketName(), aenv_->RemapFilename(fname),
      aenv_->GetSrcBucketName(), dst_fname);
  ASSERT_OK(st);

  {
    std::unique_ptr<CloudStorageReadableFile> readableFile;
    st = aenv_->GetStorageProvider()->NewCloudReadableFile(
        aenv_->GetSrcBucketName(), dst_fname, &readableFile, EnvOptions());
    ASSERT_OK(st);

    char scratch[100];
    Slice result;
    std::unique_ptr<SequentialFile> sequentialFile(readableFile.release());
    st = sequentialFile->Read(100, &result, scratch);
    ASSERT_OK(st);
    ASSERT_EQ(19, result.size());
    ASSERT_EQ(result, Slice(content));
  }

  CloseDB();
}

//
// Verify that we can cache data from S3 in persistent cache.
//
TEST_F(CloudTest, PersistentCache) {
  std::string pcache = test::TmpDir() + "/persistent_cache";
  SetPersistentCache(pcache, 1);

  // Put one key-value
  OpenDB();
  std::string value;
  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "World"));
  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_TRUE(value.compare("World") == 0);
  CloseDB();
  value.clear();

  // Reopen and validate
  OpenDB();
  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_EQ(value, "World");
  CloseDB();
}

// This test create 2 DBs that shares a block cache. Ensure that reads from one
// DB do not get the values from the other DB.
TEST_F(CloudTest, SharedBlockCache) {
  cloud_env_options_.keep_local_sst_files = false;

  // Share the block cache.
  BlockBasedTableOptions bbto;
  bbto.block_cache = NewLRUCache(10 * 1024 * 1024);
  bbto.format_version = 4;
  options_.table_factory.reset(NewBlockBasedTableFactory(bbto));

  OpenDB();

  std::unique_ptr<CloudEnv> clone_env;
  std::unique_ptr<DBCloud> clone_db;
  CloneDB("newdb1", cloud_env_options_.src_bucket.GetBucketName(),
          cloud_env_options_.src_bucket.GetObjectPath() + "-clone", &clone_db,
          &clone_env, false /* force_keep_local_on_invalid_dest_bucket */);

  // Flush the first DB.
  db_->Put(WriteOptions(), "db", "original");
  db_->Flush(FlushOptions());

  // Flush the second DB.
  clone_db->Put(WriteOptions(), "db", "clone");
  clone_db->Flush(FlushOptions());

  std::vector<LiveFileMetaData> file_metadatas;
  db_->GetLiveFilesMetaData(&file_metadatas);
  ASSERT_EQ(1, file_metadatas.size());

  file_metadatas.clear();
  clone_db->GetLiveFilesMetaData(&file_metadatas);
  ASSERT_EQ(1, file_metadatas.size());

  std::string value;
  clone_db->Get(ReadOptions(), "db", &value);
  ASSERT_EQ("clone", value);

  db_->Get(ReadOptions(), "db", &value);
  ASSERT_EQ("original", value);

  // Cleanup
  clone_db->Close();
  CloseDB();
  clone_env->GetStorageProvider()->EmptyBucket(
      cloud_env_options_.src_bucket.GetBucketName(),
      cloud_env_options_.src_bucket.GetObjectPath() + "-clone");
}

// Verify that sst_file_cache and file_cache cannot be set together
TEST_F(CloudTest, KeepLocalFilesAndFileCache) {
  cloud_env_options_.sst_file_cache = NewLRUCache(1024);  // 1 KB cache
  cloud_env_options_.keep_local_sst_files = true;
  ASSERT_TRUE(checkOpen().IsInvalidArgument());
}

// Verify that sst_file_cache can be disabled
TEST_F(CloudTest, FileCacheZero) {
  cloud_env_options_.sst_file_cache = NewLRUCache(0);  // zero size
  OpenDB();
  CloudEnvImpl* cimpl = static_cast<CloudEnvImpl*>(aenv_.get());
  ASSERT_OK(db_->Put(WriteOptions(), "a", "b"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Put(WriteOptions(), "c", "d"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  auto local_files = GetSSTFiles(dbname_);
  EXPECT_EQ(local_files.size(), 0);
  EXPECT_EQ(cimpl->FileCacheGetCharge(), 0);

  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), "a", &value));
  ASSERT_TRUE(value.compare("b") == 0);
  ASSERT_OK(db_->Get(ReadOptions(), "c", &value));
  ASSERT_TRUE(value.compare("d") == 0);
  CloseDB();
}

// Verify that sst_file_cache is very small, so no files are local.
TEST_F(CloudTest, FileCacheSmall) {
  cloud_env_options_.sst_file_cache = NewLRUCache(10);  // Practically zero size
  OpenDB();
  CloudEnvImpl* cimpl = static_cast<CloudEnvImpl*>(aenv_.get());
  ASSERT_OK(db_->Put(WriteOptions(), "a", "b"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Put(WriteOptions(), "c", "d"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  auto local_files = GetSSTFiles(dbname_);
  EXPECT_EQ(local_files.size(), 0);
  EXPECT_EQ(cimpl->FileCacheGetCharge(), 0);
  CloseDB();
}

// Relatively large sst_file cache, so all files are local.
TEST_F(CloudTest, FileCacheLarge) {
  size_t capacity = 10240L;
  std::shared_ptr<Cache> cache = NewLRUCache(capacity);
  cloud_env_options_.sst_file_cache = cache;

  // generate two sst files.
  OpenDB();
  CloudEnvImpl* cimpl = static_cast<CloudEnvImpl*>(aenv_.get());
  ASSERT_OK(db_->Put(WriteOptions(), "a", "b"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Put(WriteOptions(), "c", "d"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // check that local sst files exist
  auto local_files = GetSSTFiles(dbname_);
  EXPECT_EQ(local_files.size(), 2);

  // check that local sst files have non zero size
  uint64_t totalFileSize = 0;
  GetSSTFilesTotalSize(dbname_, &totalFileSize);
  EXPECT_GT(totalFileSize, 0);
  EXPECT_GE(capacity, totalFileSize);

  // check that cache has two entries
  EXPECT_EQ(cimpl->FileCacheGetNumItems(), 2);

  // check that cache charge matches total local sst file size
  EXPECT_EQ(cimpl->FileCacheGetNumItems(), 2);
  EXPECT_EQ(cimpl->FileCacheGetCharge(), totalFileSize);
  CloseDB();
}

// Cache will have a few files only.
TEST_F(CloudTest, FileCacheOnDemand) {
  size_t capacity = 3000;
  int num_shard_bits = 0; // 1 shard
  bool strict_capacity_limit = false;
  double high_pri_pool_ratio = 0;

  std::shared_ptr<Cache> cache =
      NewLRUCache(capacity, num_shard_bits, strict_capacity_limit,
                  high_pri_pool_ratio, nullptr, kDefaultToAdaptiveMutex,
                  CacheMetadataChargePolicy::kDontChargeCacheMetadata);
  cloud_env_options_.sst_file_cache = cache;
  options_.level0_file_num_compaction_trigger = 100;  // never compact

  OpenDB();
  CloudEnvImpl* cimpl = static_cast<CloudEnvImpl*>(aenv_.get());

  // generate four sst files, each of size about 884 bytes
  ASSERT_OK(db_->Put(WriteOptions(), "a", "b"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Put(WriteOptions(), "c", "d"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Put(WriteOptions(), "e", "f"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Put(WriteOptions(), "g", "h"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // The db should have 4 sst files in the manifest.
  std::vector<LiveFileMetaData> flist;
  db_->GetLiveFilesMetaData(&flist);
  EXPECT_EQ(flist.size(), 4);

  // verify that there are only two entries in the cache
  EXPECT_EQ(cimpl->FileCacheGetNumItems(), 2);
  EXPECT_EQ(cimpl->FileCacheGetCharge(), cache->GetUsage());

  // There should be only two local sst files.
  auto local_files = GetSSTFiles(dbname_);
  EXPECT_EQ(local_files.size(), 2);

  CloseDB();
}

TEST_F(CloudTest, FindLiveFilesFetchManifestTest) {
  OpenDB();
  ASSERT_OK(db_->Put({}, "a", "1"));
  ASSERT_OK(db_->Flush({}));
  CloseDB();

  DestroyDir(dbname_);

  // recreate cloud env, which points to the same bucket and objectpath
  CreateCloudEnv();

  std::vector<std::string> live_sst_files;
  std::string manifest_file;

  // fetch and load CloudManifest
  ASSERT_OK(aenv_->PreloadCloudManifest(dbname_));

  // manifest file will be fetched to local db
  ASSERT_OK(aenv_->FindAllLiveFiles(dbname_, &live_sst_files, &manifest_file));
  EXPECT_EQ(live_sst_files.size(), 1);
}

TEST_F(CloudTest, FileModificationTimeTest) {
  OpenDB();
  ASSERT_OK(db_->Put({}, "a", "1"));
  ASSERT_OK(db_->Flush({}));
  std::vector<std::string> live_sst_files;
  std::string manifest_file;
  ASSERT_OK(aenv_->FindAllLiveFiles(dbname_, &live_sst_files, &manifest_file));
  uint64_t modtime1;
  ASSERT_OK(aenv_->GetFileModificationTime(dbname_ + pathsep + manifest_file,
                                           &modtime1));
  CloseDB();
  DestroyDir(dbname_);
  // don't roll cloud manifest so that manifest file epoch is not updated
  cloud_env_options_.roll_cloud_manifest_on_open = false;
  OpenDB();
  uint64_t modtime2;
  ASSERT_OK(aenv_->GetFileModificationTime(dbname_ + pathsep + manifest_file,
                                           &modtime2));
  // we read local file modification time, so the second time we open db, the
  // modification time is changed
  EXPECT_GT(modtime2, modtime1);
}

TEST_F(CloudTest, EmptyCookieTest) {
  // By default cookie is empty
  OpenDB();
  auto cenv_impl = static_cast<CloudEnvImpl*>(aenv_.get());
  auto cloud_manifest_file = cenv_impl->CloudManifestFile(dbname_);
  EXPECT_EQ(basename(cloud_manifest_file), "CLOUDMANIFEST");
  CloseDB();
}

TEST_F(CloudTest, NonEmptyCookieTest) {
  cloud_env_options_.new_cookie_on_open = "000001";
  OpenDB();
  std::string value;
  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "World"));
  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_EQ(value, "World");

  auto cloud_manifest_file =
      MakeCloudManifestFile(dbname_, cloud_env_options_.new_cookie_on_open);
  ASSERT_OK(aenv_->GetStorageProvider()->ExistsCloudObject(
      aenv_->GetSrcBucketName(), cloud_manifest_file));
  EXPECT_EQ(basename(cloud_manifest_file), "CLOUDMANIFEST-000001");
  CloseDB();
  DestroyDir(dbname_);
  cloud_env_options_.cookie_on_open = "000001";
  cloud_env_options_.new_cookie_on_open = "000001";
  OpenDB();

  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_EQ(value, "World");
  ASSERT_OK(aenv_->GetStorageProvider()->ExistsCloudObject(
      aenv_->GetSrcBucketName(), cloud_manifest_file));
  EXPECT_EQ(basename(cloud_manifest_file), "CLOUDMANIFEST-000001");
  CloseDB();
}

// Verify that live sst files are the same after applying cloud manifest delta
TEST_F(CloudTest, LiveFilesConsistentAfterApplyCloudManifestDeltaTest) {
  cloud_env_options_.cookie_on_open = "1";
  cloud_env_options_.new_cookie_on_open = "1";
  OpenDB();

  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "World"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  std::vector<std::string> live_sst_files1;
  std::string manifest_file1;
  ASSERT_OK(aenv_->FindAllLiveFiles(dbname_, &live_sst_files1, &manifest_file1));

  std::string new_cookie = "2";
  std::string new_epoch = "dca7f3e19212c4b3";
  auto delta = CloudManifestDelta{GetDBImpl()->GetNextFileNumber(), new_epoch};
  ASSERT_OK(GetCloudEnvImpl()->RollNewCookie(dbname_, new_cookie, delta));
  ASSERT_OK(GetCloudEnvImpl()->ApplyCloudManifestDelta(delta));

  std::vector<std::string> live_sst_files2;
  std::string manifest_file2;
  ASSERT_OK(aenv_->FindAllLiveFiles(dbname_, &live_sst_files2, &manifest_file2));

  EXPECT_EQ(live_sst_files1, live_sst_files2);
  EXPECT_NE(manifest_file1, manifest_file2);

  CloseDB();
}


// After calling `ApplyCloudManifestDelta`, writes should be persisted in
// sst files only visible in new Manifest
TEST_F(CloudTest, WriteAfterUpdateCloudManifestArePersistedInNewEpoch) {
  cloud_env_options_.cookie_on_open = "1";
  cloud_env_options_.new_cookie_on_open = "1";
  OpenDB();
  ASSERT_OK(db_->Put(WriteOptions(), "Hello1", "world1"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  std::string new_cookie = "2";
  std::string new_epoch = "dca7f3e19212c4b3";

  auto delta = CloudManifestDelta{GetDBImpl()->GetNextFileNumber(), new_epoch};
  ASSERT_OK(GetCloudEnvImpl()->RollNewCookie(dbname_, new_cookie, delta));
  ASSERT_OK(GetCloudEnvImpl()->ApplyCloudManifestDelta(delta));
  GetDBImpl()->NewManifestOnNextUpdate();

  // following writes are not visible for old cookie
  ASSERT_OK(db_->Put(WriteOptions(), "Hello2", "world2"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // reopen with cookie = 1, new updates after rolling are not visible
  CloseDB();
  cloud_env_options_.cookie_on_open = "1";
  cloud_env_options_.new_cookie_on_open = "1";
  cloud_env_options_.dest_bucket.SetBucketName("");
  cloud_env_options_.dest_bucket.SetObjectPath("");
  OpenDB();
  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), "Hello1", &value));
  EXPECT_EQ(value, "world1");
  EXPECT_NOK(db_->Get(ReadOptions(), "Hello2", &value));
  CloseDB();

  // reopen with cookie = 2, new updates should still be visible
  CloseDB();
  cloud_env_options_.cookie_on_open = "2";
  cloud_env_options_.new_cookie_on_open = "2";
  OpenDB();
  ASSERT_OK(db_->Get(ReadOptions(), "Hello1", &value));
  EXPECT_EQ(value, "world1");
  ASSERT_OK(db_->Get(ReadOptions(), "Hello2", &value));
  EXPECT_EQ(value, "world2");
  CloseDB();

  // Make sure that the changes in cloud are correct
  DestroyDir(dbname_);
  cloud_env_options_.cookie_on_open = "2";
  cloud_env_options_.new_cookie_on_open = "2";
  OpenDB();
  ASSERT_OK(db_->Get(ReadOptions(), "Hello1", &value));
  EXPECT_EQ(value, "world1");
  ASSERT_OK(db_->Get(ReadOptions(), "Hello2", &value));
  EXPECT_EQ(value, "world2");
  CloseDB();
}

// Test various cases of crashing in the middle during CloudManifestSwitch
TEST_F(CloudTest, CMSwitchCrashInMiddleTest) {
  cloud_env_options_.roll_cloud_manifest_on_open = false;
  cloud_env_options_.cookie_on_open = "1";

  SyncPoint::GetInstance()->SetCallBack(
      "CloudEnvImpl::RollNewCookie:AfterManifestCopy",
      [](void* arg) {
        // Simulate the case of crash in the middle of
        // RollNewCookie
        *reinterpret_cast<Status*>(arg) = Status::Aborted("Aborted");
      });

  SyncPoint::GetInstance()->EnableProcessing();

  // case 1: Crash in the middle of updating local manifest files
  // our guarantee: no CLOUDMANIFEST_new_cookie locally and remotely
  OpenDB();

  std::string new_cookie = "2";
  std::string new_epoch = "dca7f3e19212c4b3";

  ASSERT_NOK(GetCloudEnvImpl()->RollNewCookie(
      dbname_, new_cookie,
      CloudManifestDelta{GetDBImpl()->GetNextFileNumber(), new_epoch}));

  CloseDB();

  EXPECT_NOK(base_env_->FileExists(MakeCloudManifestFile(dbname_, new_cookie)));

  // case 2: Crash in the middle of uploading local manifest files
  // our guarantee: no CLOUDMANFIEST_cookie remotely
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "CloudEnvImpl::UploadManifest:AfterUploadManifest", [](void* arg) {
        // Simulate the case of crashing in the middle of
        // UploadManifest
        *reinterpret_cast<Status*>(arg) = Status::Aborted("Aborted");
      });
  SyncPoint::GetInstance()->EnableProcessing();
  OpenDB();

  auto delta = CloudManifestDelta{GetDBImpl()->GetNextFileNumber(), new_epoch};
  ASSERT_NOK(GetCloudEnvImpl()->RollNewCookie(dbname_, new_cookie, delta));

  ASSERT_NOK(GetCloudEnvImpl()->GetStorageProvider()->ExistsCloudObject(
      GetCloudEnvImpl()->GetDestBucketName(),
      MakeCloudManifestFile(GetCloudEnvImpl()->GetDestObjectPath(),
                            new_cookie)));

  CloseDB();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(CloudTest, RollNewEpochTest) {
  OpenDB();
  auto epoch1 = GetCloudEnvImpl()->GetCloudManifest()->GetCurrentEpoch();
  EXPECT_OK(GetCloudEnvImpl()->GetStorageProvider()->ExistsCloudObject(
      GetCloudEnvImpl()->GetDestBucketName(),
      ManifestFileWithEpoch(GetCloudEnvImpl()->GetDestObjectPath(), epoch1)));
  CloseDB();
  OpenDB();
  auto epoch2 = GetCloudEnvImpl()->GetCloudManifest()->GetCurrentEpoch();
  EXPECT_OK(GetCloudEnvImpl()->GetStorageProvider()->ExistsCloudObject(
      GetCloudEnvImpl()->GetDestBucketName(),
      ManifestFileWithEpoch(GetCloudEnvImpl()->GetDestObjectPath(), epoch2)));
  CloseDB();
  EXPECT_NE(epoch1, epoch2);
}

// Test that we can rollback to empty cookie
TEST_F(CloudTest, CookieBackwardsCompatibilityTest) {
  cloud_env_options_.resync_on_open = true;
  cloud_env_options_.roll_cloud_manifest_on_open = true;

  cloud_env_options_.cookie_on_open = "";
  cloud_env_options_.new_cookie_on_open = "1";
  OpenDB();
  ASSERT_OK(db_->Put({}, "k1", "v1"));
  ASSERT_OK(db_->Flush({}));
  CloseDB();

  // switch cookie
  cloud_env_options_.cookie_on_open = "1";
  cloud_env_options_.new_cookie_on_open = "2";
  OpenDB();
  std::string value;
  ASSERT_OK(db_->Get({}, "k1", &value));
  EXPECT_EQ(value, "v1");

  ASSERT_OK(db_->Put({}, "k2", "v2"));
  ASSERT_OK(db_->Flush({}));
  CloseDB();

  // switch back to empty cookie
  cloud_env_options_.cookie_on_open = "2";
  cloud_env_options_.new_cookie_on_open = "";
  OpenDB();
  ASSERT_OK(db_->Get({}, "k1", &value));
  EXPECT_EQ(value, "v1");

  ASSERT_OK(db_->Get({}, "k2", &value));
  EXPECT_EQ(value, "v2");
  CloseDB();

  // open with both cookies being empty
  cloud_env_options_.cookie_on_open = "";
  cloud_env_options_.new_cookie_on_open = "";
  OpenDB();
  ASSERT_OK(db_->Get({}, "k1", &value));
  EXPECT_EQ(value, "v1");

  ASSERT_OK(db_->Get({}, "k2", &value));
  EXPECT_EQ(value, "v2");
  CloseDB();
}

// Test that once we switch to non empty cookie, we can rollback to
// empty cookie immediately and files are not deleted mistakenly
TEST_F(CloudTest, CookieRollbackTest) {
  cloud_env_options_.resync_on_open = true;

  // Create CLOUDMANFIEST with empty cookie
  cloud_env_options_.cookie_on_open = "";
  cloud_env_options_.new_cookie_on_open = "";

  OpenDB();
  ASSERT_OK(db_->Put({}, "k1", "v1"));
  ASSERT_OK(db_->Flush({}));
  CloseDB();

  // Switch to cookie 1
  cloud_env_options_.cookie_on_open = "";
  cloud_env_options_.new_cookie_on_open = "1";
  OpenDB();
  CloseDB();

  // rollback to empty cookie
  cloud_env_options_.cookie_on_open = "1";
  cloud_env_options_.new_cookie_on_open = "";

  // Setup syncpoint so that file deletion jobs are executed after we open db,
  // but before we close db. This is to make sure that file deletion job
  // won't delete files that are created when we open db (e.g., CLOUDMANIFEST
  // files and MANIFEST files) and we can catch it in test if something is
  // messed up
  SyncPoint::GetInstance()->LoadDependency({
      {// only trigger file deletion job after db open
       "CloudTest::CookieRollbackTest:AfterOpenDB",
       "CloudSchedulerImpl::DoWork:BeforeGetJob"},
  });
  SyncPoint::GetInstance()->EnableProcessing();
  OpenDB();
  TEST_SYNC_POINT("CloudTest::CookieRollbackTest:AfterOpenDB");
  // File deletion jobs are only triggered after this. Once it's triggered,
  // the job deletion queue is not empty

  std::string v;
  ASSERT_OK(db_->Get({}, "k1", &v));
  EXPECT_EQ(v, "v1");

  // wait until no scheduled jobs for current local cloud env
  // After waiting, we know for sure that all the deletion jobs scheduled
  // when opening db are executed
  WaitUntilNoScheduledJobs();
  CloseDB();

  SyncPoint::GetInstance()->DisableProcessing();

  // reopen with empty cookie
  cloud_env_options_.cookie_on_open = "";
  cloud_env_options_.new_cookie_on_open = "";
  OpenDB();
  ASSERT_OK(db_->Get({}, "k1", &v));
  EXPECT_EQ(v, "v1");
  CloseDB();
}

TEST_F(CloudTest, NewCookieOnOpenTest) {
  cloud_env_options_.cookie_on_open = "1";

  // when opening new db, only new_cookie_on_open is used as CLOUDMANIFEST suffix
  cloud_env_options_.new_cookie_on_open = "2";
  OpenDB();
  ASSERT_OK(db_->Put({}, "k1", "v1"));
  ASSERT_OK(db_->Flush({}));

  ASSERT_NOK(aenv_->GetStorageProvider()->ExistsCloudObject(
      aenv_->GetSrcBucketName(), MakeCloudManifestFile(dbname_, "1")));
  // CLOUDMANIFEST-2 should exist since this is a new db
  ASSERT_OK(aenv_->GetStorageProvider()->ExistsCloudObject(
      aenv_->GetSrcBucketName(), MakeCloudManifestFile(dbname_, "2")));
  CloseDB();

  // reopen and switch cookie
  cloud_env_options_.cookie_on_open = "2";
  cloud_env_options_.new_cookie_on_open = "3";
  OpenDB();
  // CLOUDMANIFEST-3 is the new cloud manifest
  ASSERT_OK(aenv_->GetStorageProvider()->ExistsCloudObject(
      aenv_->GetSrcBucketName(), MakeCloudManifestFile(dbname_, "3")));

  std::string value;
  ASSERT_OK(db_->Get({}, "k1", &value));
  EXPECT_EQ(value, "v1");

  ASSERT_OK(db_->Put({}, "k2", "v2"));
  ASSERT_OK(db_->Flush({}));
  CloseDB();

  // reopen DB, but don't switch CLOUDMANIFEST
  cloud_env_options_.cookie_on_open = "3";
  cloud_env_options_.new_cookie_on_open = "3";
  OpenDB();
  ASSERT_OK(db_->Get({}, "k2", &value));
  EXPECT_EQ(value, "v2");
  CloseDB();
}

// Test invisible file deletion when db is opened.
TEST_F(CloudTest, InvisibleFileDeletionOnDBOpenTest) {
  std::string cookie1 = "", cookie2 = "-1-1";
  cloud_env_options_.keep_local_sst_files = true;

  // opening with cookie1
  OpenDB();
  ASSERT_OK(db_->Put({}, "k1", "v1"));
  ASSERT_OK(db_->Flush({}));
  std::vector<std::string> cookie1_sst_files;
  std::string cookie1_manifest_file;
  ASSERT_OK(aenv_->FindAllLiveFiles(dbname_, &cookie1_sst_files,
                                    &cookie1_manifest_file));
  ASSERT_EQ(cookie1_sst_files.size(), 1);
  CloseDB();

  // MANIFEST file path of cookie1
  auto cookie1_manifest_filepath = dbname_ + pathsep + cookie1_manifest_file;
  // CLOUDMANIFEST file path of cookie1
  auto cookie1_cm_filepath =
      MakeCloudManifestFile(dbname_, cloud_env_options_.cookie_on_open);
  // sst file path of cookie1
  auto cookie1_sst_filepath = dbname_ + pathsep + cookie1_sst_files[0];

  // opening with cookie1 and switch to cookie2
  cloud_env_options_.cookie_on_open = cookie1;
  cloud_env_options_.new_cookie_on_open = cookie2;
  OpenDB();
  ASSERT_OK(db_->Put({}, "k2", "v2"));
  ASSERT_OK(db_->Flush({}));
  // CM/M/sst files of cookie1 won't be deleted
  for (auto path :
       {cookie1_cm_filepath, cookie1_manifest_filepath, cookie1_sst_filepath}) {
    EXPECT_OK(aenv_->GetBaseEnv()->FileExists(path));
    EXPECT_OK(aenv_->GetStorageProvider()->ExistsCloudObject(
        aenv_->GetSrcBucketName(), path));
  }

  std::vector<std::string> cookie2_sst_files;
  std::string cookie2_manifest_file;
  ASSERT_OK(aenv_->FindAllLiveFiles(dbname_, &cookie2_sst_files,
                                    &cookie2_manifest_file));
  ASSERT_EQ(cookie2_sst_files.size(), 2);
  CloseDB();

  // MANIFEST file path of cookie2
  auto cookie2_manifest_filepath = dbname_ + pathsep + cookie2_manifest_file;
  // CLOUDMANIFEST file path of cookie2
  auto cookie2_cm_filepath =
      MakeCloudManifestFile(dbname_, cloud_env_options_.new_cookie_on_open);
  // find sst file path of cookie2
  auto cookie2_sst_filepath = dbname_ + pathsep + cookie2_sst_files[0];
  if (cookie2_sst_filepath == cookie1_sst_filepath) {
    cookie2_sst_filepath = dbname_ + pathsep + cookie2_sst_files[1];
  }

  // Now we reopen db with cookie1 to force deleting all files generated in
  // cookie2

  // number of file deletion jobs is executed
  std::atomic_int num_job_executed(0);

  // Syncpoint callback so that we can check when the files are actually
  // deleted(which is async)
  SyncPoint::GetInstance()->SetCallBack(
      "LocalCloudScheduler::ScheduleJob:AfterEraseJob", [&](void* /*arg*/) {
        num_job_executed++;
        if (num_job_executed == 3) {
          // CM/M/SST files of cookie2 are deleted in s3
          for (auto path : {cookie2_manifest_filepath, cookie2_cm_filepath,
                            cookie2_sst_filepath}) {
            EXPECT_NOK(aenv_->GetStorageProvider()->ExistsCloudObject(
                aenv_->GetSrcBucketName(), path));
          }
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  // reopen db with cookie1 will force all files generated in cookie2 to be
  // deleted
  cloud_env_options_.cookie_on_open = cookie1;
  cloud_env_options_.new_cookie_on_open = cookie1;
  OpenDB();
  // local obsolete CM/M/SST files will be deleted immediately
  // files in cloud will be deleted later (checked in the callback)
  for (auto path :
       {cookie2_cm_filepath, cookie2_manifest_filepath, cookie2_sst_filepath}) {
    EXPECT_NOK(aenv_->GetBaseEnv()->FileExists(path)) << path;
  }
  CloseDB();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  WaitUntilNoScheduledJobs();
  // Make sure that these files are indeed deleted
  EXPECT_EQ(num_job_executed, 3);
}

// Verify that when opening with `delete_cloud_invisible_files_on_open`, local
// files will be deleted while cloud files will be kept
TEST_F(CloudTest, DisableInvisibleFileDeletionOnOpenTest) {
  std::string cookie1 = "", cookie2 = "1";
  cloud_env_options_.keep_local_sst_files = true;
  cloud_env_options_.cookie_on_open = cookie1;
  cloud_env_options_.new_cookie_on_open = cookie1;

  // opening with cookie1
  OpenDB();
  // generate sst file with cookie1
  ASSERT_OK(db_->Put({}, "k1", "v1"));
  ASSERT_OK(db_->Flush({}));

  std::vector<std::string> cookie1_sst_files;
  std::string cookie1_manifest_file;
  ASSERT_OK(aenv_->FindAllLiveFiles(dbname_, &cookie1_sst_files,
                                    &cookie1_manifest_file));
  ASSERT_EQ(cookie1_sst_files.size(), 1);

  auto cookie1_manifest_filepath = dbname_ + pathsep + cookie1_manifest_file;
  auto cookie1_cm_filepath =
      MakeCloudManifestFile(dbname_, cloud_env_options_.cookie_on_open);
  auto cookie1_sst_filepath = dbname_ + pathsep + cookie1_sst_files[0];

  SwitchToNewCookie(cookie2);

  // generate sst file with cookie2
  ASSERT_OK(db_->Put({}, "k2", "v2"));
  ASSERT_OK(db_->Flush({}));

  std::vector<std::string> cookie2_sst_files;
  std::string cookie2_manifest_file;

  ASSERT_OK(aenv_->FindAllLiveFiles(dbname_, &cookie2_sst_files,
                                    &cookie2_manifest_file));
  ASSERT_EQ(cookie2_sst_files.size(), 2);

  // exclude cookie1_sst_files from cookie2_sst_files
  std::sort(cookie2_sst_files.begin(), cookie2_sst_files.end());
  std::set_difference(cookie2_sst_files.begin(), cookie2_sst_files.end(),
                      cookie1_sst_files.begin(), cookie1_sst_files.end(),
                      cookie2_sst_files.begin());
  cookie2_sst_files.resize(1);

  auto cookie2_manifest_filepath = dbname_ + pathsep + cookie2_manifest_file;
  auto cookie2_cm_filepath =
      MakeCloudManifestFile(dbname_, cookie2);
  auto cookie2_sst_filepath = dbname_ + pathsep + cookie2_sst_files[0];

  CloseDB();

  // reopen with cookie1 = "". cookie2 sst files are not visible
  cloud_env_options_.delete_cloud_invisible_files_on_open = false;
  OpenDB();
  // files from cookie2 are deleted locally but exists in s3
  for (auto path: {cookie2_cm_filepath, cookie2_manifest_filepath, cookie2_sst_filepath}) {
    EXPECT_NOK(aenv_->GetBaseEnv()->FileExists(path));
    EXPECT_OK(aenv_->GetStorageProvider()->ExistsCloudObject(
        aenv_->GetSrcBucketName(), path));
  }
  std::string value;
  EXPECT_OK(db_->Get({}, "k1", &value));
  EXPECT_NOK(db_->Get({}, "k2", &value));
  CloseDB();

  cloud_env_options_.cookie_on_open = cookie2;
  cloud_env_options_.new_cookie_on_open = cookie2;
  // reopen with cookie2 also works since it will fetch files from s3 directly
  OpenDB();
  EXPECT_OK(db_->Get({}, "k1", &value));
  EXPECT_OK(db_->Get({}, "k2", &value));
  CloseDB();
}

TEST_F(CloudTest, DisableObsoleteFileDeletionOnOpenTest) {
  // Generate a few obsolete files first
  options_.num_levels = 3;
  options_.level0_file_num_compaction_trigger = 3;
  options_.write_buffer_size = 110 << 10;  // 110KB
  options_.arena_block_size = 4 << 10;
  options_.keep_log_file_num = 1;
  options_.use_options_file = false;
  // put wal files into one directory so that we don't need to count number of local
  // wal files
  options_.wal_dir = dbname_ + "/wal";
  cloud_env_options_.keep_local_sst_files = true;
  // disable cm roll so that no new manifest files generated
  cloud_env_options_.roll_cloud_manifest_on_open = false;

  WriteOptions wo;
  wo.disableWAL = true;
  OpenDB();
  SwitchToNewCookie("");
  db_->DisableFileDeletions();

  std::vector<LiveFileMetaData> files;

  ASSERT_OK(db_->Put(wo, "k1", "v1"));
  ASSERT_OK(db_->Flush({}));
  ASSERT_OK(db_->Put(wo, "k1", "v2"));
  ASSERT_OK(db_->Flush({}));
  db_->GetLiveFilesMetaData(&files);
  ASSERT_EQ(files.size(), 2);

  auto local_files = GetAllLocalFiles();
  // CM, MANIFEST1, MANIFEST2, CURRENT, IDENTITY, 2 sst files, wal directory
  EXPECT_EQ(local_files.size(), 8);

  ASSERT_OK(GetDBImpl()->TEST_CompactRange(0, nullptr, nullptr, nullptr, true));

  files.clear();
  db_->GetLiveFilesMetaData(&files);
  ASSERT_EQ(files.size(), 1);

  local_files = GetAllLocalFiles();
  // obsolete files are not deleted, also one extra sst files generated after compaction
  EXPECT_EQ(local_files.size(), 9);

  CloseDB();

  options_.disable_delete_obsolete_files_on_open = true;
  OpenDB();
  // obsolete files are not deleted
  EXPECT_EQ(GetAllLocalFiles().size(), 8);
  // obsolete files are deleted!
  db_->EnableFileDeletions(false /* force */);
  EXPECT_EQ(GetAllLocalFiles().size(), 6);
  CloseDB();
}

// Verify invisible CLOUDMANIFEST file deleteion
TEST_F(CloudTest, CloudManifestFileDeletionTest) {

  // create CLOUDMANIFEST file in s3
  cloud_env_options_.cookie_on_open = "";
  cloud_env_options_.new_cookie_on_open = "";
  OpenDB();
  CloseDB();

  // create CLOUDMANIFEST-1 file in s3
  cloud_env_options_.cookie_on_open = "";
  cloud_env_options_.new_cookie_on_open = "1";
  OpenDB();
  CloseDB();

  // double check that the CM files are indeed created
  for (auto cookie: {"", "1"}) {
    EXPECT_OK(GetCloudEnvImpl()->GetStorageProvider()->ExistsCloudObject(
        GetCloudEnvImpl()->GetDestBucketName(),
        MakeCloudManifestFile(GetCloudEnvImpl()->GetDestObjectPath(), cookie)));
  }

  // set large file deletion delay so that files are not deleted immediately
  GetCloudEnvImpl()->TEST_SetFileDeletionDelay(std::chrono::hours(1));

  // now we reopen the db with empty cookie_on_open and new_cookie_on_open =
  // "1". Double check that CLOUDMANIFEST-1 is not deleted!
  OpenDB();
  EXPECT_EQ(GetCloudEnvImpl()->TEST_NumScheduledJobs(), 0);
  CloseDB();


  // switch to new cookie
  cloud_env_options_.cookie_on_open = "1";
  cloud_env_options_.new_cookie_on_open = "2";
  OpenDB();
  // double check that CLOUDMANIFEST is never deleted
  EXPECT_EQ(GetCloudEnvImpl()->TEST_NumScheduledJobs(), 0);
  CloseDB();
}

// verify that two writers with different cookies can write concurrently
TEST_F(CloudTest, TwoConcurrentWritersCookieNotEmpty) {
  auto firstDB = dbname_;
  auto secondDB = dbname_ + "-1";

  DBCloud *db1, *db2;
  CloudEnv *aenv1, *aenv2;

  auto openDB1 = [&] {
    dbname_ = firstDB;
    cloud_env_options_.cookie_on_open = "1";
    cloud_env_options_.new_cookie_on_open = "2";
    OpenDB();
    db1 = db_;
    db_ = nullptr;
    aenv1 = aenv_.release();
  };
  auto openDB1NoCookieSwitch = [&](const std::string& cookie) {
    dbname_ = firstDB;
    // when reopening DB1, we should set cookie_on_open = 2 to make sure
    // we are opening with the right CM/M files
    cloud_env_options_.cookie_on_open = cookie;
    cloud_env_options_.new_cookie_on_open = cookie;
    OpenDB();
    db1 = db_;
    db_ = nullptr;
    aenv1 = aenv_.release();
  };
  auto openDB2 = [&] {
    dbname_ = secondDB;
    cloud_env_options_.cookie_on_open = "2";
    cloud_env_options_.new_cookie_on_open = "3";
    OpenDB();
    db2 = db_;
    db_ = nullptr;
    aenv2 = aenv_.release();
  };
  auto openDB2NoCookieSwitch = [&](const std::string& cookie) {
    dbname_ = secondDB;
    // when reopening DB1, we should set cookie_on_open = 3 to make sure
    // we are opening with the right CM/M files
    cloud_env_options_.cookie_on_open = cookie;
    cloud_env_options_.new_cookie_on_open = cookie;
    OpenDB();
    db2 = db_;
    db_ = nullptr;
    aenv2 = aenv_.release();
  };
  auto closeDB1 = [&] {
    db_ = db1;
    aenv_.reset(aenv1);
    CloseDB();
  };
  auto closeDB2 = [&] {
    db_ = db2;
    aenv_.reset(aenv2);
    CloseDB();
  };

  openDB1();
  db1->Put({}, "k1", "v1");
  db1->Flush({});
  closeDB1();

  // cleanup memtable of db1 to make sure k1/v1 indeed exists in sst files
  DestroyDir(firstDB);
  openDB1NoCookieSwitch("2" /* cookie */);

  // opening DB2 and running concurrently
  openDB2();

  db1->Put({}, "k2", "v2");
  db1->Flush({});

  db2->Put({}, "k3", "v3");
  db2->Flush({});

  std::string v;
  ASSERT_OK(db1->Get({}, "k1", &v));
  EXPECT_EQ(v, "v1");
  ASSERT_OK(db2->Get({}, "k1", &v));
  EXPECT_EQ(v, "v1");

  ASSERT_OK(db1->Get({}, "k2", &v));
  EXPECT_EQ(v, "v2");
  // k2 is written in db1 after db2 is opened, so it's not visible by db2
  EXPECT_NOK(db2->Get({}, "k2", &v));

  // k3 is written in db2 after db1 is opened, so it's not visible by db1
  EXPECT_NOK(db1->Get({}, "k3", &v));
  ASSERT_OK(db2->Get({}, "k3", &v));
  EXPECT_EQ(v, "v3");

  closeDB1();
  closeDB2();

  // cleanup local state to make sure writes indeed exist in sst files
  DestroyDir(firstDB);
  DestroyDir(secondDB);

  // We can't reopen db with cookie=2 anymore, since that will remove all the
  // files for cookie=3. This is guaranteed since whenever we reopen db, we
  // always get the latest cookie from metadata store.
  openDB2NoCookieSwitch("3" /* cookie */);

  ASSERT_OK(db2->Get({}, "k1", &v));
  EXPECT_EQ(v, "v1");
  EXPECT_NOK(db2->Get({}, "k2", &v));
  ASSERT_OK(db2->Get({}, "k3", &v));
  EXPECT_EQ(v, "v3");
  closeDB2();
}

// if file deletion fails, db should still be reopend
TEST_F(CloudTest, FileDeletionFailureIgnoredTest) {
  std::string manifest_file_path;
  OpenDB();
  auto epoch = GetCloudEnvImpl()->GetCloudManifest()->GetCurrentEpoch();
  manifest_file_path = ManifestFileWithEpoch(dbname_, epoch);
  ASSERT_OK(db_->Put({}, "k1", "v1"));
  ASSERT_OK(db_->Flush({}));
  CloseDB();

  // bump the manifest epoch so that next time opening it, manifest file will be deleted
  OpenDB();
  CloseDB();

  // return error during file deletion
  SyncPoint::GetInstance()->SetCallBack(
      "CloudEnvImpl::DeleteLocalInvisibleFiles:AfterListLocalFiles",
      [](void* arg) {
        auto st = reinterpret_cast<Status*>(arg);
        *st =
            Status::Aborted("Manual abortion to simulate file listing failure");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  OpenDB();
  std::string v;
  ASSERT_OK(db_->Get({}, "k1", &v));
  EXPECT_EQ(v, "v1");
  // Due to the Aborted error we generated, the manifest file which should have
  // been deleted still exists.
  EXPECT_OK(aenv_->GetBaseEnv()->FileExists(manifest_file_path));
  CloseDB();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  // reopen the db should delete the obsolete manifest file after we cleanup syncpoint
  OpenDB();
  EXPECT_NOK(aenv_->GetBaseEnv()->FileExists(manifest_file_path));
  CloseDB();
}

// verify that as long as CloudEnv is destructed, the file delection jobs
// waiting in the queue will be canceled
TEST_F(CloudTest, FileDeletionJobsCanceledWhenCloudEnvDestructed) {
  std::string manifest_file_path;
  OpenDB();
  auto epoch = GetCloudEnvImpl()->GetCloudManifest()->GetCurrentEpoch();
  manifest_file_path = ManifestFileWithEpoch(dbname_, epoch);
  CloseDB();

  // bump epoch of manifest file so next open will delete previous manifest file
  OpenDB();
  CloseDB();

  // Setup syncpoint dependency to prevent cloud scheduler from executing file
  // deletion job in the queue until CloudEnv is destructed
  SyncPoint::GetInstance()->LoadDependency(
      {{"CloudTest::FileDeletionJobsCanceledWhenCloudEnvDestructed:"
        "AfterCloudEnvDestruction",
        "CloudSchedulerImpl::DoWork:BeforeGetJob"}});
  SyncPoint::GetInstance()->EnableProcessing();
  OpenDB();
  CloseDB();

  // delete CloudEnv will cancel all file deletion jobs in the queue
  aenv_.reset();

  // jobs won't be executed until after this point. But the file deletion job
  // in the queue should have already been canceled
  TEST_SYNC_POINT(
      "CloudTest::FileDeletionJobsCanceledWhenCloudEnvDestructed:"
      "AfterCloudEnvDestruction");

  SyncPoint::GetInstance()->DisableProcessing();

  // recreate cloud env to check s3 file existence
  CreateCloudEnv();

  // wait for a while so that the rest uncanceled jobs are indeed executed by
  // cloud scheduler.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // the old manifest file is still there!
  EXPECT_OK(aenv_->GetStorageProvider()->ExistsCloudObject(
      aenv_->GetSrcBucketName(), manifest_file_path));

  // reopen db to delete the old manifest file
  OpenDB();
  EXPECT_NOK(aenv_->GetStorageProvider()->ExistsCloudObject(
      aenv_->GetSrcBucketName(), manifest_file_path));
  CloseDB();
}

// The failure case of opening a corrupted db which doesn't have MANIFEST file
TEST_F(CloudTest, OpenWithManifestMissing) {
  cloud_env_options_.resync_on_open = true;
  cloud_env_options_.resync_manifest_on_open = true;
  OpenDB();
  auto epoch = GetCloudEnvImpl()->GetCloudManifest()->GetCurrentEpoch();
  CloseDB();

  // Remove the MANIFEST file from s3
  ASSERT_OK(aenv_->GetStorageProvider()->DeleteCloudObject(
      aenv_->GetSrcBucketName(),
      ManifestFileWithEpoch(aenv_->GetSrcObjectPath(), epoch)));
  DestroyDir(dbname_);

  EXPECT_TRUE(checkOpen().IsCorruption());
}

// verify that ephemeral clone won't reference old sst file if it's reopened
// after sst file deletion on durable
// Ordering of events:
// - open durable (epoch = 1)
// - open ephemeral (epoch = 1, new_epoch=?)
// - durable delete sst files
// - reopen ephemeral (epoch = 1)
TEST_F(CloudTest, ReopenEphemeralAfterFileDeletion) {
  cloud_env_options_.resync_on_open = true;
  cloud_env_options_.keep_local_sst_files = false;

  auto durableDBName = dbname_;

  DBCloud *durable, *ephemeral;
  CloudEnv *durableEnv, *ephemeralEnv;
  std::vector<ColumnFamilyHandle*> durableHandles;

  auto openDurable = [&] {
    dbname_ = durableDBName;

    OpenDB(&durableHandles);
    durable = db_;
    db_ = nullptr;
    durableEnv = aenv_.release();
  };

  auto openEphemeral = [&] {
    std::unique_ptr<CloudEnv> cloud_env;
    std::unique_ptr<DBCloud> cloud_db;
    // open ephemeral clone with force_keep_local_on_invalid_dest_bucket=false
    // so that sst files are not kept locally
    ASSERT_OK(CloneDB("ephemeral" /* clone_name */, "" /* dest_bucket_name */,
                      "" /* dest_object_path */, &cloud_db, &cloud_env,
                      false /* force_keep_local_on_invalid_dest_bucket */));
    ephemeral = cloud_db.release();
    ephemeralEnv = cloud_env.release();
  };

  auto closeDurable = [&] {
    db_ = durable;
    aenv_.reset(durableEnv);
    CloseDB(&durableHandles);
  };

  auto closeEphemeral = [&] {
    db_ = ephemeral;
    aenv_.reset(ephemeralEnv);
    CloseDB();
  };

  options_.disable_auto_compactions = true;
  openDurable();

  ASSERT_OK(durable->Put({}, "key1", "val1"));
  ASSERT_OK(durable->Flush({}));

  ASSERT_OK(durable->Put({}, "key1", "val2"));
  ASSERT_OK(durable->Flush({}));

  closeDurable();

  openDurable();
  openEphemeral();

  std::vector<LiveFileMetaData> files;
  durable->GetLiveFilesMetaData(&files);
  ASSERT_EQ(files.size(), 2);
  // trigger compaction on durable with trivial file moves disabled, which will delete previously generated sst files
  ASSERT_OK(
      static_cast<DBImpl*>(durable->GetBaseDB())
          ->TEST_CompactRange(0, nullptr, nullptr, durableHandles[0], true));
  files.clear();
  durable->GetLiveFilesMetaData(&files);
  ASSERT_EQ(files.size(), 1);

  // reopen ephemeral
  closeEphemeral();
  openEphemeral();

  std::string val;
  ASSERT_OK(ephemeral->Get({}, "key1", &val));
  EXPECT_EQ(val, "val2");
  closeEphemeral();
  closeDurable();
}

TEST_F(CloudTest, SanitizeDirectoryTest) {
  cloud_env_options_.keep_local_sst_files = true;
  OpenDB();
  ASSERT_OK(db_->Put({}, "k1", "v1"));
  ASSERT_OK(db_->Flush({}));
  CloseDB();

  auto local_files = GetAllLocalFiles();
  // Files exist locally: cm/m, sst, options-xxx, xxx.log, identity, current
  EXPECT_EQ(local_files.size(), 7);

  EXPECT_OK(GetCloudEnvImpl()->SanitizeDirectory(
    options_, dbname_, false));
  
  // cleaning up during sanitization not triggered
  EXPECT_EQ(local_files.size(), GetAllLocalFiles().size());

  // Delete the local CLOUDMANIFEST file to force cleaning up
  ASSERT_OK(
      base_env_->DeleteFile(MakeCloudManifestFile(dbname_, "" /* cooke */)));
  
  EXPECT_OK(GetCloudEnvImpl()->SanitizeDirectory(
    options_, dbname_, false));
  
  local_files = GetAllLocalFiles();
  // IDENTITY file is downloaded after cleaning up, which is the only file that
  // exists locally
  EXPECT_EQ(GetAllLocalFiles().size(), 1);

  // reinitialize local directory
  OpenDB();
  CloseDB();
  local_files = GetAllLocalFiles();
  // we have two local MANIFEST files after opening second time.
  EXPECT_EQ(local_files.size(), 8);

  // create some random directory, which is expected to be not deleted
  ASSERT_OK(base_env_->CreateDir(dbname_ + "/tmp_writes"));

  // Delete the local CLOUDMANIFEST file to force cleaning up
  ASSERT_OK(
      base_env_->DeleteFile(MakeCloudManifestFile(dbname_, "" /* cooke */)));

  ASSERT_OK(GetCloudEnvImpl()->SanitizeDirectory(options_, dbname_, false));

  // IDENTITY file + the random directory we created
  EXPECT_EQ(GetAllLocalFiles().size(), 2);

  // reinitialize local directory
  OpenDB();
  CloseDB();

  // inject io errors during cleaning up. The io errors should be ignored
  SyncPoint::GetInstance()->SetCallBack(
    "CloudEnvImpl::SanitizeDirectory:AfterDeleteFile",
    [](void* arg) {
        auto st = reinterpret_cast<Status*>(arg);
        *st = Status::IOError("Inject io error during cleaning up");
    });
  
  SyncPoint::GetInstance()->EnableProcessing();
  // Delete the local CLOUDMANIFEST file to force cleaning up
  ASSERT_OK(
      base_env_->DeleteFile(MakeCloudManifestFile(dbname_, "" /* cooke */)));

  ASSERT_OK(GetCloudEnvImpl()->SanitizeDirectory(options_, dbname_, false));
  SyncPoint::GetInstance()->DisableProcessing();
}

}  //  namespace ROCKSDB_NAMESPACE

// A black-box test for the cloud wrapper around rocksdb
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else  // USE_AWS

#include <stdio.h>

int main(int, char**) {
  fprintf(stderr,
          "SKIPPED as DBCloud is supported only when USE_AWS is defined.\n");
  return 0;
}
#endif

#else  // ROCKSDB_LITE

#include <stdio.h>

int main(int, char**) {
  fprintf(stderr, "SKIPPED as DBCloud is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // !ROCKSDB_LITE
