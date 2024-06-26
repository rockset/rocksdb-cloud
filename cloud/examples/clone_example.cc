// Copyright (c) 2017-present, Rockset, Inc.  All rights reserved.
#include <cstdio>
#include <iostream>
#include <string>

#include "rocksdb/cloud/db_cloud.h"
#include "rocksdb/options.h"

using namespace ROCKSDB_NAMESPACE;

// This is the local directory where the db is stored. The same
// path name is used to store data inside the specified cloud
// storage bucket.
std::string kDBPath = "/tmp/rocksdb_main_db";

// This is the local directory where the clone is stored. The same
// pathname is used to store data in the specified cloud bucket.
std::string kClonePath = "/tmp/rocksdb_clone_db";

//
// This is the name of the cloud storage bucket where the db
// is made durable. If you are using AWS, you have to manually
// ensure that this bucket name is unique to you and does not
// conflict with any other S3 users who might have already created
// this bucket name.
// In this example, the database and its clone are both stored in
// the same bucket (obviously with different pathnames).
//
std::string kBucketSuffix = "cloud.clone.example.";
std::string kRegion = "us-west-2";

//
// Creates and Opens a clone
//
Status CloneDB(const std::string& clone_name, const std::string& src_bucket,
               const std::string& src_object_path,
               const std::string& dest_bucket,
               const std::string& dest_object_path,
               const CloudFileSystemOptions& cloud_fs_options,
               std::unique_ptr<DB>* cloud_db, std::unique_ptr<Env>* cloud_env) {
  // The local directory where the clone resides
  std::string cname = kClonePath + "/" + clone_name;

  // Create new AWS env
  CloudFileSystem* cfs;
  Status st = CloudFileSystem::NewAwsFileSystem(
      FileSystem::Default(), src_bucket, src_object_path, kRegion, dest_bucket,
      dest_object_path, kRegion, cloud_fs_options, nullptr, &cfs);
  if (!st.ok()) {
    fprintf(stderr,
            "Unable to create an AWS environment with "
            "bucket %s",
            src_bucket.c_str());
    return st;
  }
  std::shared_ptr<FileSystem> fs(cfs);
  *cloud_env = NewCompositeEnv(fs);

  // Create options and use the AWS env that we created earlier
  Options options;
  options.env = cloud_env->get();

  // No persistent cache
  std::string persistent_cache = "";

  // create a bucket name for debugging purposes
  const std::string bucketName = cfs->GetSrcBucketName();

  // open clone
  DBCloud* db;
  st = DBCloud::Open(options, kClonePath, persistent_cache, 0, &db);
  if (!st.ok()) {
    fprintf(stderr, "Unable to open clone at path %s in bucket %s. %s\n",
            kClonePath.c_str(), bucketName.c_str(), st.ToString().c_str());
    return st;
  }
  cloud_db->reset(db);
  return Status::OK();
}

int main() {
  // cloud environment config options here
  CloudFileSystemOptions cloud_fs_options;

  cloud_fs_options.credentials.InitializeSimple(
      getenv("AWS_ACCESS_KEY_ID"), getenv("AWS_SECRET_ACCESS_KEY"));
  if (!cloud_fs_options.credentials.HasValid().ok()) {
    fprintf(
        stderr,
        "Please set env variables "
        "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY with cloud credentials");
    return -1;
  }

  // Append the user name to the bucket name in an attempt to make it
  // globally unique. S3 bucket-namess need to be globlly unique.
  // If you want to rerun this example, then unique user-name suffix here.
  char* user = getenv("USER");
  kBucketSuffix.append(user);

  const std::string bucketPrefix = "rockset.";
  // create a bucket name for debugging purposes
  const std::string bucketName = bucketPrefix + kBucketSuffix;

  // Needed if using bucket prefix other than the default "rockset."
  cloud_fs_options.src_bucket.SetBucketName(kBucketSuffix, bucketPrefix);
  cloud_fs_options.dest_bucket.SetBucketName(kBucketSuffix, bucketPrefix);

  // Create a new AWS cloud env Status
  CloudFileSystem* cfs;
  Status s = CloudFileSystem::NewAwsFileSystem(
      FileSystem::Default(), kBucketSuffix, kDBPath, kRegion, kBucketSuffix,
      kDBPath, kRegion, cloud_fs_options, nullptr, &cfs);
  if (!s.ok()) {
    fprintf(stderr, "Unable to create cloud env in bucket %s. %s\n",
            bucketName.c_str(), s.ToString().c_str());
    return -1;
  }

  // Store a reference to a cloud env. A new cloud env object should be
  // associated with every new cloud-db.
  auto cloud_env = NewCompositeEnv(std::shared_ptr<FileSystem>(cfs));

  // Create options and use the AWS env that we created earlier
  Options options;
  options.env = cloud_env.get();
  options.create_if_missing = true;

  // No persistent cache
  std::string persistent_cache = "";

  // Create and Open DB
  DBCloud* db;
  s = DBCloud::Open(options, kDBPath, persistent_cache, 0, &db);
  if (!s.ok()) {
    fprintf(stderr, "Unable to open db at path %s in bucket %s. %s\n",
            kDBPath.c_str(), bucketName.c_str(), s.ToString().c_str());
    return -1;
  }

  // Put key-value into main db
  s = db->Put(WriteOptions(), "key1", "value");
  assert(s.ok());
  std::string value;

  // get value from main db
  s = db->Get(ReadOptions(), "key1", &value);
  assert(s.ok());
  assert(value == "value");

  // Flush all data from main db to sst files.
  db->Flush(FlushOptions());

  // Create a clone of the db and and verify that all's well.
  // In real applications, a Clone would typically be created
  // by a separate process.
  std::unique_ptr<DB> clone_db;
  std::unique_ptr<Env> clone_env;

  s = CloneDB("clone1", kBucketSuffix, kDBPath, kBucketSuffix, kClonePath,
              cloud_fs_options, &clone_db, &clone_env);
  if (!s.ok()) {
    fprintf(stderr, "Unable to clone db at path %s in bucket %s. %s\n",
            kDBPath.c_str(), bucketName.c_str(), s.ToString().c_str());
    return -1;
  }

  // insert a key-value in the clone.
  s = clone_db->Put(WriteOptions(), "name", "dhruba");
  assert(s.ok());

  // assert that values from the main db appears in the clone
  s = clone_db->Get(ReadOptions(), "key1", &value);
  assert(s.ok());
  assert(value == "value");

  // assert that the write to the clone does not appear in the main db
  s = db->Get(ReadOptions(), "name", &value);
  assert(s.IsNotFound());

  // Flush all data from clone db to sst files. Release clone.
  clone_db->Flush(FlushOptions());
  clone_db.release();
  delete db;

  fprintf(stdout, "Successfully used db at %s and clone at %s in bucket %s.\n",
          kDBPath.c_str(), kClonePath.c_str(), bucketName.c_str());
  return 0;
}
