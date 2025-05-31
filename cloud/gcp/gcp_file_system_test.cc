// Copyright (c) 2017 Rockset

#ifndef ROCKSDB_LITE

#include <cstdio>

#ifdef USE_GCP

#include "cloud/cloud_log_controller_impl.h"
#include "rocksdb/cloud/cloud_file_system.h"
#include "rocksdb/cloud/cloud_log_controller.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/cloud/cloud_storage_provider_impl.h"
#include "rocksdb/convenience.h"
#include "rocksdb/env.h"
#include "test_util/testharness.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

TEST(CloudFileSystemTest, TestBucket) {
  CloudFileSystemOptions copts;
  copts.src_bucket.SetRegion("North");
  copts.src_bucket.SetBucketName("Input", "src.");
  ASSERT_FALSE(copts.src_bucket.IsValid());
  copts.src_bucket.SetObjectPath("Here");
  ASSERT_TRUE(copts.src_bucket.IsValid());

  copts.dest_bucket.SetRegion("South");
  copts.dest_bucket.SetObjectPath("There");
  ASSERT_FALSE(copts.dest_bucket.IsValid());
  copts.dest_bucket.SetBucketName("Output", "dest.");
  ASSERT_TRUE(copts.dest_bucket.IsValid());
}

TEST(CloudFileSystemTest, ConfigureOptions) {
  ConfigOptions config_options;
  CloudFileSystemOptions copts, copy;
  copts.keep_local_sst_files = false;
  copts.keep_local_log_files = false;
  copts.create_bucket_if_missing = false;
  copts.validate_filesize = false;
  copts.skip_dbid_verification = false;
  copts.resync_on_open = false;
  copts.skip_cloud_files_in_getchildren = false;
  copts.constant_sst_file_size_in_sst_file_manager = 100;
  copts.run_purger = false;
  copts.purger_periodicity_millis = 101;

  std::string str;
  ASSERT_OK(copts.Serialize(config_options, &str));
  ASSERT_OK(copy.Configure(config_options, str));
  ASSERT_FALSE(copy.keep_local_sst_files);
  ASSERT_FALSE(copy.keep_local_log_files);
  ASSERT_FALSE(copy.create_bucket_if_missing);
  ASSERT_FALSE(copy.validate_filesize);
  ASSERT_FALSE(copy.skip_dbid_verification);
  ASSERT_FALSE(copy.resync_on_open);
  ASSERT_FALSE(copy.skip_cloud_files_in_getchildren);
  ASSERT_FALSE(copy.run_purger);
  ASSERT_EQ(copy.constant_sst_file_size_in_sst_file_manager, 100);
  ASSERT_EQ(copy.purger_periodicity_millis, 101);

  // Now try a different value
  copts.keep_local_sst_files = true;
  copts.keep_local_log_files = true;
  copts.create_bucket_if_missing = true;
  copts.validate_filesize = true;
  copts.skip_dbid_verification = true;
  copts.resync_on_open = true;
  copts.skip_cloud_files_in_getchildren = true;
  copts.constant_sst_file_size_in_sst_file_manager = 200;
  copts.run_purger = true;
  copts.purger_periodicity_millis = 201;

  ASSERT_OK(copts.Serialize(config_options, &str));
  ASSERT_OK(copy.Configure(config_options, str));
  ASSERT_TRUE(copy.keep_local_sst_files);
  ASSERT_TRUE(copy.keep_local_log_files);
  ASSERT_TRUE(copy.create_bucket_if_missing);
  ASSERT_TRUE(copy.validate_filesize);
  ASSERT_TRUE(copy.skip_dbid_verification);
  ASSERT_TRUE(copy.resync_on_open);
  ASSERT_TRUE(copy.skip_cloud_files_in_getchildren);
  ASSERT_TRUE(copy.run_purger);
  ASSERT_EQ(copy.constant_sst_file_size_in_sst_file_manager, 200);
  ASSERT_EQ(copy.purger_periodicity_millis, 201);
}

TEST(CloudFileSystemTest, ConfigureBucketOptions) {
  ConfigOptions config_options;
  CloudFileSystemOptions copts, copy;
  std::string str;
  copts.src_bucket.SetBucketName("source", "src.");
  copts.src_bucket.SetObjectPath("foo");
  copts.src_bucket.SetRegion("north");
  copts.dest_bucket.SetBucketName("dest");
  copts.dest_bucket.SetObjectPath("bar");
  ASSERT_OK(copts.Serialize(config_options, &str));

  ASSERT_OK(copy.Configure(config_options, str));
  ASSERT_EQ(copts.src_bucket.GetBucketName(), copy.src_bucket.GetBucketName());
  ASSERT_EQ(copts.src_bucket.GetObjectPath(), copy.src_bucket.GetObjectPath());
  ASSERT_EQ(copts.src_bucket.GetRegion(), copy.src_bucket.GetRegion());

  ASSERT_EQ(copts.dest_bucket.GetBucketName(),
            copy.dest_bucket.GetBucketName());
  ASSERT_EQ(copts.dest_bucket.GetObjectPath(),
            copy.dest_bucket.GetObjectPath());
  ASSERT_EQ(copts.dest_bucket.GetRegion(), copy.dest_bucket.GetRegion());
}

TEST(CloudFileSystemTest, ConfigureEnv) {
  std::unique_ptr<CloudFileSystem> cfs;

  ConfigOptions config_options;
  config_options.invoke_prepare_options = false;
  ASSERT_OK(CloudFileSystemEnv::CreateFromString(
      config_options, "keep_local_sst_files=true", &cfs));
  ASSERT_NE(cfs, nullptr);
  ASSERT_STREQ(cfs->Name(), "cloud");
  auto copts = cfs->GetOptions<CloudFileSystemOptions>();
  ASSERT_NE(copts, nullptr);
  ASSERT_TRUE(copts->keep_local_sst_files);
}

TEST(CloudFileSystemTest, TestInitialize) {
  std::unique_ptr<CloudFileSystem> cfs;
  BucketOptions bucket;
  ConfigOptions config_options;
  config_options.invoke_prepare_options = false;
  ASSERT_OK(CloudFileSystemEnv::CreateFromString(
      config_options, "id=cloud; TEST=cloudenvtest:/test/path", &cfs));
  ASSERT_NE(cfs, nullptr);
  ASSERT_STREQ(cfs->Name(), "cloud");

  ASSERT_TRUE(StartsWith(cfs->GetSrcBucketName(),
                         bucket.GetBucketPrefix() + "cloudenvtest."));
  ASSERT_EQ(cfs->GetSrcObjectPath(), "/test/path");
  ASSERT_TRUE(cfs->SrcMatchesDest());

  ASSERT_OK(CloudFileSystemEnv::CreateFromString(
      config_options, "id=cloud; TEST=cloudenvtest2:/test/path2?here", &cfs));
  ASSERT_NE(cfs, nullptr);
  ASSERT_STREQ(cfs->Name(), "cloud");
  ASSERT_TRUE(StartsWith(cfs->GetSrcBucketName(),
                         bucket.GetBucketPrefix() + "cloudenvtest2."));
  ASSERT_EQ(cfs->GetSrcObjectPath(), "/test/path2");
  ASSERT_EQ(cfs->GetCloudFileSystemOptions().src_bucket.GetRegion(), "here");
  ASSERT_TRUE(cfs->SrcMatchesDest());

  ASSERT_OK(CloudFileSystemEnv::CreateFromString(
      config_options,
      "id=cloud; TEST=cloudenvtest3:/test/path3; "
      "src.bucket=my_bucket; dest.object=/my_path",
      &cfs));
  ASSERT_NE(cfs, nullptr);
  ASSERT_STREQ(cfs->Name(), "cloud");
  ASSERT_EQ(cfs->GetSrcBucketName(), bucket.GetBucketPrefix() + "my_bucket");
  ASSERT_EQ(cfs->GetSrcObjectPath(), "/test/path3");
  ASSERT_TRUE(StartsWith(cfs->GetDestBucketName(),
                         bucket.GetBucketPrefix() + "cloudenvtest3."));
  ASSERT_EQ(cfs->GetDestObjectPath(), "/my_path");
}

TEST(CloudFileSystemTest, ConfigureGcpEnv) {
  std::unique_ptr<CloudFileSystem> cfs;

  ConfigOptions config_options;
  Status s = CloudFileSystemEnv::CreateFromString(
      config_options, "id=gcp; keep_local_sst_files=true", &cfs);
#ifdef USE_GCP
  ASSERT_OK(s);
  ASSERT_NE(cfs, nullptr);
  ASSERT_STREQ(cfs->Name(), "gcp");
  auto copts = cfs->GetOptions<CloudFileSystemOptions>();
  ASSERT_NE(copts, nullptr);
  ASSERT_TRUE(copts->keep_local_sst_files);
  ASSERT_NE(cfs->GetStorageProvider(), nullptr);
  ASSERT_STREQ(cfs->GetStorageProvider()->Name(),
               CloudStorageProviderImpl::kGcs());
#else
  ASSERT_NOK(s);
  ASSERT_EQ(cfs, nullptr);
#endif
}

TEST(CloudFileSystemTest, ConfigureGcsProvider) {
  std::unique_ptr<CloudFileSystem> cfs;

  ConfigOptions config_options;
  Status s = CloudFileSystemEnv::CreateFromString(config_options,
                                                  "provider=gcs", &cfs);
  ASSERT_NOK(s);
  ASSERT_EQ(cfs, nullptr);

#ifdef USE_GCP
  ASSERT_OK(CloudFileSystemEnv::CreateFromString(config_options,
                                                 "id=gcp; provider=gcs", &cfs));
  ASSERT_STREQ(cfs->Name(), "gcp");
  ASSERT_NE(cfs->GetStorageProvider(), nullptr);
  ASSERT_STREQ(cfs->GetStorageProvider()->Name(),
               CloudStorageProviderImpl::kGcs());
#endif
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else  // USE_GCP
#include <stdio.h>

int main(int, char**) {
  fprintf(stderr,
          "SKIPPED as DBCloud is supported only when USE_GCP is defined.\n");
  return 0;
}
#endif  // USE_GCP

#else  // ROCKSDB_LITE
#include <stdio.h>

int main(int, char**) {
  fprintf(stderr, "SKIPPED as DBCloud is not supported in ROCKSDB_LITE.\n");
  return 0;
}
#endif  // ROCKSDB_LITE
