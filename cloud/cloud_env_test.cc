// Copyright (c) 2017 Rockset

#include "cloud/cloud_log_controller_impl.h"
#include "cloud/cloud_storage_provider_impl.h"
#include "file/file_util.h"
#include "port/stack_trace.h"
#include "rocksdb/cloud/cloud_env_options.h"
#include "rocksdb/cloud/cloud_log_controller.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/convenience.h"
#include "rocksdb/env.h"
#include "test_util/testharness.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
class CloudEnvTest : public testing::Test {
 public:
  std::unique_ptr<CloudEnv> cenv_;

  virtual ~CloudEnvTest() {}
};

TEST_F(CloudEnvTest, TestBucket) {
  CloudEnvOptions copts;
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

TEST_F(CloudEnvTest, ConfigureOptions) {
  ConfigOptions config_options;
  CloudEnvOptions copts, copy;
  copts.keep_local_sst_files = false;
  copts.keep_local_log_files = false;
  copts.create_bucket_if_missing = false;
  copts.validate_filesize = false;
  copts.skip_dbid_verification = false;
  copts.ephemeral_resync_on_open = false;
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
  ASSERT_FALSE(copy.ephemeral_resync_on_open);
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
  copts.ephemeral_resync_on_open = true;
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
  ASSERT_TRUE(copy.ephemeral_resync_on_open);
  ASSERT_TRUE(copy.skip_cloud_files_in_getchildren);
  ASSERT_TRUE(copy.run_purger);
  ASSERT_EQ(copy.constant_sst_file_size_in_sst_file_manager, 200);
  ASSERT_EQ(copy.purger_periodicity_millis, 201);
}

TEST_F(CloudEnvTest, ConfigureBucketOptions) {
  ConfigOptions config_options;
  CloudEnvOptions copts, copy;
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

TEST_F(CloudEnvTest, ConfigureEnv) {
  ConfigOptions config_options;
  config_options.invoke_prepare_options = false;
  ASSERT_OK(CloudEnv::CreateFromString(config_options,
                                       "keep_local_sst_files=true", &cenv_));
  ASSERT_NE(cenv_, nullptr);
  ASSERT_STREQ(cenv_->Name(), "cloud");
  auto copts = cenv_->GetOptions<CloudEnvOptions>();
  ASSERT_NE(copts, nullptr);
  ASSERT_TRUE(copts->keep_local_sst_files);
}

TEST_F(CloudEnvTest, TestInitialize) {
  BucketOptions bucket;
  ConfigOptions config_options;
  config_options.invoke_prepare_options = false;
  ASSERT_OK(CloudEnv::CreateFromString(
      config_options, "id=cloud; TEST=cloudenvtest:/test/path", &cenv_));
  ASSERT_NE(cenv_, nullptr);
  ASSERT_STREQ(cenv_->Name(), "cloud");

  ASSERT_TRUE(StartsWith(cenv_->GetSrcBucketName(),
                         bucket.GetBucketPrefix() + "cloudenvtest."));
  ASSERT_EQ(cenv_->GetSrcObjectPath(), "/test/path");
  ASSERT_TRUE(cenv_->SrcMatchesDest());

  ASSERT_OK(CloudEnv::CreateFromString(
      config_options, "id=cloud; TEST=cloudenvtest2:/test/path2?here", &cenv_));
  ASSERT_NE(cenv_, nullptr);
  ASSERT_STREQ(cenv_->Name(), "cloud");
  ASSERT_TRUE(StartsWith(cenv_->GetSrcBucketName(),
                         bucket.GetBucketPrefix() + "cloudenvtest2."));
  ASSERT_EQ(cenv_->GetSrcObjectPath(), "/test/path2");
  ASSERT_EQ(cenv_->GetCloudEnvOptions().src_bucket.GetRegion(), "here");
  ASSERT_TRUE(cenv_->SrcMatchesDest());

  ASSERT_OK(
      CloudEnv::CreateFromString(config_options,
                                 "id=cloud; TEST=cloudenvtest3:/test/path3; "
                                 "src.bucket=my_bucket; dest.object=/my_path",
                                 &cenv_));
  ASSERT_NE(cenv_, nullptr);
  ASSERT_STREQ(cenv_->Name(), "cloud");
  ASSERT_EQ(cenv_->GetSrcBucketName(), bucket.GetBucketPrefix() + "my_bucket");
  ASSERT_EQ(cenv_->GetSrcObjectPath(), "/test/path3");
  ASSERT_TRUE(StartsWith(cenv_->GetDestBucketName(),
                         bucket.GetBucketPrefix() + "cloudenvtest3."));
  ASSERT_EQ(cenv_->GetDestObjectPath(), "/my_path");
}

TEST_F(CloudEnvTest, ConfigureAwsEnv) {
  ConfigOptions config_options;
  Status s = CloudEnv::CreateFromString(
      config_options, "id=aws; keep_local_sst_files=true", &cenv_);
#ifdef USE_AWS
  ASSERT_OK(s);
  ASSERT_NE(cenv_, nullptr);
  ASSERT_STREQ(cenv_->Name(), "aws");
  auto copts = cenv_->GetOptions<CloudEnvOptions>();
  ASSERT_NE(copts, nullptr);
  ASSERT_TRUE(copts->keep_local_sst_files);
  ASSERT_NE(cenv_->GetStorageProvider(), nullptr);
  ASSERT_STREQ(cenv_->GetStorageProvider()->Name(),
               CloudStorageProviderImpl::kS3());
#else
  ASSERT_NOK(s);
  ASSERT_EQ(cenv_, nullptr);
#endif
}

TEST_F(CloudEnvTest, ConfigureS3Provider) {
  ConfigOptions config_options;
  Status s = CloudEnv::CreateFromString(config_options, "provider=s3", &cenv_);
  ASSERT_NOK(s);
  ASSERT_EQ(cenv_, nullptr);

#ifdef USE_AWS
  ASSERT_OK(
      CloudEnv::CreateFromString(config_options, "id=aws; provider=s3", &cenv_));
  ASSERT_STREQ(cenv_->Name(), "aws");
  ASSERT_NE(cenv_->GetStorageProvider(), nullptr);
  ASSERT_STREQ(cenv_->GetStorageProvider()->Name(),
               CloudStorageProviderImpl::kS3());
#endif
}

// Test is disabled until we have a mock provider and authentication issues are
// resolved
TEST_F(CloudEnvTest, DISABLED_ConfigureKinesisController) {
  ConfigOptions config_options;
  Status s = CloudEnv::CreateFromString(
      config_options, "provider=mock; controller=kinesis", &cenv_);
  ASSERT_NOK(s);
  ASSERT_EQ(cenv_, nullptr);

#ifdef USE_AWS
  ASSERT_OK(CloudEnv::CreateFromString(
      config_options, "id=aws; controller=kinesis; TEST=dbcloud:/test", &cenv_));
  ASSERT_STREQ(cenv_->Name(), "aws");
  ASSERT_NE(cenv_->GetLogController(), nullptr);
  ASSERT_STREQ(cenv_->GetLogController()->Name(),
               CloudLogControllerImpl::kKinesis());
#endif
}

TEST_F(CloudEnvTest, ConfigureKafkaController) {
  ConfigOptions config_options;
  Status s = CloudEnv::CreateFromString(
      config_options, "provider=mock; controller=kafka", &cenv_);
#ifdef USE_KAFKA
  ASSERT_OK(s);
  ASSERT_NE(cenv_, nullptr);
  ASSERT_NE(cenv_->GetLogController(), nullptr);
  ASSERT_STREQ(cenv_->GetLogController()->Name(),
               CloudLogControllerImpl::kKafka());
#else
  ASSERT_NOK(s);
  ASSERT_EQ(cenv_, nullptr);
#endif
}

class CloudProviderTest
    : public CloudEnvTest,
      public ::testing::WithParamInterface<std::string> {
 public:
  CloudProviderTest() {
    Random64 rng(time(nullptr));
    std::string test_id = "cloud-provider-" + std::to_string(rng.Next());
    test_path_ = test::TmpDir() + "/" + test_id;
    std::string test_bucket = " TEST=providertest:" + test_path_;
    cloud_opts_str_ = GetParam() + test_bucket;
    
    fprintf(stderr, "Test[%s] ID: %s\n", GetParam().c_str(),
            test_id.c_str());

    EXPECT_OK(CloudEnv::CreateFromString(config_options_, cloud_opts_str_, &cenv_));
    EXPECT_OK(cenv_->GetBaseEnv()->CreateDirIfMissing(test_path_));
    provider_ = cenv_->GetStorageProvider().get();
    bucket_name_ = cenv_->GetDestBucketName();
    object_path_ = cenv_->GetDestObjectPath();
    EXPECT_OK(provider_->ExistsBucket(bucket_name_));
  }

  ~CloudProviderTest() {
    if (cenv_ != nullptr && provider_ != nullptr) {
      EXPECT_OK(provider_->EmptyBucket(bucket_name_, object_path_));
      EXPECT_OK(DestroyDir(cenv_->GetBaseEnv(), test_path_));
    }
  }
  
  CloudStorageProvider *provider_;
  std::string bucket_name_;
  std::string object_path_;
protected:
  ConfigOptions config_options_;
  std::string cloud_opts_str_;
  std::string test_path_;
};
  
TEST_P(CloudProviderTest, BasicTest) {
  std::vector<std::string> objects;
  std::string object = object_path_ + "/test.sst";
  printf("MJR: Object[%s]\n", object.c_str());
  ASSERT_OK(provider_->ListCloudObjects(bucket_name_, object_path_, &objects));
  printf("MJR: ListObject[%d]\n",(int)  objects.size());
  ASSERT_EQ(objects.size(), 0);
  ASSERT_EQ(provider_->ExistsCloudObject(bucket_name_, object), Status::NotFound());
  printf("MJR: ExistsObject[%s]\n",object.c_str());

  std::unique_ptr<WritableFile> wfile;
  std::string fname = test_path_ + "/test.sst";
  std::string msg = "Hello World";
  ASSERT_OK(WriteStringToFile(cenv_->GetBaseEnv(), Slice(msg), fname));
  ASSERT_OK(provider_->PutCloudObject(fname, bucket_name_, object));
  ASSERT_OK(provider_->ExistsCloudObject(bucket_name_, object));
  ASSERT_OK(provider_->ListCloudObjects(bucket_name_, object_path_, &objects));
  ASSERT_EQ(objects.size(), 1);
  printf("MJR: Passed all tests [%s]\n", objects[0].c_str());
}
  
#ifdef USE_AWS
INSTANTIATE_TEST_CASE_P(AWS, CloudProviderTest, ::testing::Values("id=aws;"));
#endif  // USE_AWS
#ifdef USE_AZURE
INSTANTIATE_TEST_CASE_P(Azure, CloudProviderTest, ::testing::Values("provider=azure;"));
#endif  // USE_AZURE
  
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
