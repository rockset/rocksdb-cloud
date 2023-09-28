#ifndef ROCKSDB_LITE
#ifdef USE_GCP
#include "google/cloud/storage/bucket_metadata.h"
#include "google/cloud/storage/client.h"

namespace gcs = ::google::cloud::storage;
namespace gcp = ::google::cloud;
#endif

#include "cloud/cloud_storage_provider_impl.h"
#include "cloud/filename.h"
#include "cloud/gcp/gcp_file_system.h"
#include "rocksdb/cloud/cloud_file_system.h"
#include "rocksdb/convenience.h"
#include <inttypes.h>

#ifdef _WIN32_WINNT
#undef GetMessage
#endif

namespace ROCKSDB_NAMESPACE {
#ifdef USE_GCP

static bool IsNotFound(gcp::Status const& status) {
  return (status.code() == gcp::StatusCode::kNotFound);
}

// AWS handle successive slashes in a path as a single slash, but GCS does not.
// So, we make it consistent by reducing multiple slashes to a single slash.
inline std::string normalzie_object_path(std::string const& object_path) {
  std::string path = ReduceSlashes(object_path);
  return ltrim_if(path, '/');
}

class CloudRequestCallbackGuard {
 public:
  CloudRequestCallbackGuard(CloudRequestCallback* callback,
                            CloudRequestOpType type, uint64_t size = 0)
      : callback_(callback), type_(type), size_(size), start_(now()) {}

  ~CloudRequestCallbackGuard() {
    if (callback_) {
      (*callback_)(type_, size_, now() - start_, success_);
    }
  }

  void SetSize(uint64_t size) { size_ = size; }
  void SetSuccess(bool success) { success_ = success; }

 private:
  uint64_t now() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::system_clock::now() -
               std::chrono::system_clock::from_time_t(0))
        .count();
  }
  CloudRequestCallback* callback_;
  CloudRequestOpType type_;
  uint64_t size_;
  bool success_{false};
  uint64_t start_;
};

/******************** GCSClientWrapper ******************/

class GCSClientWrapper {
 public:
  explicit GCSClientWrapper(CloudFileSystemOptions const& cloud_options,
                            gcp::Options gcp_options)
      : cloud_request_callback_(cloud_options.cloud_request_callback) {
    if (cloud_options.gcs_client_factory) {
      client_ = cloud_options.gcs_client_factory(gcp_options);
    } else {
      client_ = std::make_shared<gcs::Client>(gcp_options);
    }
  }

  gcp::StatusOr<gcs::BucketMetadata> CreateBucket(
      std::string bucket_name, gcs::BucketMetadata metadata) {
    CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                                CloudRequestOpType::kCreateOp);
    gcp::StatusOr<gcs::BucketMetadata> bucket_metadata =
        client_->CreateBucket(bucket_name, metadata);
    t.SetSuccess(bucket_metadata.ok());
    return bucket_metadata;
  }

  gcp::StatusOr<gcs::ListObjectsReader> ListCloudObjects(
      std::string bucket_name, std::string prefix, int /*maxium*/) {
    CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                                CloudRequestOpType::kListOp);
    gcp::StatusOr<gcs::ListObjectsReader> objects = client_->ListObjects(
        bucket_name, gcs::Prefix(prefix) /*, gcs::MaxResults(maxium)*/);
    t.SetSuccess(objects.ok());
    return objects;
  }

  gcp::StatusOr<gcs::BucketMetadata> HeadBucket(std::string bucket_name) {
    CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                                CloudRequestOpType::kInfoOp);
    gcp::StatusOr<gcs::BucketMetadata> bucket_metadata =
        client_->GetBucketMetadata(bucket_name);
    t.SetSuccess(bucket_metadata.ok());
    return bucket_metadata;
  }

  gcp::Status DeleteCloudObject(std::string bucket_name,
                                std::string object_path) {
    CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                                CloudRequestOpType::kDeleteOp);
    gcp::Status del = client_->DeleteObject(bucket_name, object_path);
    t.SetSuccess(del.ok());
    return del;
  }

  gcp::StatusOr<gcs::ObjectMetadata> CopyCloudObject(
      std::string src_bucketname, std::string src_objectpath,
      std::string dst_bucketname, std::string dst_objectpath) {
    CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                                CloudRequestOpType::kCopyOp);
    gcp::StatusOr<gcs::ObjectMetadata> object_metadata = client_->CopyObject(
        src_bucketname, src_objectpath, dst_bucketname, dst_objectpath);
    t.SetSuccess(object_metadata.ok());
    return object_metadata;
  }

  gcp::Status GetCloudObject(std::string bucket, std::string object,
                             int64_t start, size_t n, char* buf,
                             uint64_t* bytes_read) {
    CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                                CloudRequestOpType::kReadOp);
    // create a range read request
    // Ranges are inclusive, so we can't read 0 bytes; read 1 instead and
    // drop it later
    size_t rangeLen = (n != 0 ? n : 1);
    uint64_t end = start + rangeLen;
    *bytes_read = 0;

    gcs::ObjectReadStream obj =
        client_->ReadObject(bucket, object, gcs::ReadRange(start, end));
    if (obj.bad()) {
      return obj.status();
    }

    if (n != 0) {
      obj.read(buf, n);
      *bytes_read = obj.gcount();
      assert(*bytes_read <= n);
    }

    t.SetSize(*bytes_read);
    t.SetSuccess(true);

    return obj.status();
  }

  gcp::Status DownloadFile(std::string bucket_name, std::string object_path,
                           std::string dst_file, uint64_t* file_size) {
    CloudRequestCallbackGuard guard(cloud_request_callback_.get(),
                                    CloudRequestOpType::kReadOp);

    gcs::ObjectReadStream os = client_->ReadObject(bucket_name, object_path);
    if (os.bad()) {
      guard.SetSize(0);
      guard.SetSuccess(false);
      return os.status();
    }

    std::ofstream ofs(dst_file, std::ofstream::binary);
    // if ofs is not open, return error with dst_file name in message
    if (!ofs.is_open()) {
      guard.SetSize(0);
      guard.SetSuccess(false);
      std::string errmsg("Unable to open dest file ");
      errmsg.append(dst_file);
      return gcp::Status(gcp::StatusCode::kInternal, errmsg);
    }

    // Read stream for os and write to dst_file, then set the file size for
    // guard
    ofs << os.rdbuf();
    ofs.close();
    *file_size = os.size().value();
    guard.SetSize(*file_size);
    guard.SetSuccess(true);
    return gcp::Status(gcp::StatusCode::kOk, "OK");
  }

  // update object metadata
  gcp::StatusOr<gcs::ObjectMetadata> PutCloudObject(
      std::string bucket_name, std::string object_path,
      std::unordered_map<std::string, std::string> metadata,
      uint64_t size_hint = 0) {
    CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                                CloudRequestOpType::kWriteOp, size_hint);
    gcp::StatusOr<gcs::ObjectMetadata> object_meta =
        client_->InsertObject(bucket_name, object_path, "");
    if (!object_meta.ok()) {
      t.SetSuccess(false);
      return object_meta;
    }
    gcs::ObjectMetadata new_object_meta = object_meta.value();
    for (auto kv : metadata) {
      new_object_meta.mutable_metadata().emplace(kv.first, kv.second);
    }
    auto update_meta =
        client_->UpdateObject(bucket_name, object_path, new_object_meta);
    return update_meta;
  }

  gcp::StatusOr<gcs::ObjectMetadata> UploadFile(std::string bucket_name,
                                                std::string object_path,
                                                std::string loc_file) {
    CloudRequestCallbackGuard guard(cloud_request_callback_.get(),
                                    CloudRequestOpType::kWriteOp);

    gcp::StatusOr<gcs::ObjectMetadata> object_meta =
        client_->UploadFile(loc_file, bucket_name, object_path);

    if (!object_meta.ok()) {
      guard.SetSize(0);
      guard.SetSuccess(false);
      return object_meta;
    }

    guard.SetSize(object_meta.value().size());
    guard.SetSuccess(true);

    return object_meta;
  }

  gcp::StatusOr<gcs::ObjectMetadata> HeadObject(std::string bucket_name,
                                                std::string object_path) {
    CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                                CloudRequestOpType::kInfoOp);
    gcp::StatusOr<gcs::ObjectMetadata> object_metadata =
        client_->GetObjectMetadata(bucket_name, object_path);
    t.SetSuccess(object_metadata.ok());
    return object_metadata;
  }

  CloudRequestCallback* GetRequestCallback() {
    return cloud_request_callback_.get();
  }

 private:
  std::shared_ptr<google::cloud::storage::Client> client_;
  std::shared_ptr<CloudRequestCallback> cloud_request_callback_;
};

/******************** GcsReadableFile ******************/
class GcsReadableFile : public CloudStorageReadableFileImpl {
 public:
  GcsReadableFile(std::shared_ptr<GCSClientWrapper> const& gcs_client,
                  Logger* info_log, std::string const& bucket,
                  std::string const& fname, uint64_t size,
                  std::string content_hash)
      : CloudStorageReadableFileImpl(info_log, bucket, fname, size),
        gcs_client_(gcs_client),
        content_hash_(std::move(content_hash)) {}

  virtual char const* Type() const { return "gcs"; }

  size_t GetUniqueId(char* id, size_t max_size) const override {
    if (content_hash_.empty()) {
      return 0;
    }

    max_size = std::min(content_hash_.size(), max_size);
    memcpy(id, content_hash_.c_str(), max_size);
    return max_size;
  }

  // random access, read data from specified offset in file
  IOStatus DoCloudRead(uint64_t offset, size_t n, IOOptions const& /*options*/,
                       char* scratch, uint64_t* bytes_read,
                       IODebugContext* /*dbg*/) const override {
    // read the range
    auto status = gcs_client_->GetCloudObject(bucket_, fname_, offset, n,
                                              scratch, bytes_read);
    if (!status.ok()) {
      if (IsNotFound(status)) {
        Log(InfoLogLevel::ERROR_LEVEL, info_log_,
            "[gcs] GcsReadableFile ReadObject Not Found %s \n", fname_.c_str());
        return IOStatus::NotFound();
      } else {
        Log(InfoLogLevel::ERROR_LEVEL, info_log_,
            "[gcs] GcsReadableFile ReadObject error %s offset %" PRIu64
            " rangelen %" ROCKSDB_PRIszt ", message: %s\n",
            fname_.c_str(), offset, n, status.message().c_str());
        return IOStatus::IOError(fname_.c_str(), status.message().c_str());
      }
    }

    return IOStatus::OK();
  }

 private:
  std::shared_ptr<GCSClientWrapper> const& gcs_client_;
  std::string content_hash_;
};  // End class GcsReadableFile

/******************** Writablefile ******************/

class GcsWritableFile : public CloudStorageWritableFileImpl {
 public:
  GcsWritableFile(CloudFileSystem* fs, std::string const& local_fname,
                  std::string const& bucket, std::string const& cloud_fname,
                  FileOptions const& options)
      : CloudStorageWritableFileImpl(fs, local_fname, bucket, cloud_fname,
                                     options) {}
  virtual char const* Name() const override {
    return CloudStorageProviderImpl::kGcs();
  }
};  // End class GcsWritableFile

/******************** GcsStorageProvider ******************/
class GcsStorageProvider : public CloudStorageProviderImpl {
 public:
  ~GcsStorageProvider() override {}
  virtual char const* Name() const override { return kGcs(); }
  IOStatus CreateBucket(std::string const& bucket) override;
  IOStatus ExistsBucket(std::string const& bucket) override;
  IOStatus EmptyBucket(std::string const& bucket_name,
                       std::string const& object_path) override;
  IOStatus DeleteCloudObject(std::string const& bucket_name,
                             std::string const& object_path) override;
  IOStatus ListCloudObjects(std::string const& bucket_name,
                            std::string const& object_path,
                            std::vector<std::string>* result) override;
  IOStatus ExistsCloudObject(std::string const& bucket_name,
                             std::string const& object_path) override;
  IOStatus GetCloudObjectSize(std::string const& bucket_name,
                              std::string const& object_path,
                              uint64_t* filesize) override;
  IOStatus GetCloudObjectModificationTime(std::string const& bucket_name,
                                          std::string const& object_path,
                                          uint64_t* time) override;
  IOStatus GetCloudObjectMetadata(std::string const& bucket_name,
                                  std::string const& object_path,
                                  CloudObjectInformation* info) override;
  IOStatus PutCloudObjectMetadata(
      std::string const& bucket_name, std::string const& object_path,
      std::unordered_map<std::string, std::string> const& metadata) override;
  IOStatus CopyCloudObject(std::string const& bucket_name_src,
                           std::string const& object_path_src,
                           std::string const& bucket_name_dest,
                           std::string const& object_path_dest) override;
  IOStatus DoNewCloudReadableFile(
      std::string const& bucket, std::string const& fname, uint64_t fsize,
      std::string const& content_hash, FileOptions const& options,
      std::unique_ptr<CloudStorageReadableFile>* result,
      IODebugContext* dbg) override;
  IOStatus NewCloudWritableFile(
      std::string const& local_path, std::string const& bucket_name,
      std::string const& object_path, FileOptions const& options,
      std::unique_ptr<CloudStorageWritableFile>* result,
      IODebugContext* dbg) override;
  Status PrepareOptions(ConfigOptions const& options) override;

 protected:
  IOStatus DoGetCloudObject(std::string const& bucket_name,
                            std::string const& object_path,
                            std::string const& destination,
                            uint64_t* remote_size) override;
  IOStatus DoPutCloudObject(std::string const& local_file,
                            std::string const& bucket_name,
                            std::string const& object_path,
                            uint64_t file_size) override;

 private:
  struct HeadObjectResult {
    // If any of the field is non-nullptr, returns requested data
    std::unordered_map<std::string, std::string>* metadata = nullptr;
    uint64_t* size = nullptr;
    uint64_t* modtime = nullptr;
    std::string* etag = nullptr;
  };

  // Retrieves metadata from an object
  IOStatus HeadObject(std::string const& bucket, std::string const& path,
                      HeadObjectResult* result);

  // The Gcs client
  std::shared_ptr<GCSClientWrapper> gcs_client_;
};  // End class GcsStorageProvider

/******************** GcsFileSystem ******************/
IOStatus GcsStorageProvider::CreateBucket(std::string const& bucket) {
  std::string bucket_location =
      cfs_->GetCloudFileSystemOptions().dest_bucket.GetRegion();
  // storage_class: https://cloud.google.com/storage/docs/storage-classes
  // default storage_class = STANDARD
  std::string sc("STANDARD");
  auto bucket_metadata = gcs_client_->CreateBucket(
      bucket, gcs::BucketMetadata().set_storage_class(sc).set_location(
                  bucket_location));
  if (!bucket_metadata.ok()) {
    // Bucket already exists is not an error
    if (gcp::StatusCode::kAlreadyExists != bucket_metadata.status().code()) {
      std::string errmsg(bucket_metadata.status().message());
      return IOStatus::IOError(bucket.c_str(), errmsg.c_str());
    }
  }
  return IOStatus::OK();
}

IOStatus GcsStorageProvider::ExistsBucket(std::string const& bucket) {
  gcp::StatusOr<gcs::BucketMetadata> bucketmetadata =
      gcs_client_->HeadBucket(bucket);
  if (IsNotFound(bucketmetadata.status())) {
    return IOStatus::NotFound();
  }
  return IOStatus::OK();
}

IOStatus GcsStorageProvider::EmptyBucket(std::string const& bucket_name,
                                         std::string const& object_path) {
  std::vector<std::string> results;
  auto st = ListCloudObjects(bucket_name, object_path, &results);
  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, cfs_->GetLogger(),
        "[Gcs] EmptyBucket unable to find objects in bucket %s %s",
        bucket_name.c_str(), st.ToString().c_str());
    return st;
  }
  Log(InfoLogLevel::DEBUG_LEVEL, cfs_->GetLogger(),
      "[Gcs] EmptyBucket going to delete %" ROCKSDB_PRIszt
      " objects in bucket %s",
      results.size(), bucket_name.c_str());

  // Delete all objects from bucket
  for (auto const& path : results) {
    st = DeleteCloudObject(bucket_name, object_path + "/" + path);
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, cfs_->GetLogger(),
          "[Gcs] EmptyBucket Unable to delete %s in bucket %s %s", path.c_str(),
          bucket_name.c_str(), st.ToString().c_str());
      return st;
    }
  }
  return IOStatus::OK();
}

IOStatus GcsStorageProvider::DeleteCloudObject(std::string const& bucket_name,
                                               std::string const& object_path) {
  auto normalized_path = normalzie_object_path(object_path);
  auto st = gcs_client_->DeleteCloudObject(bucket_name, normalized_path);
  if (!st.ok()) {
    if (IsNotFound(st)) {
      return IOStatus::NotFound(object_path, st.message().c_str());
    } else {
      return IOStatus::IOError(object_path, st.message().c_str());
    }
  }
  Log(InfoLogLevel::INFO_LEVEL, cfs_->GetLogger(),
      "[Gcs] DeleteFromGcs %s/%s, status %s", bucket_name.c_str(),
      object_path.c_str(), st.message().c_str());
  return IOStatus::OK();
}

IOStatus GcsStorageProvider::ListCloudObjects(
    std::string const& bucket_name, std::string const& object_path,
    std::vector<std::string>* result) {
  // follow with aws_s3
  auto prefix = normalzie_object_path(object_path);
  prefix = ensure_ends_with_pathsep(prefix);
  // MaxResults is about page limits
  // https://stackoverflow.com/questions/77069696/how-to-limit-number-of-objects-returned-from-listobjects
  auto objects = gcs_client_->ListCloudObjects(
      bucket_name, prefix,
      cfs_->GetCloudFileSystemOptions().number_objects_listed_in_one_iteration);
  if (!objects.ok()) {
    std::string errmsg(objects.status().message());
    if (IsNotFound(objects.status())) {
      Log(InfoLogLevel::ERROR_LEVEL, cfs_->GetLogger(),
          "[Gcs] GetChildren dir %s does not exist: %s", object_path.c_str(),
          errmsg.c_str());
      return IOStatus::NotFound(object_path, errmsg.c_str());
    }
    return IOStatus::IOError(object_path, errmsg.c_str());
  }
  for (auto const& obj : objects.value()) {
    // Our path should be a prefix of the fetched value
    std::string name = obj.value().name();
    if (name.find(prefix) != 0) {  // npos or other value
      return IOStatus::IOError("Unexpected result from Gcs: " + name);
    }
    auto fname = name.substr(prefix.size());
    result->push_back(std::move(fname));
  }
  return IOStatus::OK();
}

IOStatus GcsStorageProvider::ExistsCloudObject(std::string const& bucket_name,
                                               std::string const& object_path) {
  HeadObjectResult result;
  return HeadObject(bucket_name, object_path, &result);
}

IOStatus GcsStorageProvider::GetCloudObjectSize(std::string const& bucket_name,
                                                std::string const& object_path,
                                                uint64_t* filesize) {
  HeadObjectResult result;
  result.size = filesize;
  return HeadObject(bucket_name, object_path, &result);
}

IOStatus GcsStorageProvider::GetCloudObjectModificationTime(
    std::string const& bucket_name, std::string const& object_path,
    uint64_t* time) {
  HeadObjectResult result;
  result.modtime = time;
  return HeadObject(bucket_name, object_path, &result);
}

IOStatus GcsStorageProvider::GetCloudObjectMetadata(
    std::string const& bucket_name, std::string const& object_path,
    CloudObjectInformation* info) {
  assert(info != nullptr);
  HeadObjectResult result;
  result.metadata = &info->metadata;
  result.size = &info->size;
  result.modtime = &info->modification_time;
  result.etag = &info->content_hash;
  return HeadObject(bucket_name, object_path, &result);
}

IOStatus GcsStorageProvider::PutCloudObjectMetadata(
    std::string const& bucket_name, std::string const& object_path,
    std::unordered_map<std::string, std::string> const& metadata) {
  auto normalized_path = normalzie_object_path(object_path);
  auto outcome =
      gcs_client_->PutCloudObject(bucket_name, normalized_path, metadata);
  if (!outcome.ok()) {
    auto const& error = outcome.status().message();
    std::string errmsg(error.c_str(), error.size());
    Log(InfoLogLevel::ERROR_LEVEL, cfs_->GetLogger(),
        "[Gcs] Bucket %s error in saving metadata %s", bucket_name.c_str(),
        errmsg.c_str());
    return IOStatus::IOError(object_path, errmsg.c_str());
  }
  return IOStatus::OK();
}

IOStatus GcsStorageProvider::CopyCloudObject(
    std::string const& bucket_name_src, std::string const& object_path_src,
    std::string const& bucket_name_dest, std::string const& object_path_dest) {
  std::string src_url = bucket_name_src + object_path_src;
  auto normalized_src_path = normalzie_object_path(object_path_src);
  auto normalized_dest_path = normalzie_object_path(object_path_dest);
  auto copy =
      gcs_client_->CopyCloudObject(bucket_name_src, normalized_src_path,
                                   bucket_name_dest, normalized_dest_path);
  if (!copy.ok()) {
    auto const& error = copy.status().message();
    std::string errmsg(error.c_str(), error.size());
    Log(InfoLogLevel::ERROR_LEVEL, cfs_->GetLogger(),
        "[Gcs] GcsWritableFile src path %s error in copying to %s %s",
        src_url.c_str(), object_path_dest.c_str(), errmsg.c_str());
    return IOStatus::IOError(object_path_dest.c_str(), errmsg.c_str());
  }
  Log(InfoLogLevel::INFO_LEVEL, cfs_->GetLogger(),
      "[Gcs] GcsWritableFile src path %s copied to %s OK", src_url.c_str(),
      object_path_dest.c_str());
  return IOStatus::OK();
}

IOStatus GcsStorageProvider::DoNewCloudReadableFile(
    std::string const& bucket, std::string const& fname, uint64_t fsize,
    std::string const& content_hash, FileOptions const& /*options*/,
    std::unique_ptr<CloudStorageReadableFile>* result,
    IODebugContext* /*dbg*/) {
  auto normalized_path = normalzie_object_path(fname);
  result->reset(new GcsReadableFile(gcs_client_, cfs_->GetLogger(), bucket,
                                    normalized_path, fsize, content_hash));
  return IOStatus::OK();
}

IOStatus GcsStorageProvider::NewCloudWritableFile(
    std::string const& local_path, std::string const& bucket_name,
    std::string const& object_path, FileOptions const& file_opts,
    std::unique_ptr<CloudStorageWritableFile>* result,
    IODebugContext* /*dbg*/) {
  auto normalized_path = normalzie_object_path(object_path);
  result->reset(new GcsWritableFile(cfs_, local_path, bucket_name,
                                    normalized_path, file_opts));
  return (*result)->status();
}

Status GcsStorageProvider::PrepareOptions(ConfigOptions const& options) {
  auto cfs = dynamic_cast<CloudFileSystem*>(options.env->GetFileSystem().get());
  assert(cfs);
  auto const& cloud_opts = cfs->GetCloudFileSystemOptions();
  if (std::string(cfs->Name()) != CloudFileSystemImpl::kGcp()) {
    return Status::InvalidArgument("gcs Provider requires gcp Environment");
  }
  // TODO: support buckets being in different regions
  if (!cfs->SrcMatchesDest() && cfs->HasSrcBucket() && cfs->HasDestBucket()) {
    if (cloud_opts.src_bucket.GetRegion() !=
        cloud_opts.dest_bucket.GetRegion()) {
      Log(InfoLogLevel::ERROR_LEVEL, cfs->GetLogger(),
          "[gcp] NewGcpFileSystem Buckets %s, %s in two different regions %s, "
          "%s is not supported",
          cloud_opts.src_bucket.GetBucketName().c_str(),
          cloud_opts.dest_bucket.GetBucketName().c_str(),
          cloud_opts.src_bucket.GetRegion().c_str(),
          cloud_opts.dest_bucket.GetRegion().c_str());
      return Status::InvalidArgument("Two different regions not supported");
    }
  }
  // initialize the Gcs client
  gcp::Options gcp_options;
  Status status = GcpCloudOptions::GetClientConfiguration(
      cfs, cloud_opts.src_bucket.GetRegion(), gcp_options);
  if (status.ok()) {
    gcs_client_ = std::make_shared<GCSClientWrapper>(cloud_opts, gcp_options);
    return CloudStorageProviderImpl::PrepareOptions(options);
  }
  return status;
}

IOStatus GcsStorageProvider::DoGetCloudObject(std::string const& bucket_name,
                                              std::string const& object_path,
                                              std::string const& destination,
                                              uint64_t* remote_size) {
  auto normalized_path = normalzie_object_path(object_path);
  auto get = gcs_client_->DownloadFile(bucket_name, normalized_path,
                                       destination, remote_size);
  if (!get.ok()) {
    std::string errmsg;
    errmsg = get.message();
    if (IsNotFound(get)) {
      Log(InfoLogLevel::ERROR_LEVEL, cfs_->GetLogger(),
          "[gcs] GetObject %s/%s error %s.", bucket_name.c_str(),
          object_path.c_str(), errmsg.c_str());
      return IOStatus::NotFound(std::move(errmsg));
    } else {
      Log(InfoLogLevel::INFO_LEVEL, cfs_->GetLogger(),
          "[gcs] GetObject %s/%s error %s.", bucket_name.c_str(),
          object_path.c_str(), errmsg.c_str());
      return IOStatus::IOError(std::move(errmsg));
    }
  }
  return IOStatus::OK();
}

// Uploads local_file to GCS bucket_name/object_path
IOStatus GcsStorageProvider::DoPutCloudObject(std::string const& local_file,
                                              std::string const& bucket_name,
                                              std::string const& object_path,
                                              uint64_t file_size) {
  auto normalized_path = normalzie_object_path(object_path);
  auto put = gcs_client_->UploadFile(bucket_name, normalized_path, local_file);
  if (!put.ok()) {
    auto const& error = put.status().message();
    std::string errmsg(error.c_str(), error.size());
    Log(InfoLogLevel::ERROR_LEVEL, cfs_->GetLogger(),
        "[s3] PutCloudObject %s/%s, size %" PRIu64 ", ERROR %s",
        bucket_name.c_str(), object_path.c_str(), file_size, errmsg.c_str());
    return IOStatus::IOError(local_file, errmsg);
  }

  Log(InfoLogLevel::INFO_LEVEL, cfs_->GetLogger(),
      "[gcs] PutCloudObject %s/%s, size %" PRIu64 ", OK", bucket_name.c_str(),
      object_path.c_str(), file_size);
  return IOStatus::OK();
}

IOStatus GcsStorageProvider::HeadObject(std::string const& bucket,
                                        std::string const& path,
                                        HeadObjectResult* result) {
  assert(result != nullptr);
  auto object_path = normalzie_object_path(path);
  auto head = gcs_client_->HeadObject(bucket, object_path);
  if (!head.ok()) {
    auto const& errMessage = head.status().message();
    Slice object_path_slice(object_path.data(), object_path.size());
    if (IsNotFound(head.status())) {
      return IOStatus::NotFound(object_path_slice, errMessage.c_str());
    } else {
      return IOStatus::IOError(object_path_slice, errMessage.c_str());
    }
  }

  auto const& head_val = head.value();
  if (result->metadata != nullptr) {
    // std::map<std::string, std::string> metadata_
    for (auto const& m : head_val.metadata()) {
      (*(result->metadata))[m.first.c_str()] = m.second.c_str();
    }
  }
  if (result->size != nullptr) {
    *(result->size) = head_val.size();
  }
  if ((result->modtime) != nullptr) {
    int64_t modtime = std::chrono::duration_cast<std::chrono::milliseconds>(
                          head_val.updated().time_since_epoch())
                          .count();
    *(result->modtime) = modtime;
  }
  if ((result->etag) != nullptr) {
    *(result->etag) =
        std::string(head_val.etag().data(), head_val.etag().length());
  }
  return IOStatus::OK();
}

#endif  // USE_GCP

Status CloudStorageProviderImpl::CreateGcsProvider(
    std::unique_ptr<CloudStorageProvider>* provider) {
#ifndef USE_GCP
  provider->reset();
  return Status::NotSupported(
      "In order to use Google Cloud Storage, make sure you're compiling with "
      "USE_GCS=1");
#else
  provider->reset(new GcsStorageProvider());
  return Status::OK();
#endif
}
}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE