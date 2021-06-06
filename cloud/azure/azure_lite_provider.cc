//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
// This file defines an AZURE environment for rocksdb.
// A directory maps to an an zero-size object in a bucket
// A sst file maps to an object in that bucket.
//
#ifdef USE_AZURE
#include "blob/blob_client.h"
#include "storage_account.h"
#include "storage_credential.h"
#endif  // USE_AZURE

#include <cassert>
#include <cinttypes>
#include <fstream>
#include <iostream>

#include "cloud/cloud_env_impl.h"
#include "cloud/cloud_storage_provider_impl.h"
#include "cloud/filename.h"
#include "port/port.h"
#include "rocksdb/cloud/cloud_env_options.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/convenience.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/object_registry.h"
#include "util/string_util.h"

#ifdef _WIN32_WINNT
#undef GetMessage
#endif

namespace ROCKSDB_NAMESPACE {

/******************** AzureBlobClient ******************/

class AzureClient {
 public:
  static const char* kAzure() { return "azure"; }
  static Status Create(const CloudEnvOptions& cloud_opts,
                       std::shared_ptr<AzureClient>* result);
  virtual ~AzureClient() { }
  virtual Status CreateContainer(const std::string& container) = 0;
  virtual Status ExistsContainer(const std::string& container) = 0;
  virtual Status DeleteBlob(const std::string& container,
                            const std::string& blob) = 0;
  virtual Status ExistsBlob(const std::string& container,
                            const std::string& blob) = 0;
  virtual Status ListBlobs(const std::string& container,
                           const std::string& prefix,
                           int max_objects,
                           std::string* marker,
                           std::vector<std::string>* blobs) = 0;
  virtual Status GetBlobProperties(
      const std::string& container, const std::string& blob,
      std::unordered_map<std::string, std::string>* metadata, uint64_t* size,
      uint64_t* modtime, std::string* tag) = 0;
  virtual Status SetBlobMetadata(
      const std::string& container, const std::string& blob,
      const std::unordered_map<std::string, std::string>& metadata) = 0;
  virtual Status CopyBlob(const std::string& src_container,
                          const std::string& src_blob,
                          const std::string& dst_container,
                          const std::string& dst_blob) = 0;
  virtual Status DownloadBlob(const std::string& container,
                              const std::string& blob,
                              const std::string& local_file, uint64_t size) = 0;
  virtual Status UploadBlob(const std::string& local_file,
                            const std::string& container,
                            const std::string& blob, uint64_t size) = 0;
  virtual Status ReadBlobChunk(const std::string& container,
                               const std::string& blob, uint64_t offset,
                               uint64_t size, char *data, uint64_t *read) = 0;
};

#ifdef USE_AZURE
class AzureBlobClient : public AzureClient {
 protected:
  bool IsNotFound(const azure::storage_lite::storage_error& error) const {
    return error.code == "404";
  }
  
  template <typename T>
  Status ToStatus(const azure::storage_lite::storage_outcome<T>& outcome,
                  const std::string& method, const std::string& container,
                  const std::string& blob = "") {
    if (outcome.success()) {
      return Status::OK();
    } else {
      //**TODO: What error information should we gather?
      std::string message =
          method + ": ";  // + std::to_string(outcome.error().code);
      if (IsNotFound(outcome.error())) {
        return Status::NotFound(container, message);
      } else if (blob.empty()) {
        return Status::IOError(container, message);
      } else {
        return Status::IOError(container + "://" + blob, message);
      }
    }
  }

 public:
  AzureBlobClient(
      const std::shared_ptr<azure::storage_lite::blob_client>& client)
      : blob_client_(client) {}

  Status CreateContainer(const std::string& container) override {
    auto ret = blob_client_->create_container(container).get();
    return ToStatus(ret, "CreateContainer", container);
  }

  Status ExistsContainer(const std::string& container) override {
    auto ret = blob_client_->get_container_properties(container).get();
    return (ret.success()) ? Status::OK() : Status::NotFound();
  }

  Status DeleteBlob(const std::string& container,
                    const std::string& blob) override {
    auto ret = blob_client_->delete_blob(container, blob).get();
    return ToStatus(ret, "DeleteBlob", container, blob);
  }

  Status ExistsBlob(const std::string& container,
                    const std::string& blob) override {
    auto ret = blob_client_->get_blob_properties(container, blob).get();
    return (ret.success()) ? Status::OK() : Status::NotFound();
  }

  Status ListBlobs(const std::string& container,
                   const std::string& prefix,
                   int max_objects,
                   std::string* marker,
                   std::vector<std::string>* result) override {
    auto ret = blob_client_->list_blobs_segmented(container, "", *marker, prefix, max_objects).get();
    if (ret.success()) {
      std::string path = prefix;
      path = ensure_ends_with_pathsep(std::move(path));
      
      const auto& blobs = ret.response();
      for (const auto& blob : blobs.blobs) {
        if (blob.name.find(path) == 0) {
          result->push_back(blob.name.substr(path.size()));
        }
      }
        *marker = blobs.next_marker;
      return Status::OK();
    } else {
      return ToStatus(ret, "ListBlobs", container, prefix);
    }
  }
  
  Status GetBlobProperties(
      const std::string& container, const std::string& blob,
      std::unordered_map<std::string, std::string>* metadata, uint64_t* size,
      uint64_t* modtime, std::string* tag) override {
    auto ret = blob_client_->get_blob_properties(container, blob).get();
    if (ret.success()) {
      const auto& props = ret.response();
      if (metadata != nullptr) {
        metadata->clear();
        for (auto prop : props.metadata) {
          metadata->insert(prop);
        }
      }
      if (size != nullptr) {
        *size = static_cast<uint64_t>(props.size);
      }
      if (modtime != nullptr) {
        *modtime = static_cast<uint64_t>(props.last_modified);
      }
      if (tag != nullptr) {
        *tag = props.etag;
      }
      return Status::OK();
    } else {
      return ToStatus(ret, "GetBlobProperties", container, blob);
    }
  }

  Status SetBlobMetadata(
      const std::string& container, const std::string& blob,
      const std::unordered_map<std::string, std::string>& metadata) override {
    std::vector<std::pair<std::string, std::string>> blob_metadata;
    for (const auto& m : metadata) blob_metadata.push_back(m);
    auto ret =
        blob_client_->set_blob_metadata(container, blob, blob_metadata).get();
    if (ret.success()) {
      return Status::OK();
    } else if (IsNotFound(ret.error())) {
      auto ret2 = blob_client_->upload_block_blob_from_buffer(container, blob, "", blob_metadata, 0).get();
      return ToStatus(ret2, "SetBlobMetadata", container, blob);
    } else {
      return ToStatus(ret, "SetBlobMetaData", container, blob);
    }
  }

  Status CopyBlob(const std::string& src_container, const std::string& src_blob,
                  const std::string& dst_container,
                  const std::string& dst_blob) override {
    auto ret =
        blob_client_
            ->start_copy(src_container, src_blob, dst_container, dst_blob)
            .get();
    return ToStatus(ret, "CopyBlob", src_container, src_blob);
  }

  Status DownloadBlob(const std::string& container, const std::string& blob,
                      const std::string& local_file, uint64_t size) override {
    std::ofstream ofs;
    ofs.open(local_file,
             std::ofstream::out | std::ofstream::binary | std::ofstream::trunc);
    auto ret =
        blob_client_->download_blob_to_stream(container, blob, 0, size, ofs).get();
    return ToStatus(ret, "DownloadBlob", container, blob);
  }

  Status UploadBlob(const std::string& local_file, const std::string& container,
                    const std::string& blob, uint64_t size) override {
    std::vector<std::pair<std::string, std::string>> metadata;
    std::ifstream ifs;
    ifs.open(local_file);
    auto ret = blob_client_
                   ->upload_block_blob_from_stream(container, blob, ifs,
                                                   metadata, size)
                   .get();
    return ToStatus(ret, "UploadBlob", container, blob);
  }

  Status ReadBlobChunk(const std::string& container,
                       const std::string& blob, uint64_t offset,
                       uint64_t size, char *data, uint64_t *read) override {
    //**TODO: This could be made more efficient by putting data directly into a stream...
    std::ostringstream oss;
    
    auto ret = blob_client_->get_chunk_to_stream_sync(container, blob, static_cast<unsigned long long>(offset),
                                                      static_cast<unsigned long long>(size), oss);
    if (ret.success()) {
      const auto & str = oss.str();
      *read = static_cast<uint64_t>(str.size());
      memcpy(data, str.data(), *read);
      return Status::OK();
    } else {
      return ToStatus(ret, "ReadBlobChunk", container, blob);
    }
  }
 private:
  std::shared_ptr<azure::storage_lite::blob_client> blob_client_;
};

class AzureWrappedClient : public AzureBlobClient {
 protected:
  Status ToStatus(int err, const std::string& method,
                  const std::string& container, const std::string& blob = "") {
    if (err == 0) {
      return Status::OK();
    } else {
      std::string message = method + ": " + std::to_string(err);
      if (blob.empty()) {
        return Status::IOError(container, message);
      } else {
        return Status::IOError(container + "://" + blob, message);
      }
    }
  }

 public:
  AzureWrappedClient(
      const std::shared_ptr<azure::storage_lite::blob_client>& client)
    : AzureBlobClient(client) {
    blob_client_wrapper_ = std::make_shared<azure::storage_lite::blob_client_wrapper>(client);
  }
  
  Status CreateContainer(const std::string& container) override {
    blob_client_wrapper_->create_container(container);
    return ToStatus(errno, "CreateContainer", container);
  }

  Status ExistsContainer(const std::string& container) override {
    bool exists = blob_client_wrapper_->container_exists(container);
    return (exists) ? Status::OK() : Status::NotFound();
  }

  Status DeleteBlob(const std::string& container,
                    const std::string& blob) override {
    blob_client_wrapper_->delete_blob(container, blob);
    return ToStatus(errno, "DeleteBlob", container, blob);
  }

  Status ExistsBlob(const std::string& container,
                    const std::string& blob) override {
    bool exists = blob_client_wrapper_->blob_exists(container, blob);
    return (exists) ? Status::OK() : Status::NotFound();
  }

  Status GetBlobProperties(
      const std::string& container, const std::string& blob,
      std::unordered_map<std::string, std::string>* metadata, uint64_t* size,
      uint64_t* modtime, std::string* tag) override {
    auto props = blob_client_wrapper_->get_blob_property(container, blob);
    if (errno != 0) {
      return ToStatus(errno, "GetBlobProperties", container, blob);
    } else {
      if (metadata != nullptr) {
        metadata->clear();
        for (auto prop : props.metadata) {
          metadata->insert(prop);
        }
      }
      if (size != nullptr) {
        *size = static_cast<uint64_t>(props.size);
      }
      if (modtime != nullptr) {
        *modtime = static_cast<uint64_t>(props.last_modified);
      }
      if (tag != nullptr) {
        *tag = props.etag;
      }
      return Status::OK();
    }
  }

  Status DownloadBlob(const std::string& container, const std::string& blob,
                      const std::string& local_file, uint64_t /*size*/) override {
    time_t unused;
    blob_client_wrapper_->download_blob_to_file(container, blob, local_file,
                                                unused);
    return ToStatus(errno, "DownloadBlob", container, blob);
  }

  Status UploadBlob(const std::string& local_file, const std::string& container,
                    const std::string& blob, uint64_t size) override {
    std::vector<std::pair<std::string, std::string>> metadata;
    blob_client_wrapper_->upload_file_to_blob(local_file, container, blob,
                                              metadata, size);
    return ToStatus(errno, "UploadBlob", container, blob);
  }

 private:
  std::shared_ptr<azure::storage_lite::blob_client_wrapper> blob_client_wrapper_;
};
#endif // USE_AZURE

Status AzureClient::Create(const CloudEnvOptions& cloud_opts, std::shared_ptr<AzureClient>* client) {
#ifdef USE_AZURE
  std::string user = getenv("AZURE_USER_NAME");
  std::string key  = getenv("AZURE_ACCESS_KEY");
  
  auto cred = std::make_shared<azure::storage_lite::shared_key_credential>(user, key);
  auto account = std::make_shared<azure::storage_lite::storage_account>(user, cred, /* use_https */ true);
  auto blob_client = std::make_shared<azure::storage_lite::blob_client>(account, 16);
  if (cloud_opts.use_aws_transfer_manager || true) {
    client->reset(new AzureBlobClient(blob_client));
  } else {
    client->reset(new AzureWrappedClient(blob_client));
  }
  return Status::OK();
#else
  (void) cloud_opts;
  client->reset();
  return Status::NotSupported("In order to use Azure, compile withUSE_AZURE=1");
#endif  // USE_AZURE
}
#ifdef USE_AZURE
class AzureWritableFile : public CloudStorageWritableFileImpl {
 public:
  AzureWritableFile(CloudEnv* env, const std::string& local_fname,
                 const std::string& bucket, const std::string& cloud_fname,
                 const EnvOptions& options)
      : CloudStorageWritableFileImpl(env, local_fname, bucket, cloud_fname,
                                     options) {}
  virtual const char* Name() const override {
    return AzureClient::kAzure();
  }
};

class AzureReadableFile : public CloudStorageReadableFileImpl {
 public:
  AzureReadableFile(const std::shared_ptr<AzureClient>& client,
                    Logger* info_log, const std::string& bucket,
                    const std::string& fname, uint64_t size,
                    std::string content_hash)
      : CloudStorageReadableFileImpl(info_log, bucket, fname, size),
        client_(client),
        content_hash_(std::move(content_hash)) {}

  virtual const char* Type() const {
    return AzureClient::kAzure();
  }

  virtual size_t GetUniqueId(char* id, size_t max_size) const override {
    if (content_hash_.empty()) {
      return 0;
    }

    max_size = std::min(content_hash_.size(), max_size);
    memcpy(id, content_hash_.c_str(), max_size);
    return max_size;
  }

  // random access, read data from specified offset in file
  Status DoCloudRead(uint64_t offset, size_t n, char* scratch,
                     uint64_t* bytes_read) const override {
    Status s = client_->ReadBlobChunk(bucket_, fname_, offset, n, scratch, bytes_read);
    return s;
  }

 private:
  std::shared_ptr<AzureClient> client_;
  std::string content_hash_;
};  // End class AzureReadableFile


/******************** S3StorageProvider ******************/
class AzureStorageProvider : public CloudStorageProviderImpl {
 public:
  ~AzureStorageProvider() override {}
  static const char* kClassName() { return AzureClient::kAzure(); }
  virtual const char* Name() const override { return AzureClient::kAzure(); }

  Status CreateBucket(const std::string& bucket) override;
  Status ExistsBucket(const std::string& bucket) override;
  // Empties all contents of the associated cloud storage bucket.
  // Status EmptyBucket(const std::string& bucket_name,
  //                   const std::string& object_path) override;
  // Delete the specified object from the specified cloud bucket
  Status DeleteCloudObject(const std::string& bucket_name,
                           const std::string& object_path) override;
  Status ExistsCloudObject(const std::string& bucket_name,
                           const std::string& object_path) override;
  Status GetCloudObjectSize(const std::string& bucket_name,
                            const std::string& object_path,
                            uint64_t* filesize) override;
  // Get the modification time of the object in cloud storage
  Status GetCloudObjectModificationTime(const std::string& bucket_name,
                                        const std::string& object_path,
                                        uint64_t* time) override;

  // Get the metadata of the object in cloud storage
  Status GetCloudObjectMetadata(const std::string& bucket_name,
                                const std::string& object_path,
                                CloudObjectInformation* info) override;

  Status PutCloudObjectMetadata(
      const std::string& bucket_name, const std::string& object_path,
      const std::unordered_map<std::string, std::string>& metadata) override;
  Status CopyCloudObject(const std::string& bucket_name_src,
                         const std::string& object_path_src,
                         const std::string& bucket_name_dest,
                         const std::string& object_path_dest) override;
  Status DoNewCloudReadableFile(
      const std::string& bucket, const std::string& fname, uint64_t fsize,
      const std::string& content_hash,
      std::unique_ptr<CloudStorageReadableFile>* result,
      const EnvOptions& options) override;
  Status NewCloudWritableFile(const std::string& local_path,
                              const std::string& bucket_name,
                              const std::string& object_path,
                              std::unique_ptr<CloudStorageWritableFile>* result,
                              const EnvOptions& options) override;
  Status PrepareOptions(const ConfigOptions& options) override;

 protected:
  Status DoListCloudObjects(const std::string& bucket_name,
                            const std::string& object_path,
                            int max_objects,
                            std::string* marker,
                            std::vector<std::string>* results) override;
  Status DoGetCloudObject(const std::string& bucket_name,
                          const std::string& object_path,
                          const std::string& destination,
                          uint64_t* remote_size) override;
  Status DoPutCloudObject(const std::string& local_file,
                          const std::string& bucket_name,
                          const std::string& object_path,
                          uint64_t file_size) override;

 private:
  // The blob client
  std::shared_ptr<AzureClient> azure_client_;
};

Status AzureStorageProvider::PrepareOptions(const ConfigOptions& options) {
  auto cenv = static_cast<CloudEnv*>(options.env);
  const CloudEnvOptions& cloud_opts = cenv->GetCloudEnvOptions();
  Status s = AzureClient::Create(cloud_opts, &azure_client_);
  Header(cenv->GetLogger(), "Azure connection to endpoint");
  if (s.ok()) {
    s = CloudStorageProviderImpl::PrepareOptions(options);
  }
  return s;
}
Status AzureStorageProvider::CreateBucket(const std::string& bucket) {
  return azure_client_->CreateContainer(bucket);
}

Status AzureStorageProvider::ExistsBucket(const std::string& bucket) {
  return azure_client_->ExistsContainer(bucket);
}

Status AzureStorageProvider::DeleteCloudObject(const std::string& bucket_name,
                                               const std::string& object_path) {
  return azure_client_->DeleteBlob(bucket_name, object_path);
}

// Delete the specified object from the specified cloud bucket
Status AzureStorageProvider::ExistsCloudObject(const std::string& bucket_name,
                                               const std::string& object_path) {
  return azure_client_->ExistsBlob(bucket_name, object_path);
}

Status AzureStorageProvider::DoListCloudObjects(const std::string& bucket_name,
                                                const std::string& object_path,
                                                int max_objects,
                                                std::string* marker,
                                                std::vector<std::string>* results) {
  return azure_client_->ListBlobs(bucket_name, object_path, max_objects, marker, results);
}

// Return size of cloud object
Status AzureStorageProvider::GetCloudObjectSize(const std::string& bucket_name,
                                                const std::string& object_path,
                                                uint64_t* filesize) {
  return azure_client_->GetBlobProperties(bucket_name, object_path, nullptr,
                                          filesize, nullptr, nullptr);
}

Status AzureStorageProvider::GetCloudObjectModificationTime(
    const std::string& bucket_name, const std::string& object_path,
    uint64_t* time) {
  return azure_client_->GetBlobProperties(bucket_name, object_path, nullptr,
                                          nullptr, time, nullptr);
}

Status AzureStorageProvider::GetCloudObjectMetadata(
    const std::string& bucket_name, const std::string& object_path,
    CloudObjectInformation* info) {
  assert(info != nullptr);
  return azure_client_->GetBlobProperties(
      bucket_name, object_path, &info->metadata, &info->size,
      &info->modification_time, &info->content_hash);
}

Status AzureStorageProvider::PutCloudObjectMetadata(
    const std::string& bucket_name, const std::string& object_path,
    const std::unordered_map<std::string, std::string>& metadata) {
  return azure_client_->SetBlobMetadata(bucket_name, object_path, metadata);
}

Status AzureStorageProvider::CopyCloudObject(
    const std::string& bucket_name_src, const std::string& object_path_src,
    const std::string& bucket_name_dest, const std::string& object_path_dest) {
  return azure_client_->CopyBlob(bucket_name_src, object_path_src,
                                 bucket_name_dest, object_path_dest);
}
  
Status AzureStorageProvider::DoNewCloudReadableFile(
      const std::string& bucket, const std::string& fname, uint64_t fsize,
      const std::string& content_hash,
      std::unique_ptr<CloudStorageReadableFile>* result,
      const EnvOptions& /*options*/) {
  result->reset(new AzureReadableFile(azure_client_, env_->GetLogger(), bucket, fname,
                                      fsize, content_hash));
  return Status::OK();
}

Status AzureStorageProvider::NewCloudWritableFile(const std::string& local_path,
                                                  const std::string& bucket_name,
                                                  const std::string& object_path,
                                                  std::unique_ptr<CloudStorageWritableFile>* result,
                                                  const EnvOptions& options) {
  //**TODO: Blobs and SST files should be treated differently...
  result->reset(new AzureWritableFile(env_, local_path, bucket_name, object_path, options));
  return (*result)->status();
}
  
Status AzureStorageProvider::DoGetCloudObject(const std::string& bucket_name,
                                              const std::string& object_path,
                                              const std::string& destination,
                                              uint64_t* remote_size) {
  Status s = GetCloudObjectSize(bucket_name, object_path, remote_size);
  if (s.ok()) {
    azure_client_->DownloadBlob(bucket_name, object_path, destination, *remote_size);
  }
  return s;
}

Status AzureStorageProvider::DoPutCloudObject(const std::string& local_file,
                                              const std::string& bucket_name,
                                              const std::string& object_path,
                                              uint64_t file_size) {
  return azure_client_->UploadBlob(local_file, bucket_name, object_path,
                                   file_size);
}
#endif // USE_AZURE
  
int CloudEnvImpl::RegisterAzureObjects(ObjectLibrary& library,
                                       const std::string& /*arg*/) {
  int count = 0;
  library.Register<CloudStorageProvider>(  // s3
      AzureClient::kAzure(),
      [](const std::string& /*uri*/,
         std::unique_ptr<CloudStorageProvider>* guard, std::string* errmsg) {
#ifndef USE_AZURE
        guard->reset();
        *errmsg =
            "In order to use Azure, make sure you're compiling with "
            "USE_AZURE=1";
#else
        errmsg->clear();
        guard->reset(new AzureStorageProvider());
#endif /* USE_AWS */
        return guard->get();
      });
  count++;
  return count;
}
}  // namespace ROCKSDB_NAMESPACE
