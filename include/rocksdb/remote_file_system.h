#include <rocksdb/cloud/cloud_env_options.h>
#include <rocksdb/cloud/cloud_storage_provider.h>
#include <rocksdb/file_system.h>
#include <rocksdb/io_status.h>

namespace rocksdb {
class RemoteFileSystem : public FileSystemWrapper {
 public:
  RemoteFileSystem(std::shared_ptr<FileSystem> default_fs,
                   const CloudEnvOptions& cloud_options)
      : FileSystemWrapper(default_fs),
        cloud_storage_provider_(cloud_options.storage_provider),
        src_bucket_(cloud_options.src_bucket),
        dest_bucket_(cloud_options.dest_bucket) {}

  IOStatus NewSequentialFile(const std::string& f, const FileOptions& file_opts,
                             std::unique_ptr<FSSequentialFile>* r,
                             IODebugContext* dbg);

  IOStatus NewRandomAccessFile(const std::string& f,
                               const FileOptions& file_opts,
                               std::unique_ptr<FSRandomAccessFile>* r,
                               IODebugContext* dbg);

  IOStatus NewWritableFile(const std::string& f, const FileOptions& file_opts,
                           std::unique_ptr<FSWritableFile>* r,
                           IODebugContext* dbg);

 private:
  std::string destname(const std::string& localname);
  bool isSstFile(const std::string& f);

  const std::shared_ptr<CloudStorageProvider>& cloud_storage_provider_;
  BucketOptions src_bucket_;
  BucketOptions dest_bucket_;
  CloudEnv* cloud_env_;
};

}  // namespace rocksdb
