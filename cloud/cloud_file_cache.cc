// Copyright (c) 2021 Rockset.
#ifndef ROCKSDB_LITE

#include <cinttypes>

#include "rocksdb/cloud/cloud_file_system_impl.h"

namespace ROCKSDB_NAMESPACE {

namespace {

// The Value inside every cached entry
struct Value {
  std::string path;
  CloudFileSystemImpl* cfs;

  Value(const std::string& _path, CloudFileSystemImpl* _cfs)
      : path(_path), cfs(_cfs) {}
};

// static method to use as a callback from the cache.
// static void DeleteEntry(const Slice& key, void* v) {
// Value* value = reinterpret_cast<Value*>(v);
// std::string filename(key.data(), key.size());
// value->cfs->FileCacheDeleter(filename);
// delete value;
//}

// static method to use as a callback from the cache.
static void DeleteEntry(Cache::ObjectPtr v, MemoryAllocator* /*allocator*/) {
  Value* value = reinterpret_cast<Value*>(v);
  std::string filename(value->path);
  value->cfs->FileCacheDeleter(filename);
  delete value;
}

static Cache::CacheItemHelper cache_helper(CacheEntryRole::kMisc, &DeleteEntry);

// These are used to retrieve all the values from the cache.
// Only used for unit tests.
static Value* DecodeValue(void* v) {
  return static_cast<Value*>(reinterpret_cast<Value*>(v));
}

static std::vector<std::pair<Value*, uint64_t>> callback_state;
//static void callback(void* entry, size_t charge) {
  //callback_state.push_back({DecodeValue(entry), charge});
//}
static void callback(const Slice& /*key*/, Cache::ObjectPtr obj, size_t charge,
                               const Cache::CacheItemHelper* /*helper*/) {
  callback_state.push_back({DecodeValue(obj), charge});
}
static void clear_callback_state() { callback_state.clear(); }
}  // namespace

//
// Touch the file so that is the the most-recent LRU item in cache.
//
void CloudFileSystemImpl::FileCacheAccess(const std::string& fname) {
  if (!cloud_fs_options.hasSstFileCache()) {
    return;
  }
  Slice key(fname);
  Cache::Handle* handle = cloud_fs_options.sst_file_cache->Lookup(key);

  if (handle) {
    cloud_fs_options.sst_file_cache->Release(handle);
  }
  log(InfoLogLevel::DEBUG_LEVEL, fname, "access");
}

//
// Record the file into the cache.
//
void CloudFileSystemImpl::FileCacheInsert(const std::string& fname,
                                          uint64_t filesize) {
  if (!cloud_fs_options.hasSstFileCache()) {
    return;
  }

  // insert into cache, key is the file path.
  Slice key(fname);
  cloud_fs_options.sst_file_cache->Insert(key, new Value(fname, this),
                                          &cache_helper, filesize);
  log(InfoLogLevel::INFO_LEVEL, fname, "insert");
}

//
// Remove a specific entry from the cache.
//
void CloudFileSystemImpl::FileCacheErase(const std::string& fname) {
  // We erase from the cache even if the cache size is zero. This is needed
  // to protect against the when the cache size was dynamically reduced to zero
  // on a running database.
  if (!cloud_fs_options.sst_file_cache) {
    return;
  }

  Slice key(fname);
  cloud_fs_options.sst_file_cache->Erase(key);
  log(InfoLogLevel::INFO_LEVEL, fname, "erased");
}

//
// When the cache is full, delete files from local store
//
void CloudFileSystemImpl::FileCacheDeleter(const std::string& fname) {
  base_fs_->DeleteFile(fname, IOOptions(), nullptr /*dbg*/);
  log(InfoLogLevel::INFO_LEVEL, fname, "purged");
}

//
// Get total charge in the cache.
// This is not thread-safe and is used only for unit tests.
//
uint64_t CloudFileSystemImpl::FileCacheGetCharge() {
  clear_callback_state();
  Cache::ApplyToAllEntriesOptions atae_opts;
  atae_opts.average_entries_per_lock = UINT32_MAX;
  cloud_fs_options.sst_file_cache->ApplyToAllEntries(callback, atae_opts);
  uint64_t total = 0;
  for (auto& it : callback_state) {
    total += it.second;
  }
  return total;
}

//
// Get total number of items in the cache.
// This is not thread-safe and is used only for unit tests.
//
uint64_t CloudFileSystemImpl::FileCacheGetNumItems() {
  clear_callback_state();
  Cache::ApplyToAllEntriesOptions atae_opts;
  atae_opts.average_entries_per_lock = UINT32_MAX;
  cloud_fs_options.sst_file_cache->ApplyToAllEntries(callback, atae_opts);
  return callback_state.size();
}

// Removes all items for the env from the cache.
// This is not thread-safe.
void CloudFileSystemImpl::FileCachePurge() {
  // We erase from the cache even if the cache size is zero. This is needed
  // to protect against the when the cache size was dynamically reduced to zero
  // on a running database.
  if (!cloud_fs_options.sst_file_cache) {
    return;
  }
  // fetch all items from cache
  clear_callback_state();
  Cache::ApplyToAllEntriesOptions atae_opts;
  atae_opts.average_entries_per_lock = UINT32_MAX;
  cloud_fs_options.sst_file_cache->ApplyToAllEntries(callback, atae_opts);
  // for all those items that have a matching cfs, remove them from cache.
  for (auto& it : callback_state) {
    Value* value = it.first;
    if (value->cfs == this) {
      Slice key(value->path);
      cloud_fs_options.sst_file_cache->Erase(key);
    }
  }
  log(InfoLogLevel::INFO_LEVEL, "ENV-DELETE", "purged");
}

void CloudFileSystemImpl::log(InfoLogLevel level, const std::string& fname,
                              const std::string& msg) {
  uint64_t usage = cloud_fs_options.sst_file_cache->GetUsage();
  uint64_t capacity = cloud_fs_options.sst_file_cache->GetCapacity();
  long percent = (capacity > 0 ? (100L * usage / capacity) : 0);
  Log(level, info_log_,
      "[%s] FileCache %s %s cache-used %" PRIu64 "/%" PRIu64 "(%ld%%) bytes",
      Name(), fname.c_str(), msg.c_str(), usage, capacity, percent);
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
