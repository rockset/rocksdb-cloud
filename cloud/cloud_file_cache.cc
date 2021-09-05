// Copyright (c) 2021 Rockset.
#ifndef ROCKSDB_LITE

#include <cinttypes>

#include "cloud/cloud_env_impl.h"

namespace ROCKSDB_NAMESPACE {

namespace {

// The Value inside every cached entry
struct Value {
  std::string path;
  CloudEnvImpl* cenv;

  Value(const std::string& _path, CloudEnvImpl* _cenv)
      : path(_path), cenv(_cenv) {}
};

// static method to use as a callback from the cache.
static void DeleteEntry(const Slice& key, void* v) {
  Value* value = reinterpret_cast<Value*>(v);
  std::string filename(key.data(), key.size());
  value->cenv->FileCacheDeleter(filename);
  delete value;
}

// These are used to retrieve all the values from the cache.
// Only used for unit tests.
static Value* DecodeValue(void* v) {
  return static_cast<Value*>(reinterpret_cast<Value*>(v));
}

static std::vector<std::pair<Value*, uint64_t>> callback_state;
static void callback(void* entry, size_t charge) {
  callback_state.push_back({DecodeValue(entry), charge});
}
static void clear_callback_state() { callback_state.clear(); }
}  // namespace

//
// Touch the file so that is the the most-recent LRU item in cache.
//
void CloudEnvImpl::FileCacheAccess(const std::string& fname) {
  if (!cloud_env_options.hasSstFileCache()) {
    return;
  }
  Slice key(fname);
  Cache::Handle* handle = cloud_env_options.sst_file_cache->Lookup(key);
  if (handle) {
    cloud_env_options.sst_file_cache->Release(handle);
  }
  log(InfoLogLevel::DEBUG_LEVEL, fname, "access");
}

//
// Record the file into the cache.
//
void CloudEnvImpl::FileCacheInsert(const std::string& fname,
                                   uint64_t filesize) {
  if (!cloud_env_options.hasSstFileCache()) {
    return;
  }

  // insert into cache, key is the file path.
  Slice key(fname);
  cloud_env_options.sst_file_cache->Insert(key, new Value(fname, this),
                                           filesize, DeleteEntry);
  log(InfoLogLevel::INFO_LEVEL, fname, "insert");
}

//
// Remove a specific entry from the cache.
//
void CloudEnvImpl::FileCacheErase(const std::string& fname) {
  // We erase from the cache even if the cache size is zero. This is needed
  // to protect against the when the cache size was dynamically reduced to zero
  // on a running database.
  if (!cloud_env_options.sst_file_cache) {
    return;
  }

  Slice key(fname);
  cloud_env_options.sst_file_cache->Erase(key);
  log(InfoLogLevel::INFO_LEVEL, fname, "erased");
}

//
// When the cache is full, delete files from local store
//
void CloudEnvImpl::FileCacheDeleter(const std::string& fname) {
  Status st = base_env_->DeleteFile(fname);
  log(InfoLogLevel::INFO_LEVEL, fname, "purged");
}

//
// Get total charge in the cache.
// This is not thread-safe and is used only for unit tests.
//
uint64_t CloudEnvImpl::FileCacheGetCharge() {
  clear_callback_state();
  cloud_env_options.sst_file_cache->ApplyToAllCacheEntries(callback, true);
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
uint64_t CloudEnvImpl::FileCacheGetNumItems() {
  clear_callback_state();
  cloud_env_options.sst_file_cache->ApplyToAllCacheEntries(callback, true);
  return callback_state.size();
}

// Removes all items for the env from the cache.
// This is not thread-safe.
void CloudEnvImpl::FileCachePurge() {
  // We erase from the cache even if the cache size is zero. This is needed
  // to protect against the when the cache size was dynamically reduced to zero
  // on a running database.
  if (!cloud_env_options.sst_file_cache) {
    return;
  }
  // fetch all items from cache
  clear_callback_state();
  cloud_env_options.sst_file_cache->ApplyToAllCacheEntries(callback, true);
  // for all those items that have a matching cenv, remove them from cache.
  uint64_t count = 0;
  for (auto& it : callback_state) {
    Value* value = it.first;
    if (value->cenv == this) {
      Slice key(value->path);
      cloud_env_options.sst_file_cache->Erase(key);
      count++;
    }
  }
  log(InfoLogLevel::INFO_LEVEL, "ENV-DELETE", "purged");
}

void CloudEnvImpl::log(InfoLogLevel level, const std::string& fname,
                       const std::string& msg) {
  uint64_t usage = cloud_env_options.sst_file_cache->GetUsage();
  uint64_t capacity = cloud_env_options.sst_file_cache->GetCapacity();
  auto percent = (capacity > 0 ? (100L * usage / capacity) : 0);
  Log(level, info_log_,
      "[%s] FileCache %s %s cache-used %" PRIu64 "/%" PRIu64 "(%ld%%) bytes",
      Name(), fname.c_str(), msg.c_str(), usage, capacity, percent);
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
