//  Copyright (c) 2013-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef GFLAGS
#include <cstdio>
int main() {
  fprintf(stderr, "Please install gflags to run rocksdb tools\n");
  return 1;
}
#else
#include "rocksdb/db_bench_tool.h"
#ifndef ROCKSDB_LITE
#ifdef USE_AWS
#include <aws/core/Aws.h>
#endif
#endif
int main(int argc, char** argv) {
#ifndef ROCKSDB_LITE
#ifdef USE_AWS
    Aws::SDKOptions aws_options;
    aws_options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;
    Aws::InitAPI(aws_options);
#endif
#endif
   int res = ROCKSDB_NAMESPACE::db_bench_tool(argc, argv);
#ifndef ROCKSDB_LITE
#ifdef USE_AWS
    Aws::ShutdownAPI(aws_options);
#endif
#endif
    return res;
}
#endif  // GFLAGS
