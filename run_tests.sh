#!/bin/bash

set -eu

args=$(getopt -o '+hj:' -l 'help,parallel_jobs:' -n "$0" -- "$@")
eval set -- "$args"

set +e
read -r -d '' usage_str <<END
Usage:
  $0 [options...]

Entry-point for the RocksDB-Cloud test script.

Options:
  -h, --help               Help
  -j, --parallel_jobs      Quiet
END
set -e

die_usage() {
  echo "$@" >&2
  echo >&2
  echo "$usage_str" >&2
  exit 1
}

while :; do
  case "$1" in
      -h|--help)
          echo "$usage_str"
          exit 0
          ;;
      -j|--parallel_jobs)
          PARALLEL_JOBS="$2"
          shift
          ;;
      --)
          shift
          break
          ;;
      *)
          die "Internal error: unknown option $1"
  esac
  shift
done

echo "Running with $PARALLEL_JOBS parallel jobs"

echo "Pulling base image..."
docker pull rockset/rocksdb_cloud_runtime:test

export SRC_ROOT=$(git rev-parse --show-toplevel)

echo "Building tests..."
docker run -v $SRC_ROOT:/opt/rocksdb-cloud/src -w /opt/rocksdb-cloud/src \
    -u $UID -e V=1 -e USE_AWS=1 -e USE_KAFKA=1 \
    --rm rockset/rocksdb_cloud_runtime:test \
    /bin/bash -c "make -j $PARALLEL_JOBS db_test db_test2 db_basic_test env_basic_test db_cloud_test cloud_manifest_test"

echo "Running db_test2, db_basic_test and env_basic_test"
docker run --network none -v $SRC_ROOT:/opt/rocksdb-cloud/src -w /opt/rocksdb-cloud/src \
    -u $UID -e V=1 -e USE_AWS=1 -e USE_KAFKA=1 \
    --rm rockset/rocksdb_cloud_runtime:test \
    /bin/bash -c "./db_test2 && ./db_basic_test && ./env_basic_test"

echo "Running cloud tests..."
docker run --network none -v $SRC_ROOT:/opt/rocksdb-cloud/src -w /opt/rocksdb-cloud/src \
    -u $UID -e V=1 -e USE_AWS=1 -e USE_KAFKA=1 \
    --rm rockset/rocksdb_cloud_runtime:test \
    /bin/bash -c "./cloud_manifest_test && ./db_cloud_test --gtest_filter=-CloudTest.KeepLocalLogKafka"

echo "Running db_test. This test might take a while. Get some coffee :)"
docker run --network none -v $SRC_ROOT:/opt/rocksdb-cloud/src -w /opt/rocksdb-cloud/src \
    -u $UID -e V=1 -e USE_AWS=1 -e USE_KAFKA=1 \
    --rm rockset/rocksdb_cloud_runtime:test \
    /bin/bash -c "./db_test --gtest_filter=-*MultiThreadedDBTest*"
