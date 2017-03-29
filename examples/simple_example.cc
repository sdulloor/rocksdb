// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <cstdio>
#include <string>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/backupable_db.h"

#include <vector>

using namespace rocksdb;

std::string kDBPath = "/tmp/rocksdb_simple_example";

int main() {
  DB* db;
  Options options;

  // set the affinity of background threads
  options.env->set_affinity(16, 32);

  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;

  // create background threads
#if 1
  int num_threads = 16;
  options.PrepareForBulkLoad();
  options.write_buffer_size = 1024 * 1024 * 256;
  options.target_file_size_base = 1024 * 1024 * 512;
  options.IncreaseParallelism(num_threads);
  options.max_background_compactions = num_threads;
  options.max_background_flushes = num_threads;
  options.max_write_buffer_number = num_threads;
  //options.min_write_buffer_number_to_merge = max(num_threads/2, 1);
  //options.compaction_style = rocksdb::kCompactionStyleNone;
  //options.memtable_factory.reset(new rocksdb::VectorRepFactory(1000));
#endif

  // open DB
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  // Put key-value
  s = db->Put(WriteOptions(), "key1", "value");
  assert(s.ok());
  std::string value;
  // get value
  s = db->Get(ReadOptions(), "key1", &value);
  assert(s.ok());
  assert(value == "value");

  // atomically apply a set of updates
  {
    WriteBatch batch;
    batch.Delete("key1");
    batch.Put("key2", value);
    s = db->Write(WriteOptions(), &batch);
  }

  s = db->Get(ReadOptions(), "key1", &value);
  assert(s.IsNotFound());

  db->Get(ReadOptions(), "key2", &value);
  assert(value == "value");

  // Backup
  BackupEngine* backup_engine;
  s = BackupEngine::Open(Env::Default(), BackupableDBOptions("/tmp/rocksdb_backup"), &backup_engine);
  assert(s.ok());
  s = backup_engine->CreateNewBackup(db);
  assert(s.ok());
  std::vector<BackupInfo> backup_info;
  backup_engine->GetBackupInfo(&backup_info);
  s = backup_engine->VerifyBackup(1);
  assert(s.ok());

  delete db;
  delete backup_engine;

  return 0;
}
