#ifndef COCKROACHDB_H_
#define COCKROACHDB_H_

#include "slice.h"

class CockroachDB;
class CockroachIterator {
 public:
  CockroachIterator(CockroachDB* db, const std::string& trx_id) {}

  void Seek(const cockroachdb::Slice& key) {
  }

  bool Valid() {
    return false;
  }

  bool Next() {
    return true;
  }

  cockroachdb::Slice key() {
    return key_;
  }

  cockroachdb::Slice value() {
    return val_;
  }
 private:
  cockroachdb::Slice key_;
  cockroachdb::Slice val_;
};

class CockroachDB {
 public:
  bool Put(const cockroachdb::Slice& key, const cockroachdb::Slice& val) {
    return true;
  }

  bool Delete(const cockroachdb::Slice& key) {
    return true;
  }
};

#endif
