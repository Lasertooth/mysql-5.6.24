#ifndef APPLY_ITERATOR_H_
#define APPLY_ITERATOR_H_

#include "cockroachdb.h"
#include "row_table.h"
#include "slice.h"

class RowTable;
class RowTableIter;

class ApplyChangesIter {
  bool valid;
  bool cur_is_trx;

  RowTableIter *trx;
  CockroachIterator *cocdb;

 public:
  ApplyChangesIter();
  ~ApplyChangesIter();

  void init(RowTable *trx_arg, CockroachIterator *rdb_arg);

  void Next();

  void Seek(cockroachdb::Slice &key);

  bool Valid() {
    return valid;
  }

  cockroachdb::Slice key();
  cockroachdb::Slice value();

 private:
  void advance(int direction);
};

#endif
