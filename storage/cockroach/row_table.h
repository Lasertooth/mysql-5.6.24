#ifndef ROW_TABLE_H_
#define ROW_TABLE_H_

#include "my_tree.h"
#include "slice.h"

#define test(a) ((a) ? 1 : 0)

typedef struct st_row_data {
  size_t key_len;
  size_t value_len;
  struct st_row_data *prev_version;
  int stmt_id;
} ROW_DATA;

const size_t ROW_DATA_SIZE= ALIGN_SIZE(sizeof(ROW_DATA));
const size_t DATA_IS_TOMBSTONE= size_t(-1);

class RowTable;

/*
  A iterator for traversing contents of Row_table.
  Changes (insertion/removal of records) to the underlying Row_table will not
  invalidate this iterator (internally, the iterator will detect the change
  and re-position itself).

  The iterator uses ideas from B-TREE index scans on ha_heap tables.
*/

class RowTableIter {
  RowTable *rtable; /* Table this iterator is for*/

  /* The following are for tree iteration: */
  TREE_ELEMENT *parents[MAX_TREE_HEIGHT+1];
  TREE_ELEMENT **last_pos;
  ROW_DATA **row_ptr;

  /*
    If rtable->change_id is greater than ours, the iterator is invalidated and
    we need to re-position in the tree
  */
  int change_id;
  friend class RowTable;
public:
  RowTableIter(RowTable *rtable_arg);

  /* Scanning functions */
  void Seek(const cockroachdb::Slice &slice);
  void SeekToFirst();
  void SeekToLast();

  void Next();
  void Prev();

  /* Functions to get information about the current element */
  bool Valid();
  bool is_tombstone();
  cockroachdb::Slice key();
  cockroachdb::Slice value();
};

/*
  A storage for rows, or their tombstones. One can use rocksdb-like iterators
  to traverse the rows.
*/

class RowTable {
  TREE tree;
  MEM_ROOT mem_root;

  /* Current statement id */
  int stmt_id;

  /*
    This is incremented on every change, so iterators can know 
    if they were invalidated and should re-position themselves.
  */
  int change_id;

  friend class RowTableIter;
public:
  /* This is like a constructor */
  void init();

  void cleanup();
  void reinit();

  /* Operations to put a row, or a tombstone */
  bool Put(cockroachdb::Slice& key, cockroachdb::Slice& val);
  bool Delete(cockroachdb::Slice& key);

  /* Lookup may find nothing, find row, of find a tombstone */
  bool Get(cockroachdb::Slice &key, std::string *record, bool *found);

  /*
    Statement support. It is possible to rollback all changes made by the
    current statement.
  */
  void start_stmt();
  void rollback_stmt();

  /* This may return false when there are really no changes (TODO: still true?) */
  bool is_empty() {
    return test(tree.elements_in_tree == 0);
  };

private:
  static int compare_rows(const void* arg, const void *a,const void *b);
};


#endif
