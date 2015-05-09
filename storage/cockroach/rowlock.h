#ifndef COCKROACHDB_ROW_LOCK_H_
#define COCKROACHDB_ROW_LOCK_H_

#include "my_sys.h"
#include "lf.h"

/*
   A row lock that one gets from LockTable.

note: the structure is stored in LF_HASH, which will copy a part of
structure with memcpy(). See LockTable::init().
*/
class Row_lock {
 public:
  char *rowkey; /* The key this lock is for */
  int len; /* length of the rowkey */

  /* TRUE - this row_lock is being deleted */
  bool deleted;

  /* How many are waiting for the lock */
  int waiters;

  /*
     busy==0 - means free
     busy>=1 - means the lock is occupied, the number tells how many rows the
     lock was acquired
     */
  int busy;

  /*
     Some opaque data that identifies the lock owner.  This is needed so we can
     tell if this is the lock owner requesting the lock the second time, or
     somebody else.
     */
  void *owner_data;

  /*
     One must hold this mutex
     - when marking lock as busy or free
     - when adding/removing himself from waiters
     the mutex is also associated with the condition when waiting for the lock.
     */
  mysql_mutex_t mutex;
  mysql_cond_t cond;
};


/*
   A table of locks. It is backed by a lock-free hash.

   INTERNALS
   - Locks are exclusive.
   - If a thread has an element in the hashtable, it has a lock.
   */

class LockTable
{
  public:
    LF_HASH lf_hash;

  public:
    void init(lf_key_comparison_func_t key_cmp_func,
        lf_hashfunc_t hashfunc);

    void cleanup();
    /*
       Before using the LockTable, each thread should get its own "pins".
       */
    LF_PINS* get_pins() { return lf_hash_get_pins(&lf_hash); }
    void put_pins(LF_PINS *pins) { return lf_hash_put_pins(pins); }

    Row_lock* get_lock(LF_PINS *pins, const uchar* key, size_t keylen,
        int timeout_sec);
    void release_lock(LF_PINS *pins, Row_lock *own_lock);
};

#endif
