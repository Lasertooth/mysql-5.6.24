/*
   copyright (c) 2013 monty program ab

   this program is free software; you can redistribute it and/or modify
   it under the terms of the gnu general public license as published by
   the free software foundation; version 2 of the license.

   this program is distributed in the hope that it will be useful,
   but without any warranty; without even the implied warranty of
   merchantability or fitness for a particular purpose.  see the
   gnu general public license for more details.

   you should have received a copy of the gnu general public license
   along with this program; if not, write to the free software
   foundation, inc., 59 temple place, suite 330, boston, ma  02111-1307  usa */

#include "my_global.h"                   /* ulonglong */
#include "my_base.h"                     /* ha_rows */
#include "my_sys.h"
#include "my_tree.h"

#include "applyiter.h"

int compare_mem_comparable_keys(const uchar *a, size_t a_len, const uchar *b, size_t b_len);

ApplyChangesIter::ApplyChangesIter() :
  trx(NULL), cocdb(NULL) {}


ApplyChangesIter::~ApplyChangesIter() {
  delete trx;
  delete cocdb;
}


void ApplyChangesIter::init(RowTable* trx_arg, CockroachIterator *it) {
  delete trx;
  delete cocdb;

  trx = new RowTableIter(trx_arg);
  cocdb = it;

  valid = false;
}


void ApplyChangesIter::Next() {
  if (cur_is_trx) {
    trx->Next();
  } else {
    cocdb->Next();
  }
  advance(1);
}

void ApplyChangesIter::Seek(cockroachdb::Slice &key) {
  cocdb->Seek(key);
  trx->Seek(key);
  advance(1);
}

/*
  @param direction  1 means forward, -1 means backward.
*/

void ApplyChangesIter::advance(int direction)
{
  valid= true;
  while (1)
  {
    if (!trx->Valid() && !cocdb->Valid())
    {
      // ok we got here if neither scan nor trx have any records.
      cur_is_trx= false;  //just set it to something
      valid= false;
      return;
    }

    if (!trx->Valid())
    {
      /* Got record from cockroachdb but not from trx */
      cur_is_trx= false;
      break;
    }

    if (!cocdb->Valid())
    {
      cur_is_trx= true;
      if (trx->is_tombstone())
      {
        if (direction == 1)
          trx->Next();
        continue;  /* A tombstone.. (but no matching record? odd..) */
      }
      break;
    }

    if (cocdb->Valid() && trx->Valid())
    {
      cockroachdb::Slice rdb_key= cocdb->key();
      cockroachdb::Slice trx_key= trx->key();
      int cmp= direction *
               compare_mem_comparable_keys((const uchar*)trx_key.data(), trx_key.size(),
                                           (const uchar*)rdb_key.data(), rdb_key.size());
      if (!cmp) // keys are equal
      {
        if (trx->is_tombstone())
        {
          /* cockroachdb has a record, but trx says we have deleted it */
          if (direction == 1)
          {
            cocdb->Next();
            trx->Next();
          }
          continue;  // restart the logic
        }

        /* trx has a newer version of the record */
        if (direction == 1)
          cocdb->Next();
        cur_is_trx= true;
        break;
      }
      else if (cmp > 0)
      {
        /* record from cockroachdb comes first */
        cur_is_trx= false;
        break;
      }
      else // cmp < 0
      {
        /* record from transaction comes first */
        if (trx->is_tombstone())
        {
          if (direction == 1)
            trx->Next();
          continue;  /* A tombstone.. (but no matching record? odd..) */
        }
        /* A record from transaction but not in the db */
        cur_is_trx= true;
        break;
      }
    }
  }
}


cockroachdb::Slice ApplyChangesIter::value()
{
  if (cur_is_trx)
    return trx->value();
  else
    return cocdb->value();
}


cockroachdb::Slice ApplyChangesIter::key()
{
  if (cur_is_trx)
    return trx->key();
  else
    return cocdb->key();
}
