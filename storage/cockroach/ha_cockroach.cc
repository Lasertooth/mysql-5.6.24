/*
   Copyright (c) 2012, Monty Program Ab

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation        // gcc: Class implementation
#endif

#include <mysql/plugin.h>
#include "ha_cockroach.h"
#include "sql_class.h"
#include "sql_array.h"

#include "my_bit.h"

static handler *cockroachdb_create_handler(handlerton *hton,
        TABLE_SHARE *table,
        MEM_ROOT *mem_root);

void key_copy(uchar *to_key, uchar *from_record, KEY *key_info,
        uint key_length);

handlerton *cockroachdb_hton;

/*
   Hash used to track the number of open tables; variable for example share
   methods
   */
static HASH cockroachdb_open_tables;

/* The mutex used to init the hash; variable for example share methods */
mysql_mutex_t cockroachdb_mutex;


//TODO: 0 means don't wait at all, and we don't support it yet?
static MYSQL_THDVAR_ULONG(lock_wait_timeout, PLUGIN_VAR_RQCMDARG,
        "Number of seconds to wait for lock",
        NULL, NULL, /*default*/ 1, /*min*/ 1, /*max*/ 1024*1024*1024, 0);

static MYSQL_THDVAR_BOOL(bulk_load, PLUGIN_VAR_RQCMDARG,
        "Use bulk-load mode for inserts", NULL, NULL, FALSE);

static MYSQL_THDVAR_ULONG(max_row_locks, PLUGIN_VAR_RQCMDARG,
        "Maximum number of locks a transaction can have",
        NULL, NULL, /*default*/ 1024*1024*1024, /*min*/ 1, /*max*/ 1024*1024*1024, 0);

static MYSQL_THDVAR_ULONG(bulk_load_size, PLUGIN_VAR_RQCMDARG,
        "Max #records in a batch for bulk-load mode",
        NULL, NULL, /*default*/ 1000, /*min*/ 1, /*max*/ 1024*1024*1024, 0);

static struct st_mysql_sys_var* cockroachdb_system_variables[]= {
    MYSQL_SYSVAR(lock_wait_timeout),
    MYSQL_SYSVAR(max_row_locks),
    MYSQL_SYSVAR(bulk_load),
    MYSQL_SYSVAR(bulk_load_size),
    NULL
};


#if 0
static SHOW_VAR cockroachdb_status_variables[]= {
    {"row_insert_batches",
        (char*) &cockroachdb_counters.row_insert_batches,  SHOW_LONG},
    ...
    {NullS, NullS, SHOW_LONG}
};
#endif

///////////////////////////////////////////////////////////////////////////////////////////

/**
  @brief
  Function we use in the creation of our hash to get key.
  */

static uchar* cockroachdb_get_key(COCKROACHDB_SHARE *share, size_t *length,
        my_bool not_used __attribute__((unused)))
{
    *length=share->table_name_length;
    return (uchar*) share->table_name;
}

/*
   The following is needed as an argument for thd_enter_cond, irrespectively of
   whether we're compiling with P_S or not.
   */
PSI_stage_info stage_waiting_on_row_lock= { 0, "Waiting for row lock", 0};

static PSI_stage_info *all_cockroachdb_stages[]=
{
    & stage_waiting_on_row_lock
};


#ifdef HAVE_PSI_INTERFACE
static PSI_mutex_key ex_key_mutex_example, ex_key_mutex_cockroachdb_SHARE_mutex;

static PSI_mutex_info all_cockroachdb_mutexes[]=
{
    { &ex_key_mutex_example, "cockroachdb", PSI_FLAG_GLOBAL},
    { &ex_key_mutex_cockroachdb_SHARE_mutex, "cockroachdb_SHARE::mutex", 0}
};

static void init_cockroachdb_psi_keys()
{
    const char* category= "cockroachdb";
    int count;

    if (PSI_server == NULL)
        return;

    count= array_elements(all_cockroachdb_mutexes);
    PSI_server->register_mutex(category, all_cockroachdb_mutexes, count);

    count= array_elements(all_cockroachdb_stages);
    mysql_stage_register(category, all_cockroachdb_stages, count);
}
#endif


/*
   The keys are in form: {index_number} {mem-comparable-key}

   (todo: knowledge about this format is shared between this class and
   RDBSE_KEYDEF)
   */

class Primary_key_comparator : public cockroachdb::Comparator
{
    public:
        int Compare(const cockroachdb::Slice& a, const cockroachdb::Slice& b) const
        {
            size_t a_size= a.size();
            size_t b_size= b.size();
            size_t len= (a_size < b_size) ? a_size : b_size;
            int res;

            if ((res= memcmp(a.data(), b.data(), len)))
                return res;

            /* Ok, res== 0 */
            if (a_size != b_size)
            {
                return a_size < b_size? -1 : 1;
            }
            return 0;
        }

        /* The following is not needed by cockroachdb, but conceptually should be here: */
        static ulong get_hashnr(const char *key, size_t key_len);
        const char* Name() const { return "cockroachdb_SE_v2.2"; }

        //TODO: advanced funcs:
        // - FindShortestSeparator
        // - FindShortSuccessor
        // for now, do-nothing implementations:
        void FindShortestSeparator(std::string* start, const cockroachdb::Slice& limit) const {}
        void FindShortSuccessor(std::string* key) const {}
};

Primary_key_comparator primary_key_comparator;

int compare_mem_comparable_keys(const uchar *a, size_t a_len, const uchar *b, size_t b_len)
{
    cockroachdb::Slice a_slice((char*)a, a_len);
    cockroachdb::Slice b_slice((char*)b, b_len);
    return primary_key_comparator.Compare(a_slice, b_slice);
}

static int cockroachdb_close_connection(handlerton* hton, THD* thd)
{
    return 0;
}


static int cockroachdb_commit(handlerton* hton, THD* thd, bool commit_trx)
{
    return 0;
}


static int cockroachdb_rollback(handlerton* hton, THD* thd, bool rollback_trx)
{
    return 0;
}


static int cockroachdb_init_func(void *p)
{
    DBUG_ENTER("cockroachdb_init_func");

#ifdef HAVE_PSI_INTERFACE
    init_cockroachdb_psi_keys();
#endif

    cockroachdb_hton= (handlerton *)p;
    mysql_mutex_init(ex_key_mutex_example, &cockroachdb_mutex, MY_MUTEX_INIT_FAST);
    (void) my_hash_init(&cockroachdb_open_tables,system_charset_info,32,0,0,
            (my_hash_get_key) cockroachdb_get_key,0,0);

    cockroachdb_hton->state=    SHOW_OPTION_YES;
    cockroachdb_hton->create=   cockroachdb_create_handler;
    cockroachdb_hton->close_connection= cockroachdb_close_connection;
    cockroachdb_hton->commit=   cockroachdb_commit;
    cockroachdb_hton->rollback= cockroachdb_rollback;
    cockroachdb_hton->db_type=  DB_TYPE_EXAMPLE_DB;

    /*
       Don't specify HTON_CAN_RECREATE in flags. re-create is used by TRUNCATE
       TABLE to create an empty table from scratch. cockroachdb cannot efficiently
       re-create a table.
    */
    cockroachdb_hton->flags= HTON_TEMPORARY_NOT_SUPPORTED;

    /*
       As for the datadir, innobase_init() uses mysql_real_data_home for
       embedded server, and current directory for the "full server".
    */
    DBUG_ASSERT(!mysqld_embedded);

    sql_print_information("cockroachdb instance opened");
    DBUG_RETURN(0);
}


static int cockroachdb_done_func(void *p)
{
    DBUG_ENTER("cockroachdb_done_func");
    DBUG_RETURN(0);
}

/* Get PK value from table->record[0]. */
void ha_cockroachdb::update_auto_incr_val()
{
}

/**
  @brief
  Example of simple lock controls. The "share" it creates is a
  structure we will pass to each rocksdb handler. Do you have to have
  one of these? Well, you have pieces that are used for locking, and
  they are needed to function.
*/

static COCKROACHDB_SHARE *get_share(const char *table_name, TABLE *table)
{
  COCKROACHDB_SHARE *share;
  uint length;
  char *tmp_name;

  mysql_mutex_lock(&cockroachdb_mutex);
  length=(uint) strlen(table_name);

  if (!(share=(COCKROACHDB_SHARE*) my_hash_search(&cockroachdb_open_tables,
                                              (uchar*) table_name,
                                              length)))
  {
    if (!(share=(COCKROACHDB_SHARE *)
          my_multi_malloc(MYF(MY_WME | MY_ZEROFILL),
                          &share, sizeof(*share),
                          &tmp_name, length+1,
                          NullS)))
    {
      mysql_mutex_unlock(&cockroachdb_mutex);
      return NULL;
    }

    share->use_count=0;
    share->table_name_length=length;
    share->table_name=tmp_name;
    strmov(share->table_name,table_name);

    if (my_hash_insert(&cockroachdb_open_tables, (uchar*) share))
      goto error;
    thr_lock_init(&share->lock);
  }
  share->use_count++;
  mysql_mutex_unlock(&cockroachdb_mutex);

  return share;

error:
  my_free(share);

  return NULL;
}

/**
  @brief
  Free lock controls. We call this whenever we close a table. If the table had
  the last reference to the share, then we free memory associated with it.
  */

static int free_share(COCKROACHDB_SHARE *share)
{
    mysql_mutex_lock(&cockroachdb_mutex);
    if (!--share->use_count)
    {
        my_hash_delete(&cockroachdb_open_tables, (uchar*) share);
        thr_lock_delete(&share->lock);
        my_free(share);
    }
    mysql_mutex_unlock(&cockroachdb_mutex);

    return 0;
}


static handler* cockroachdb_create_handler(handlerton *hton,
        TABLE_SHARE *table,
        MEM_ROOT *mem_root)
{
    return new (mem_root) ha_cockroachdb(hton, table);
}


ha_cockroachdb::ha_cockroachdb(handlerton *hton, TABLE_SHARE *table_arg)
: handler(hton, table_arg),
    pk_can_be_decoded(false),
    pk_tuple(NULL), pk_packed_tuple(NULL),
    sec_key_packed_tuple(NULL), sec_key_tails(NULL),
    lock_rows(FALSE),
    keyread_only(FALSE)
{}


static const char *ha_cockroachdb_exts[] = {
    NullS
};


const char **ha_cockroachdb::bas_ext() const
{
    return ha_cockroachdb_exts;
}


/*
   Convert record from table->record[0] form into a form that can be written
   into cockroachdb.

   @param packed_rec OUT Data slice with record data.
   */

void ha_cockroachdb::convert_record_to_storage_format(cockroachdb::Slice *packed_rec)
{
    Field **field;

    for (field= table->field; *field; field++)
    {
        if ((*field)->real_type() == MYSQL_TYPE_VARCHAR)
        {
            Field_varstring* field_var= (Field_varstring*)*field;
            /* Fill unused bytes with zeros */
            uint used_size= field_var->length_bytes + (*field)->data_length();
            uint total_size= (*field)->pack_length();
            memset((*field)->ptr + used_size, 0, total_size - used_size);
        }
    }

    if (!table->s->blob_fields)
    {
        *packed_rec = cockroachdb::Slice((char*)table->record[0], table->s->reclength);
        return;
    }

    /* Ok have blob fields */
    storage_record.length(0);
    storage_record.append((const char*)table->record[0], table->s->reclength);

    // for each blob column
    for (field= table->field; *field; field++)
    {
        if ((*field)->type() == MYSQL_TYPE_BLOB)
        {
            Field_blob *blob= (Field_blob*)(*field);
            uint32 value_len= blob->get_length();
            uint length_bytes= blob->pack_length() - 8;
            char *data_ptr;
            memcpy(&data_ptr, blob->ptr + length_bytes, sizeof(void*));

            storage_record.append(data_ptr, value_len);
            uint32 size_to_write= htons(value_len);
            size_t pointer_offset= (blob->ptr - table->record[0]) + length_bytes;
            memcpy((char*)storage_record.ptr() + pointer_offset, &size_to_write,
                    sizeof(uint32));
        }
    }
    *packed_rec= cockroachdb::Slice(storage_record.ptr(), storage_record.length());
}


void ha_cockroachdb::convert_record_from_storage_format(cockroachdb::Slice *slice,
        uchar *buf)
{
    if (!table->s->blob_fields)
    {
        DBUG_ASSERT(slice->size() == table->s->reclength);
        memcpy(buf, slice->data(), slice->size());
    }
    else
    {
        retrieved_record.assign(slice->data(), slice->size());
        convert_record_from_storage_format(buf);
    }
}

/*
   Unpack the record in this->retrieved_record from storage format into
   buf (which can be table->record[0] or table->record[1])

   If the table has blobs, the unpacked data in buf may keep pointers to the
   data in this->retrieved_record.
   */

void ha_cockroachdb::convert_record_from_storage_format(uchar * buf)
{
    if (!table->s->blob_fields)
    {
        DBUG_ASSERT(retrieved_record.length() == table->s->reclength);
        memcpy(buf, retrieved_record.c_str(), retrieved_record.length());
        return;
    }
    else
        unpack_blobs_from_retrieved_record(buf);
}


void ha_cockroachdb::unpack_blobs_from_retrieved_record(uchar *buf)
{
    /*
       Unpack the blobs
       Blobs in the record are stored as
       [record-0 format] [blob data#1] [blob data#2]
       */
    memcpy(buf, retrieved_record.c_str(), table->s->reclength);

    const char *blob_ptr= retrieved_record.c_str() + table->s->reclength;

    // for each blob column
    for (Field **field= table->field; *field; field++)
    {
        if ((*field)->type() == MYSQL_TYPE_BLOB)
        {
            Field_blob *blob= (Field_blob*)(*field);
            my_ptrdiff_t ptr_diff= buf - table->record[0];
            blob->move_field_offset(ptr_diff);
            /*
               We've got the blob length when we've memcpy'ed table->record[0].
               But there's still offset instead of blob pointer.
               */
            uint32 value_len= blob->get_length();
            uint length_bytes= blob->pack_length() - 8;

            // set 8-byte pointer to 0, like innodb does.
            memset(blob->ptr + length_bytes, 0, 8);

            memcpy(blob->ptr + length_bytes, &blob_ptr, sizeof(void*));

            blob_ptr += value_len;
            blob->move_field_offset(-ptr_diff);
        }
    }
}

int ha_cockroachdb::open(const char *name, int mode, uint test_if_locked)
{
    DBUG_ENTER("ha_cockroachdb::open");
    if (!(share = get_share(name, table)))
        DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
    thr_lock_data_init(&share->lock, &lock, NULL);

    DBUG_RETURN(0);
}


int ha_cockroachdb::close(void)
{
    DBUG_ENTER("ha_cockroachdb::close");
    DBUG_RETURN(free_share(share));
}


const int HA_ERR_cockroachdb_UNIQUE_NOT_SUPPORTED= HA_ERR_LAST+1;
const int HA_ERR_cockroachdb_PK_REQUIRED= HA_ERR_LAST+2;
const int HA_ERR_cockroachdb_TOO_MANY_LOCKS= HA_ERR_LAST+3;

bool ha_cockroachdb::get_error_message(int error, String *buf)
{
    if (error == HA_ERR_cockroachdb_PK_REQUIRED)
        buf->append("Table must have a PRIMARY KEY");
    else if (error == HA_ERR_cockroachdb_UNIQUE_NOT_SUPPORTED)
        buf->append("Unique indexes are not supported");
    else if (error == HA_ERR_cockroachdb_TOO_MANY_LOCKS)
        buf->append("Number of locks held reached @@cockroachdb_max_row_locks");
    return FALSE; /* not a temporary error */
}


/*
   Create structures needed for storing data in cockroachdb. This is called when the
   table is created. The structures will be shared by all TABLE* objects.

   @param
   table_arg  Table with definition
   db_table   "dbname.tablename"
   len        strlen of the above

   @return
   0      - Ok
   other  - error, either given table ddl is not supported by cockroachdb or OOM.
   */

int ha_cockroachdb::create_key_defs(TABLE *table_arg, const char *db_table, uint len)
{
    DBUG_ENTER("ha_cockroachdb::create_key_defs");
    DBUG_RETURN(0);
}


static int cockroachdb_normalize_tablename(const char *tablename,
        StringBuffer<256> *strbuf)
{
    DBUG_ASSERT(tablename[0] == '.' && tablename[1] == '/');
    tablename += 2;

    const char *p= tablename;
    for (; *p != '/'; p++)
    {
        if (*p =='\0')
        {
            DBUG_ASSERT(0); // We were not passed table name?
            return HA_ERR_INTERNAL_ERROR ;
        }
    }
    strbuf->append(tablename, p - tablename);
    strbuf->append('.');
    strbuf->append(p + 1);
    return 0;
}


/**
  @brief
  create() is called to create a database. The variable name will have the name
  of the table.

  @details
  When create() is called you do not need to worry about
  opening the table. Also, the .frm file will have already been
  created so adjusting create_info is not necessary. You can overwrite
  the .frm file at this point if you wish to change the table
  definition, but there are no methods currently provided for doing
  so.

  Called from handle.cc by ha_create_table().

  @see
  ha_create_table() in handle.cc
  */

int ha_cockroachdb::create(const char *name, TABLE *table_arg,
        HA_CREATE_INFO *create_info)
{
    int res;
    StringBuffer<256> strbuf;
    DBUG_ENTER("ha_cockroachdb::create");

    /*
       Construct dbname.tablename ourselves, because parititioning
       passes strings like "./test/t14#P#p0" for individual partitions,
       while table_arg->s->table_name has none of that.
       */
    if (cockroachdb_normalize_tablename(name, &strbuf))
        DBUG_RETURN(HA_ERR_INTERNAL_ERROR);

    /*
       We have HA_REQUIRE_PRIMARY_KEY flag, which guarantees there will be a
       primary key.
       But there is an exception: the query

       ALTER TABLE ... DROP COLUMN primary_key_column

       will attempt to create a table without primary key.
       */
    if (table_arg->s->primary_key == MAX_INDEXES)
    {
        DBUG_RETURN(HA_ERR_cockroachdb_PK_REQUIRED);
    }

    if ((res= create_key_defs(table_arg, strbuf.ptr(), strbuf.length())))
        DBUG_RETURN(res);

    /*
       We only support auto_increment at start of the PRIMARY KEY.
       */
    //Field *field;
    //if ((field= table_arg->next_number_field))
    /* TODO mdcallag: disable this for now to let UNIQUE indexes kind of work
       if ((field= table_arg->found_next_number_field))
       {
       int pk= table_arg->s->primary_key;
       Field *pk_field= table_arg->key_info[pk].key_part[0].field;
       if (field->field_index !=  pk_field->field_index)
       DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
       }
       */
    DBUG_RETURN(0);
}


/*
   This function is used only when the table has not yet been opened, and
   keyread_allowed bitmap doesn't have the correct values yet.

   See comment it ha_cockroachdb::index_flags() for details.
   */

bool ha_cockroachdb::check_keyread_allowed(uint inx, uint part, bool all_parts) const
{
    return true;
}


/*
   @note
   The problem with this function is that SQL layer calls it, when
   - the table has not been yet opened (no ::open() call done)
   - this->table_share already exists, but it is in the process of being
   filled, so some of fields are still NULL.
   - In particular, table_share->key_info[inx].key_part[] is filled only up
   to part #part. Subsequent key parts are not yet filled.

   To complicate things further, SQL layer will call index_flags() with
   all_parts=TRUE. Essentially, we're asked to provide flags for reading
   keyparts whose datatype is not yet known.

   We walk around this problem by using check_keyread_allowed(), which uses
   table_share object and is careful not to step on unitialized data.

   When we get a call with all_parts=TRUE, we try to analyze all parts but
   ignore those that have key_part->field==NULL (these are not initialized
   yet).
   */

ulong ha_cockroachdb::index_flags(uint inx, uint part, bool all_parts) const
{
    ulong base_flags= HA_READ_NEXT  |   // doesn't seem to be used
        HA_READ_ORDER |
        HA_READ_RANGE |
        HA_READ_PREV;

    return base_flags;
}


/*
   Read next index tuple through the secondary index.
   */

int ha_cockroachdb::secondary_index_read(int keyno, uchar *buf)
{
    return 0;
}


//void store_key_image_to_rec(Field *field, uchar *ptr, uint len);

int ha_cockroachdb::index_read_map(uchar *buf, const uchar *key,
        key_part_map keypart_map,
        enum ha_rkey_function find_flag)
{
    int rc;
    DBUG_ENTER("ha_cockroach::index_read");
    rc= HA_ERR_WRONG_COMMAND;
    DBUG_RETURN(rc);
}


int ha_cockroachdb::index_read_last_map(uchar * buf, const uchar * key,
        key_part_map keypart_map)
{
    return index_read_map(buf, key, keypart_map, HA_READ_PREFIX_LAST);
}

int ha_cockroachdb::get_row_by_rowid(uchar *buf, const char *rowid, uint rowid_size)
{
    DBUG_ENTER("ha_cockroachdb::get_row_by_rowid");
    DBUG_RETURN(0);
}


int ha_cockroachdb::index_next(uchar *buf)
{
    return HA_ERR_WRONG_COMMAND;
}


int ha_cockroachdb::index_prev(uchar *buf)
{
    return HA_ERR_WRONG_COMMAND;
}


int ha_cockroachdb::index_next_with_direction(uchar *buf, bool move_forward)
{
    DBUG_ENTER("ha_cockroachdb::index_next");
    DBUG_RETURN(0);
}


int ha_cockroachdb::index_first(uchar *buf)
{
    DBUG_ENTER("ha_cockroachdb::index_first");
    DBUG_RETURN(0);
}


int ha_cockroachdb::index_last(uchar *buf)
{
    DBUG_ENTER("ha_cockroachdb::index_first");
    DBUG_RETURN(0);
}


void ha_cockroachdb::unlock_row()
{
}


//static const char* empty_str="";

int ha_cockroachdb::write_row(uchar *buf)
{
    DBUG_ENTER("ha_cockroachdb::index_first");
    DBUG_RETURN(0);
}


/*
   Open a cursor and position it at the passed record
   */

void ha_cockroachdb::setup_index_scan(cockroachdb::Slice *slice)
{
}


int ha_cockroachdb::rnd_init(bool scan)
{
    DBUG_ENTER("ha_cockroachdb::rnd_init");
    DBUG_RETURN(0);
}


int ha_cockroachdb::rnd_next(uchar *buf)
{
    return rnd_next_with_direction(buf, true);
}


/*
   See also secondary_index_read().
   */
int ha_cockroachdb::rnd_next_with_direction(uchar *buf, bool move_forward)
{
    int rc = 0;
    DBUG_ENTER("ha_cockroachdb::rnd_next");
    rc = HA_ERR_END_OF_FILE;
    table->status = STATUS_NOT_FOUND;
    DBUG_RETURN(rc);
}


int ha_cockroachdb::rnd_end()
{
    DBUG_ENTER("ha_cockroachdb::rnd_end");
    DBUG_RETURN(0);
}


int ha_cockroachdb::index_end()
{
    return 0;
}


int ha_cockroachdb::delete_all_rows()
{
    DBUG_ENTER("ha_cockroachdb::delete_all_rows");
    /*
       We will have to delete records one-by-one anyway. Let the SQL layer do
       it.
       */
    DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

int ha_cockroachdb::truncate()
{
    return 0;
}

/*
   Delete the row we've last read. Ignore the parameter.
   (Check out InnoDB: row_update_for_mysql() has "UT_NOT_USED(mysql_rec)")
   */
int ha_cockroachdb::delete_row(const uchar *buf)
{
    DBUG_ENTER("ha_cockroachdb::delete_row");
    DBUG_RETURN(0);
}


int ha_cockroachdb::info(uint flag)
{
    DBUG_ENTER("ha_cockroachdb::info");
    DBUG_RETURN(0);
}


void ha_cockroachdb::position(const uchar *record)
{
    DBUG_ENTER("ha_cockroachdb::position");
    DBUG_VOID_RETURN;
}


int ha_cockroachdb::rnd_pos(uchar *buf, uchar *pos)
{
    DBUG_ENTER("ha_cockroachdb::rnd_pos");
    DBUG_RETURN(0);
}


int ha_cockroachdb::return_lock_failure(bool timed_out)
{
    return HA_ERR_cockroachdb_TOO_MANY_LOCKS;
}


int ha_cockroachdb::update_row(const uchar *old_data, uchar *new_data)
{
    DBUG_ENTER("ha_cockroachdb::update_row");
    DBUG_RETURN(0);
}


/* The following function was copied from ha_blackhole::store_lock: */
THR_LOCK_DATA **ha_cockroachdb::store_lock(THD *thd,
        THR_LOCK_DATA **to,
        enum thr_lock_type lock_type)
{
    DBUG_ENTER("ha_cockroachdb::store_lock");
    if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK)
    {
        /*
           Here is where we get into the guts of a row level lock.
           If TL_UNLOCK is set
           If we are not doing a LOCK TABLE or DISCARD/IMPORT
           TABLESPACE, then allow multiple writers
           */

        if ((lock_type >= TL_WRITE_CONCURRENT_INSERT &&
                    lock_type <= TL_WRITE) && !thd_in_lock_tables(thd)
                && !thd_tablespace_op(thd))
            lock_type = TL_WRITE_ALLOW_WRITE;

        /*
           In queries of type INSERT INTO t1 SELECT ... FROM t2 ...
           MySQL would use the lock TL_READ_NO_INSERT on t2, and that
           would conflict with TL_WRITE_ALLOW_WRITE, blocking all inserts
           to t2. Convert the lock to a normal read lock to allow
           concurrent inserts to t2.
           */

        if (lock_type == TL_READ_NO_INSERT && !thd_in_lock_tables(thd))
            lock_type = TL_READ;

        lock.type= lock_type;
    }
    *to++= &lock;
    DBUG_RETURN(to);
}


int ha_cockroachdb::external_lock(THD *thd, int lock_type)
{
    int res= 0;
    DBUG_ENTER("ha_cockroachdb::external_lock");
    if (lock_type == F_UNLCK)
    {
        if (!thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))
        {
            /*
               Do like InnoDB: when we get here, it's time to commit a
               single-statement transaction.

               If the statement involved multiple tables, this code will be executed
               for each of them, but that's ok because non-first trx->commit() calls
               will be no-ops.
               */
        }
        lock_rows= FALSE;
    }
    else
    {
        /*
           It's nice to do the following on start of every statement. The problem
           is, handler->start_stmt() is not called for INSERTs.
           So, we put this code here.
           */
        if (lock_type == F_WRLCK)
        {
            /*
               SQL layer signals us to take a write lock. It does so when starting DML
               statement. We should put locks on the rows we're reading.
               */
            lock_rows= TRUE;
        }

        trans_register_ha(thd, FALSE, cockroachdb_hton);
        if (thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))
        {
            trans_register_ha(thd, TRUE, cockroachdb_hton);
        }
    }

    DBUG_RETURN(res);
}


int ha_cockroachdb::start_stmt(THD *thd, thr_lock_type lock_type)
{
    return 0;
}

/*
Note: the following function is called when the table is not open. That is,
this->table==NULL, pk_key_descr==NULL, etc.

tablename points to line in form "./dbname/tablename".
*/

int ha_cockroachdb::delete_table(const char *tablename)
{
    DBUG_ENTER("ha_cockroachdb::delete_table");
    DBUG_RETURN(0);
}


void ha_cockroachdb::remove_rows(RDBSE_TABLE_DEF *tbl)
{
}


int ha_cockroachdb::rename_table(const char *from, const char *to)
{
    return 0;
}

/**
  check_if_incompatible_data() called if ALTER TABLE can't detect otherwise
  if new and old definition are compatible

  @details If there are no other explicit signs like changed number of
  fields this function will be called by compare_tables()
  (sql/sql_tables.cc) to decide should we rewrite whole table or only .frm
  file.

*/

bool ha_cockroachdb::check_if_incompatible_data(HA_CREATE_INFO *info,
        uint table_changes)
{
    DBUG_ENTER("ha_cockroachdb::check_if_incompatible_data");
    // this function is needed only for online alter-table
    DBUG_RETURN(COMPATIBLE_DATA_NO);
}


int ha_cockroachdb::extra(enum ha_extra_function operation)
{
    DBUG_ENTER("ha_cockroachdb::extra");
    DBUG_RETURN(0);
}


ha_rows ha_cockroachdb::records_in_range(uint inx, key_range *min_key,
        key_range *max_key)
{
    DBUG_ENTER("ha_cockroachdb::records_in_range");
    DBUG_RETURN(10);                         // low number to force index usage
}


void ha_cockroachdb::update_create_info(HA_CREATE_INFO *create_info)
{
}


void ha_cockroachdb::get_auto_increment(ulonglong offset, ulonglong increment,
        ulonglong nb_desired_values,
        ulonglong *first_value,
        ulonglong *nb_reserved_values)
{
}

struct st_mysql_storage_engine cockroachdb_storage_engine=
{ MYSQL_HANDLERTON_INTERFACE_VERSION };

static struct st_mysql_show_var func_status[]=
{
    // {"Cassandra",  (char *)show_cockroachdb_vars, SHOW_FUNC},
    {0,0,SHOW_UNDEF}
};

mysql_declare_plugin(cockroachdb_se)
{
    MYSQL_STORAGE_ENGINE_PLUGIN,
    &cockroachdb_storage_engine,
    "cockroachdb",
    "pingcap.com",
    "cockroachdb storage engine",
    PLUGIN_LICENSE_GPL,
    cockroachdb_init_func,                            /* Plugin Init */
    cockroachdb_done_func,                            /* Plugin Deinit */
    0x0001,                                       /* version number (0.1) */
    func_status,                                  /* status variables */
    cockroachdb_system_variables,                     /* system variables */
    NULL,                                         /* config options */
    0,                                            /* flags */
}
mysql_declare_plugin_end;


/*
   Compute a hash number for a PK value in RowKeyFormat.

   @note
   RowKeyFormat is comparable with memcmp. This means, any hash function will
   work correctly. We use my_charset_bin's hash function.

   Note from Bar: could also use crc32 function.
   */

ulong Primary_key_comparator::get_hashnr(const char *key, size_t key_len)
{
    ulong nr=1, nr2=4;
    my_charset_bin.coll->hash_sort(&my_charset_bin, (const uchar*)key, key_len,
            &nr, &nr2);
    return((ulong) nr);
}

