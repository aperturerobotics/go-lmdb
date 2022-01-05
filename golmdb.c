/* golmdb.c
 *
 * This code was originally written by Bryan Matsuo, at
 * https://github.com/bmatsuo/lmdb-go/blob/master/lmdb/lmdbgo.c and
 * licensed under BSD 3-clause.
 *
 * I have made some small naming changes to it to make it consistent
 * with this package. And I have removed some functionality that I
 * don't need. But I have added nothing of value to this, and so I
 * consider the copyright of this code to belong to Bryan Matsuo.
 */
#include <lmdb.h>
#include "golmdb.h"

#define GOLMDB_SET_VAL(val, size, data) \
    *(val) = (MDB_val){.mv_size = (size), .mv_data = (data)}

int golmdb_mdb_get(MDB_txn *txn, MDB_dbi dbi, char *kdata, size_t kn, MDB_val *val) {
    MDB_val key;
    GOLMDB_SET_VAL(&key, kn, kdata);
    return mdb_get(txn, dbi, &key, val);
}

int golmdb_mdb_put(MDB_txn *txn, MDB_dbi dbi, char *kdata, size_t kn, char *vdata, size_t vn, unsigned int flags) {
    MDB_val key, val;
    GOLMDB_SET_VAL(&key, kn, kdata);
    GOLMDB_SET_VAL(&val, vn, vdata);
    return mdb_put(txn, dbi, &key, &val, flags);
}

int golmdb_mdb_del(MDB_txn *txn, MDB_dbi dbi, char *kdata, size_t kn, char *vdata, size_t vn) {
    MDB_val key, val;
    GOLMDB_SET_VAL(&key, kn, kdata);
    GOLMDB_SET_VAL(&val, vn, vdata);
    return mdb_del(txn, dbi, &key, &val);
}

int golmdb_mdb_cursor_get1(MDB_cursor *cur, char *kdata, size_t kn, MDB_val *key, MDB_val *val, MDB_cursor_op op) {
    GOLMDB_SET_VAL(key, kn, kdata);
    return mdb_cursor_get(cur, key, val, op);
}

int golmdb_mdb_cursor_get2(MDB_cursor *cur, char *kdata, size_t kn, char *vdata, size_t vn, MDB_val *key, MDB_val *val, MDB_cursor_op op) {
    GOLMDB_SET_VAL(key, kn, kdata);
    GOLMDB_SET_VAL(val, vn, vdata);
    return mdb_cursor_get(cur, key, val, op);
}

int golmdb_mdb_cursor_put(MDB_cursor *cur, char *kdata, size_t kn, char *vdata, size_t vn, unsigned int flags) {
    MDB_val key, val;
    GOLMDB_SET_VAL(&key, kn, kdata);
    GOLMDB_SET_VAL(&val, vn, vdata);
    return mdb_cursor_put(cur, &key, &val, flags);
}
