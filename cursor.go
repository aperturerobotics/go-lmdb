package golmdb

/*
#include <stdlib.h>
#include <memory.h>
#include <lmdb.h>
#include "golmdb.h"
*/
import "C"
import "unsafe"

// Cursors allow you to walk over a database, or sections of them.
type ReadOnlyCursor struct {
	cursor *C.MDB_cursor
}

// A ReadWriteCursor extends ReadOnlyCursor with methods for mutating
// the database.
type ReadWriteCursor struct {
	ReadOnlyCursor
}

// Create a new read-only cursor.
//
// You should call Close() on each cursor before the end of the
// transaction.  The exact rules for cursor lifespans are more
// complex, and are documented at
// http://www.lmdb.tech/doc/group__mdb.html#ga9ff5d7bd42557fd5ee235dc1d62613aa
// but it's simplest if you treat each cursor as scoped to the
// lifespan of its transaction, and you explicitly Close() each cursor
// before the end of the transaction.
//
// See http://www.lmdb.tech/doc/group__mdb.html#ga9ff5d7bd42557fd5ee235dc1d62613aa
func (self *ReadOnlyTxn) NewCursor(db DBRef) (*ReadOnlyCursor, error) {
	var cursor *C.MDB_cursor
	err := asError(C.mdb_cursor_open(self.txn, C.MDB_dbi(db), &cursor))
	if err != nil {
		return nil, err
	}
	return &ReadOnlyCursor{cursor: cursor}, nil
}

// Create a new read-write cursor.
//
// You should call Close() on each cursor before the end of the
// transaction.  The exact rules for cursor lifespans are more
// complex, and are documented at
// http://www.lmdb.tech/doc/group__mdb.html#ga9ff5d7bd42557fd5ee235dc1d62613aa
// but it's simplest if you treat each cursor as scoped to the
// lifespan of its transaction, and you explicitly Close() each cursor
// before the end of the transaction.
//
// See http://www.lmdb.tech/doc/group__mdb.html#ga9ff5d7bd42557fd5ee235dc1d62613aa
func (self *ReadWriteTxn) NewCursor(db DBRef) (*ReadWriteCursor, error) {
	var cursor *C.MDB_cursor
	err := asError(C.mdb_cursor_open(self.txn, C.MDB_dbi(db), &cursor))
	if err != nil {
		return nil, err
	}
	return &ReadWriteCursor{ReadOnlyCursor{cursor: cursor}}, nil
}

// Close the current cursor.
func (self *ReadOnlyCursor) Close() {
	C.mdb_cursor_close(self.cursor)
	self.cursor = nil
}

// Position the cursor, and fetch the value and sometimes key at the
// cursor's new position. Whether or not the key and val parameters
// are needed depends on the cursor operation specified. When not
// needed, it's best to provide nil.
//
// The returned key and value, if non nil, are bytes that are owned by
// the database. Do not modify them. They are valid only until a
// subsequent update operation, or the end of the transaction. If you
// need the key or value around longer than that, you must take a
// copy.
//
// See http://www.lmdb.tech/doc/group__mdb.html#ga48df35fb102536b32dfbb801a47b4cb0
func (self *ReadOnlyCursor) MoveAndGet(move CursorOp, key, val []byte) (keyAtCursor, valAtCursor []byte, err error) {
	var keyVal, valVal value

	switch {
	case len(key) == 0:
		// no key, so we must be doing something like GetCurrent, First, Last, Next.
		err = asError(C.mdb_cursor_get(self.cursor, (*C.MDB_val)(&keyVal), (*C.MDB_val)(&valVal), C.MDB_cursor_op(move)))

	case len(val) == 0:
		// key is given though. So key must be being used; something like Set, SetRange.
		err = asError(C.golmdb_mdb_cursor_get1(self.cursor,
			(*C.char)(unsafe.Pointer(&key[0])), C.size_t(len(key)),
			(*C.MDB_val)(&keyVal), (*C.MDB_val)(&valVal), C.MDB_cursor_op(move)))

	default:
		// both key and value given. Something like GetBoth, GetBothRange.
		err = asError(C.golmdb_mdb_cursor_get2(self.cursor,
			(*C.char)(unsafe.Pointer(&key[0])), C.size_t(len(key)),
			(*C.char)(unsafe.Pointer(&val[0])), C.size_t(len(val)),
			(*C.MDB_val)(&keyVal), (*C.MDB_val)(&valVal), C.MDB_cursor_op(move)))
	}

	if err != nil {
		return nil, nil, err
	}

	return keyVal.bytesNoCopy(), valVal.bytesNoCopy(), nil
}

// Put a key-value pair into the database.
//
// There's not a lot of good reason for this to exist. Using the
// Current flag still requires the key to be specified. If using the
// Current flag, and the length of val happens to be the same as the
// length of the existing value then this will do less work than a
// normal ReadWriteTxn.Delete followed by ReadWriteTxn.Put. But that's
// about it.
//
// If no error occurs, the cursor is positioned at the new key-value
// pair.
//
// See http://www.lmdb.tech/doc/group__mdb.html#ga1f83ccb40011837ff37cc32be01ad91e
func (self *ReadWriteCursor) Put(key, val []byte, flags PutFlag) error {
	return asError(C.golmdb_mdb_cursor_put(
		self.cursor,
		(*C.char)(unsafe.Pointer(&key[0])), C.size_t(len(key)),
		(*C.char)(unsafe.Pointer(&val[0])), C.size_t(len(val)),
		C.uint(flags)))
}

// Delete the key-value pair at the cursor.
//
// The only possible flag is NoDupData which is only for DupSort
// databases, and means "delete all values for the current key".
//
// See http://www.lmdb.tech/doc/group__mdb.html#ga26a52d3efcfd72e5bf6bd6960bf75f95
func (self *ReadWriteCursor) Delete(flags PutFlag) error {
	return asError(C.mdb_cursor_del(self.cursor, C.uint(flags)))
}
