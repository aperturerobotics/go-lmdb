package golmdb

/*
#include <stdlib.h>
#include <memory.h>
#include <lmdb.h>
#include "golmdb.h"
*/
import "C"
import (
	"unsafe"
)

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

func (self *ReadOnlyCursor) moveAndGet0(op cursorOp) (key, val []byte, err error) {
	var keyVal, valVal value
	err = asError(C.mdb_cursor_get(self.cursor, (*C.MDB_val)(&keyVal), (*C.MDB_val)(&valVal), C.MDB_cursor_op(op)))
	if err != nil {
		return nil, nil, err
	}

	return keyVal.bytesNoCopy(), valVal.bytesNoCopy(), nil
}

func (self *ReadOnlyCursor) moveAndGet1(op cursorOp, keyIn []byte) (key, val []byte, err error) {
	var keyVal, valVal value
	err = asError(C.golmdb_mdb_cursor_get1(self.cursor,
		(*C.char)(unsafe.Pointer(&keyIn[0])), C.size_t(len(keyIn)),
		(*C.MDB_val)(&keyVal), (*C.MDB_val)(&valVal), C.MDB_cursor_op(op)))
	if err != nil {
		return nil, nil, err
	}

	return keyVal.bytesNoCopy(), valVal.bytesNoCopy(), nil
}

func (self *ReadOnlyCursor) moveAndGet2(op cursorOp, keyIn, valIn []byte) (val []byte, err error) {
	var valVal value
	err = asError(C.golmdb_mdb_cursor_get2(self.cursor,
		(*C.char)(unsafe.Pointer(&keyIn[0])), C.size_t(len(keyIn)),
		(*C.char)(unsafe.Pointer(&valIn[0])), C.size_t(len(valIn)),
		(*C.MDB_val)(&valVal), C.MDB_cursor_op(op)))

	if err != nil {
		return nil, err
	}

	return valVal.bytesNoCopy(), nil
}

// Move to the first key-value pair of the database.
func (self *ReadOnlyCursor) First() (key, val []byte, err error) {
	return self.moveAndGet0(first)
}

// Only for DupSort. Move to the first key-value pair without changing
// the current key.
func (self *ReadOnlyCursor) FirstInSameKey() (val []byte, err error) {
	_, val, err = self.moveAndGet0(firstDup)
	return val, err
}

// Move to the last key-value pair of the database.
func (self *ReadOnlyCursor) Last() (key, val []byte, err error) {
	return self.moveAndGet0(last)
}

// Only for DupSort. Move to the last key-value pair without changing
// the current key.
func (self *ReadOnlyCursor) LastInSameKey() (val []byte, err error) {
	_, val, err = self.moveAndGet0(lastDup)
	return val, err
}

// Get the current key-value pair of the cursor.
func (self *ReadOnlyCursor) Current() (key, val []byte, err error) {
	return self.moveAndGet0(getCurrent)
}

// Move to the next key-value pair.
//
// For DupSort databases, move to the next value of the current
// key, if there is one, otherwise the first value of the next
// key.
func (self *ReadOnlyCursor) Next() (key, val []byte, err error) {
	return self.moveAndGet0(next)
}

// Only for DupSort. Move to the next key-value pair, but only if the
// key is the same as the current key.
func (self *ReadOnlyCursor) NextInSameKey() (key, val []byte, err error) {
	return self.moveAndGet0(nextDup)
}

// Only for DupSort. Move to the first key-value pair of the next key.
func (self *ReadOnlyCursor) NextKey() (key, val []byte, err error) {
	return self.moveAndGet0(nextNoDup)
}

// Move to the previous key-value pair.
//
// For DupSort databases, move to the previous value of the current
// key, if there is one, otherwise the last value of the previous
// key.
func (self *ReadOnlyCursor) Prev() (key, val []byte, err error) {
	return self.moveAndGet0(prev)
}

// Only for DupSort. Move to the previous key-value pair, but only if
// the key is the same as the current key.
func (self *ReadOnlyCursor) PrevInSameKey() (key, val []byte, err error) {
	return self.moveAndGet0(prevDup)
}

// Only for DupSort. Move to the last key-value pair of the previous
// key.
func (self *ReadOnlyCursor) PrevKey() (key, val []byte, err error) {
	return self.moveAndGet0(prevNoDup)
}

// Move to the key-value pair indicated by the given key.
//
// If the exact key doesn't exist, return NotFound.
//
// For DupSort databases, move to the first value of the given key.
func (self *ReadOnlyCursor) SeekExactKey(key []byte) (val []byte, err error) {
	_, val, err = self.moveAndGet1(setKey, key)
	return val, err
}

// Move to the key-value pair indicated by the given key.
//
// If the exact key doesn't exist, move to the nearest key greater
// than the given key.
func (self *ReadOnlyCursor) SeekGreaterThanOrEqualKey(keyIn []byte) (keyOut, val []byte, err error) {
	return self.moveAndGet1(setRange, keyIn)
}

// Only for DupSort. Move to the key-value pair indicated.
//
// If the exact key-value pair doesn't exist, return NotFound.
func (self *ReadOnlyCursor) SeekExactKeyAndValue(keyIn, valIn []byte) (err error) {
	_, err = self.moveAndGet2(getBoth, keyIn, valIn)
	return err
}

// Only for DupSort. Move to the key-value pair indicated.
//
// If the exact key-value pair doesn't exist, move to the nearest
// value in the same key greater than the given value. I.e. this will
// not move to a greater key, only a greater value.
//
// If there is no such value within the current key, return NotFound.
func (self *ReadOnlyCursor) SeekGreaterThanOrEqualKeyAndValue(keyIn, valIn []byte) (valOut []byte, err error) {
	return self.moveAndGet2(getBothRange, keyIn, valIn)
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
