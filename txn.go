package golmdb

/*
#include <lmdb.h>
#include <stdlib.h>
#include <memory.h>
*/
import "C"
import (
	"unsafe"
)

// A single LMDB database can contain several top-level named
// "databases". These can be created and accessed by using the DBRef()
// method on ReadOnlyTxn and ReadWriteTxn. The DBRef is a reference to
// such a named top-level "database". They cannot be nested further,
// and you ideally only want to use a handful of these.
//
// See
// http://www.lmdb.tech/doc/group__mdb.html#gac08cad5b096925642ca359a6d6f0562a
type DBRef C.MDB_dbi

type value C.MDB_val

// this is for getting a Go-slice from memory owned by C. Go will not
// try and garbage collect it as it's memory owned by C.
func (self *value) bytesNoCopy() []byte {
	return unsafe.Slice((*byte)(self.mv_data), self.mv_size)
}

func (self *value) free() {
	if self == nil {
		return
	}
	C.free(unsafe.Pointer(self))
}

// this is for sending data to C-lmdb. This is only safe if the C side
// does not hold on to the data indefinitely. Read
// https://pkg.go.dev/cmd/cgo#hdr-Passing_pointers a lot, and
// carefully!
func asValue(data []byte) *value {
	if data == nil {
		return nil
	}
	dataLen := C.size_t(len(data))
	ptr := C.malloc(C.sizeof_MDB_val + dataLen)
	val := (*C.MDB_val)(ptr)
	val.mv_size = dataLen

	if len(data) > 0 {
		ptr = unsafe.Add(ptr, C.sizeof_MDB_val)
		C.memcpy(ptr, unsafe.Pointer(&data[0]), dataLen)
		val.mv_data = ptr
	}

	return (*value)(val)
}

type ReadOnlyTxn struct {
	txn *C.MDB_txn
}

// A ReadWriteTxn extends ReadOnlyTxn with methods for mutating the
// database.
type ReadWriteTxn struct {
	ReadOnlyTxn
}

// DBRef gets a reference to a named database within the LMDB. If you
// provide the flag Create then it'll be created if it doesn't already
// exist (provided you're in an Update transaction).
//
// If you call this from an Update and it succeeds, then once that txn
// commits, the DBRef can be used by other transactions (both Updates
// and Views) until it is terminated/closed.
//
// If you call this from a View and it succeeds, then the DBRef is
// only valid until the end of that View transaction.
//
// See
// http://www.lmdb.tech/doc/group__mdb.html#gac08cad5b096925642ca359a6d6f0562a
func (self *ReadOnlyTxn) DBRef(name string, flags DatabaseFlag) (DBRef, error) {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	var dbRef C.MDB_dbi
	err := asError(C.mdb_dbi_open(self.txn, cName, C.uint(flags), &dbRef))
	if err != nil {
		return 0, err
	}
	return DBRef(dbRef), nil
}

// Get the value corresponding to the key from the database.
//
// The returned bytes are owned by the database. Do not modify
// them. They are valid only until a subsequent update operation, or
// the end of the transaction. If you need the value around longer
// than that, you must take a copy.
//
// See
// http://www.lmdb.tech/doc/group__mdb.html#ga8bf10cd91d3f3a83a34d04ce6b07992d
func (self *ReadOnlyTxn) Get(db DBRef, key []byte) ([]byte, error) {
	keyVal := asValue(key)
	var data value
	err := asError(C.mdb_get(self.txn, C.MDB_dbi(db), (*C.MDB_val)(keyVal), (*C.MDB_val)(&data)))
	keyVal.free()
	if err != nil {
		return nil, err
	}
	return data.bytesNoCopy(), nil
}

// Put a key-value pair into the database.
//
// Internally, this uses the Reserve flag which avoids one memcpy of
// the val. But, this is illegal if the DBRef was opened with the
// DupSort flag. So if you're using DupSort then you must call
// PutDupSort instead.
//
// See
// http://www.lmdb.tech/doc/group__mdb.html#ga4fa8573d9236d54687c61827ebf8cac0
func (self *ReadWriteTxn) Put(db DBRef, key, val []byte, flags PutFlag) error {
	keyVal := asValue(key)

	valSize := C.size_t(len(val))
	valVal := &C.MDB_val{
		mv_size: valSize,
	}

	err := asError(C.mdb_put(self.txn, C.MDB_dbi(db), (*C.MDB_val)(keyVal), valVal, C.uint(flags|Reserve)))
	if err == nil && valSize > 0 {
		C.memcpy(valVal.mv_data, unsafe.Pointer(&val[0]), valSize)
	}

	keyVal.free()
	return err
}

// PutDupSort a key-value pair into the database.
//
// This is slower than calling Put as there's one extra memcpy going
// on, but it's always safe: even if you've created the DBRef with the
// DupSort flag (unlike Put). Internally, this clears the Reserve
// flag, if you happen to have set it.
//
// See
// http://www.lmdb.tech/doc/group__mdb.html#ga4fa8573d9236d54687c61827ebf8cac0
func (self *ReadWriteTxn) PutDupSort(db DBRef, key, val []byte, flags PutFlag) error {
	keyVal := asValue(key)
	valVal := asValue(val)

	err := asError(C.mdb_put(self.txn, C.MDB_dbi(db), (*C.MDB_val)(keyVal), (*C.MDB_val)(valVal), C.uint(flags&^Reserve)))

	keyVal.free()
	valVal.free()
	return err
}

// Delete a key-value pair from the database.
//
// The val is only necessary if you're using DupSort. If not, it's
// fine to use nil as val.
//
// See
// http://www.lmdb.tech/doc/group__mdb.html#gab8182f9360ea69ac0afd4a4eaab1ddb0
func (self *ReadWriteTxn) Delete(db DBRef, key, val []byte) error {
	keyVal := asValue(key)
	valVal := asValue(val)
	err := asError(C.mdb_del(self.txn, C.MDB_dbi(db), (*C.MDB_val)(keyVal), (*C.MDB_val)(valVal)))
	keyVal.free()
	valVal.free()
	return err
}
