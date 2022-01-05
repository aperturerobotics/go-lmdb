package golmdb

/*
#include <lmdb.h>
*/
import "C"
import (
	"fmt"
	"syscall"
)

// The version of LMDB that has been linked against.
const Version = C.MDB_VERSION_STRING

// Used in calls to NewLMDB() and NewManagedLMDB()
type EnvironmentFlag C.uint

// Environment flags
//
// See
// http://www.lmdb.tech/doc/group__mdb__env.html and
// http://www.lmdb.tech/doc/group__mdb.html#ga32a193c6bf4d7d5c5d579e71f22e9340
const (
	FixedMap    = EnvironmentFlag(C.MDB_FIXEDMAP)
	NoSubDir    = EnvironmentFlag(C.MDB_NOSUBDIR)
	NoSync      = EnvironmentFlag(C.MDB_NOSYNC)
	ReadOnly    = EnvironmentFlag(C.MDB_RDONLY)
	NoMetaSync  = EnvironmentFlag(C.MDB_NOMETASYNC)
	WriteMap    = EnvironmentFlag(C.MDB_WRITEMAP)
	MapAsync    = EnvironmentFlag(C.MDB_MAPASYNC)
	NoTLS       = EnvironmentFlag(C.MDB_NOTLS)
	NoLock      = EnvironmentFlag(C.MDB_NOLOCK)
	NoReadAhead = EnvironmentFlag(C.MDB_NORDAHEAD)
	NoMemLimit  = EnvironmentFlag(C.MDB_NOMEMINIT)
)

// Used in calls to ReadOnlyTxn.DBRef()
type DatabaseFlag C.uint

// Database flags
//
// See http://www.lmdb.tech/doc/group__mdb__dbi__open.html
const (
	ReverseKey = DatabaseFlag(C.MDB_REVERSEKEY)
	DupSort    = DatabaseFlag(C.MDB_DUPSORT)
	IntegerKey = DatabaseFlag(C.MDB_INTEGERKEY)
	DupFixed   = DatabaseFlag(C.MDB_DUPFIXED)
	IntegerDup = DatabaseFlag(C.MDB_INTEGERDUP)
	ReverseDup = DatabaseFlag(C.MDB_REVERSEDUP)
	Create     = DatabaseFlag(C.MDB_CREATE)
)

// Used in calls to ReadWriteTxn.Put(), ReadWriteTxn.PutDupSort(), Cursor.Put(), and Cursor.PutDupSort()
type PutFlag C.uint

// Put flags
//
// See http://www.lmdb.tech/doc/group__mdb__put.html
const (
	NoOverwrite = PutFlag(C.MDB_NOOVERWRITE)
	NoDupData   = PutFlag(C.MDB_NODUPDATA)
	Current     = PutFlag(C.MDB_CURRENT)
	Reserve     = PutFlag(C.MDB_RESERVE)
	Append      = PutFlag(C.MDB_APPEND)
	AppendDup   = PutFlag(C.MDB_APPENDDUP)
	multiple    = PutFlag(C.MDB_MULTIPLE) // not exported as the API doesn't support it
)

// Used in calls to Cursor.GetAndMove
type CursorOp C.uint

// Cursor ops
//
// See http://www.lmdb.tech/doc/group__mdb.html#ga1206b2af8b95e7f6b0ef6b28708c9127
const (
	First    = CursorOp(C.MDB_FIRST)     // Move to the start of the database. Return the key and value.
	FirstDup = CursorOp(C.MDB_FIRST_DUP) // For DupSort only: move to the first value of the current key. Return the key and value.

	Last    = CursorOp(C.MDB_LAST)     // Move to the end of the database. Return the key and value.
	LastDup = CursorOp(C.MDB_LAST_DUP) // For DupSort only: move to the last value of the current key. Return the key and value.

	GetCurrent = CursorOp(C.MDB_GET_CURRENT) // No movement. Return the current key and value.

	GetBoth      = CursorOp(C.MDB_GET_BOTH)       // For DupSort only: move to the given key and value and return them.
	GetBothRange = CursorOp(C.MDB_GET_BOTH_RANGE) // For DupSort only: move to the first key and value greater than or equal to the given key and value. Return the key and value.

	Set      = CursorOp(C.MDB_SET)       // Move to the given key. Don't return anything.
	SetKey   = CursorOp(C.MDB_SET_KEY)   // Move to the given key. Return the key and value.
	SetRange = CursorOp(C.MDB_SET_RANGE) // Move to the first key and value greater than or equal to the given key. Return the key and value.

	Next      = CursorOp(C.MDB_NEXT)       // Move to the next key-value pair. For DupSort databases, move to the next value of the current key, if there is one, otherwise the first value of the next key. Return the key and value.
	NextDup   = CursorOp(C.MDB_NEXT_DUP)   // For DupSort only: move to the next value of the current key, if there is one. Return the key and value.
	NextNoDup = CursorOp(C.MDB_NEXT_NODUP) // For DupSort only: move to the first value of the next key. Return the key and value.

	Prev      = CursorOp(C.MDB_PREV)       // Move to the previous key-value pair. For DupSort databases, move to the previous value of the current key, if there is one, otherwise the last value of the previous key. Return the key and value.
	PrevDup   = CursorOp(C.MDB_PREV_DUP)   // For DupSort only: move to the previous value of the current key, if there is one. Return the key and value.
	PrevNoDup = CursorOp(C.MDB_PREV_NODUP) // For DupSort only: move to the last value of the previous key. Return the key and value.

	getMultiple  = CursorOp(C.MDB_GET_MULTIPLE)  // not exported as the API doesn't support it
	nextMultiple = CursorOp(C.MDB_NEXT_MULTIPLE) // not exported as the API doesn't support it
)

// Copy flags. http://www.lmdb.tech/doc/group__mdb__copy.html
const copyCompact = C.MDB_CP_COMPACT

// An LMDB error. See the Return Codes in the Constants section.
type LMDBError C.int

// Return codes
//
// KeyExist and NotFound are return codes you may well encounter and
// expect to deal with in application code. The rest of them probably
// indicate something has gone terribly wrong.
//
// See
// http://www.lmdb.tech/doc/group__errors.html
const (
	success         = C.MDB_SUCCESS
	KeyExist        = LMDBError(C.MDB_KEYEXIST)
	NotFound        = LMDBError(C.MDB_NOTFOUND)
	PageNotFound    = LMDBError(C.MDB_PAGE_NOTFOUND)
	Corrupted       = LMDBError(C.MDB_CORRUPTED)
	PanicMDB        = LMDBError(C.MDB_PANIC)
	VersionMismatch = LMDBError(C.MDB_VERSION_MISMATCH)
	Invalid         = LMDBError(C.MDB_INVALID)
	MapFull         = LMDBError(C.MDB_MAP_FULL)
	DBsFull         = LMDBError(C.MDB_DBS_FULL)
	ReadersFull     = LMDBError(C.MDB_READERS_FULL)
	TLSFull         = LMDBError(C.MDB_TLS_FULL)
	TxnFull         = LMDBError(C.MDB_TXN_FULL)
	CursorFull      = LMDBError(C.MDB_CURSOR_FULL)
	PageFull        = LMDBError(C.MDB_PAGE_FULL)
	MapResized      = LMDBError(C.MDB_MAP_RESIZED)
	Incompatible    = LMDBError(C.MDB_INCOMPATIBLE)
	BadRSlot        = LMDBError(C.MDB_BAD_RSLOT)
	BadTxt          = LMDBError(C.MDB_BAD_TXN)
	BadValSize      = LMDBError(C.MDB_BAD_VALSIZE)
	BadDBI          = LMDBError(C.MDB_BAD_DBI)
)

const minErrno, maxErrno = C.MDB_KEYEXIST, C.MDB_LAST_ERRCODE

func (self LMDBError) Error() string {
	str := C.GoString(C.mdb_strerror(C.int(self)))
	if str == "" {
		return fmt.Sprintf(`LMDB Error: %d`, int(self))
	}
	return str
}

func asError(code C.int) error {
	if code == success {
		return nil
	}
	// If you check the url http://www.lmdb.tech/doc/group__errors.html
	// it should show that the return codes form a contiguous sequence,
	// and that maxErrno is inclusive as it's an alias of BadDBI
	if minErrno <= code && code <= maxErrno {
		return LMDBError(code)
	}
	return syscall.Errno(code)
}
