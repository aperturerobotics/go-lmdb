package golmdb

/*
#include <lmdb.h>
*/
import "C"
import (
	"fmt"
	"syscall"
)

const Version = C.MDB_VERSION_STRING

type EnvironmentFlag C.uint

// Environment flags, used when opening a database. See
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

type DatabaseFlag C.uint

// Database flags. http://www.lmdb.tech/doc/group__mdb__dbi__open.html
// Used in calls to DBRef
const (
	ReverseKey = DatabaseFlag(C.MDB_REVERSEKEY)
	DupSort    = DatabaseFlag(C.MDB_DUPSORT)
	IntegerKey = DatabaseFlag(C.MDB_INTEGERKEY)
	DupFixed   = DatabaseFlag(C.MDB_DUPFIXED)
	IntegerDup = DatabaseFlag(C.MDB_INTEGERDUP)
	ReverseDup = DatabaseFlag(C.MDB_REVERSEDUP)
	Create     = DatabaseFlag(C.MDB_CREATE)
)

type PutFlag C.uint

// Write flags. http://www.lmdb.tech/doc/group__mdb__put.html
// Used in calls to Put
const (
	NoOverwrite = PutFlag(C.MDB_NOOVERWRITE)
	NoDupData   = PutFlag(C.MDB_NODUPDATA)
	Current     = PutFlag(C.MDB_CURRENT)
	Reserve     = PutFlag(C.MDB_RESERVE)
	Append      = PutFlag(C.MDB_APPEND)
	AppendDup   = PutFlag(C.MDB_APPENDDUP)
	Multiple    = PutFlag(C.MDB_MULTIPLE)
)

// copy flags. http://www.lmdb.tech/doc/group__mdb__copy.html
const copyCompact = C.MDB_CP_COMPACT

// An LMDB error. See the Return Codes in the Constants section.
type LMDBError C.int

// Return codes. http://www.lmdb.tech/doc/group__errors.html
//
// KeyExist and NotFound are return codes you may well encounter and
// expect to deal with in application code. The rest of them probably
// indicate something has gone terribly wrong.
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
