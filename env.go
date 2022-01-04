package golmdb

/*
#include <lmdb.h>
#include <stdlib.h>
*/
import "C"
import (
	"io/fs"
	"os"
	"unsafe"

	"github.com/rs/zerolog"
	"wellquite.org/actors"
)

type environment struct {
	env      *C.MDB_env
	readOnly bool
	mapSize  uint64
	pageSize uint64
}

func newEnvironment() (*environment, error) {
	var env *C.MDB_env
	err := asError(C.mdb_env_create(&env))
	if err != nil {
		return nil, err
	}
	return &environment{
		env:      env,
		pageSize: uint64(os.Getpagesize()),
	}, nil
}

// mdb_env_open. http://www.lmdb.tech/doc/group__mdb.html#ga32a193c6bf4d7d5c5d579e71f22e9340
// Open a path in the environment. You can only open one path at a
// time per environment. If the resulting error is non-nil, then you
// must call environment.close()
func (self *environment) open(path string, flags EnvironmentFlag, mode fs.FileMode) error {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	return asError(C.mdb_env_open(self.env, cPath, C.uint(flags), C.mdb_mode_t(mode)))
}

// mdb_env_close. http://www.lmdb.tech/doc/group__mdb.html#ga4366c43ada8874588b6a62fbda2d1e95
// Must be called if opening failed. Once this is called, the
// environment is unusable, and a new environment should be created.
func (self *environment) close() {
	C.mdb_env_close(self.env)
	self.env = nil
}

// mdb_env_set_mapsize. http://www.lmdb.tech/doc/group__mdb.html#gaa2506ec8dab3d969b0e609cd82e619e5
// Only valid to call before calling open, or if you can guarantee
// there are no transactions running at all. Up to you to ensure
// that. Size should be multiple of page size, which in Go can be
// found at os.Getpagesize()
//
// The mapsize is persisted into the path (assuming one is opened) on
// the next update to the path.
func (self *environment) setMapSize(size uint64) error {
	return asError(C.mdb_env_set_mapsize(self.env, C.size_t(size)))
}

// Uses mdb_env_info to access the current map size.
// http://www.lmdb.tech/doc/group__mdb.html#ga18769362c7e7d6cf91889a028a5c5947
func (self *environment) getMapSize() (uint64, error) {
	var cInfo C.MDB_envinfo
	err := asError(C.mdb_env_info(self.env, &cInfo))
	if err != nil {
		return 0, err
	}
	return uint64(cInfo.me_mapsize), nil
}

// mdb_env_set_maxreaders. http://www.lmdb.tech/doc/group__mdb.html#gae687966c24b790630be2a41573fe40e2
// The default is 126. This function may only be called after mdb_env_create and before mdb_env_open.
func (self *environment) setMaxReaders(size uint) error {
	return asError(C.mdb_env_set_maxreaders(self.env, C.uint(size)))
}

// mdb_env_set_maxdbs. http://www.lmdb.tech/doc/group__mdb.html#gaa2fc2f1f37cb1115e733b62cab2fcdbc
// This function may only be called after mdb_env_create and before mdb_env_open.
func (self *environment) setMaxNumberOfDBs(max uint) error {
	return asError(C.mdb_env_set_maxdbs(self.env, C.MDB_dbi(max)))
}

// mdb_txn_begin. http://www.lmdb.tech/doc/group__mdb.html#gad7ea55da06b77513609efebd44b26920
// This wrapping does not support nested transactions, so there is no
// provision for providing the parent transaction.
func (self *environment) txnBegin(readOnlyTxn bool) (txn *C.MDB_txn, err error) {
	flags := C.uint(0)
	if readOnlyTxn {
		flags = C.uint(ReadOnly)
	}
	err = asError(C.mdb_txn_begin(self.env, nil, flags, &txn))
	return
}

type LMDB struct {
	environment *environment

	clientFactory *lmdbClientFactory
}

// NewLMDB opens an LMDB database at the given path, creating it if necessary.
//
// NoTLS is always added to the flags. A sensible default flag is WriteMap.
//
// If the flags include ReadOnly then the database is opened in
// read-only mode, and all calls to Update will fail.
//
// If the flags do not include ReadOnly then an actor will be spawned
// to run and batch Update transactions. The actor will use the
// batchSize parameter to control the maximum number of Update
// transactions that get batched together. This is a maximum: if the
// actor has received some number of Update transactions and there are
// no further messages in its mailbox, then it'll run and commit them
// immediately. A reasonable starting value is the number of
// go-routines that could concurrently submit Update transactions.
func NewLMDB(log zerolog.Logger, path string, mode fs.FileMode, numReaders, numDBs uint, flags EnvironmentFlag, batchSize uint) (*LMDB, error) {
	environment, err := setupEnvironment(path, mode, numReaders, numDBs, flags)
	if err != nil {
		return nil, err
	}

	if flags&ReadOnly != 0 {
		return readOnlyLMDB(environment), nil

	} else {
		clientFactory, err := spawnLMDBActor(nil, &log, environment, batchSize)
		if err != nil {
			return nil, err
		}
		return &LMDB{
			environment:   environment,
			clientFactory: clientFactory,
		}, nil
	}
}

// NewManagedLMDB opens an LMDB database at the given path, creating it if necessary.
//
// NoTLS is always added to the flags. A sensible default flag is WriteMap.
//
// If the flags include ReadOnly then the database is opened in
// read-only mode, and all calls to Update will fail.
//
// If the flags do not include ReadOnly then an actor will be spawned
// as a child of the manager to run and batch Update transactions. The
// actor will use the batchSize parameter to control the maximum
// number of Update transactions that get batched together. This is a
// maximum: if the actor has received some number of Update
// transactions and there are no further messages in its mailbox, then
// it'll run and commit them immediately. A reasonable starting value
// is the number of go-routines that could concurrently submit Update
// transactions.
func NewManagedLMDB(manager actors.ManagerClient, path string, mode fs.FileMode, numReaders, numDBs uint, flags EnvironmentFlag, batchSize uint) (*LMDB, error) {
	environment, err := setupEnvironment(path, mode, numReaders, numDBs, flags)
	if err != nil {
		return nil, err
	}

	if flags&ReadOnly != 0 {
		return readOnlyLMDB(environment), nil

	} else {
		clientFactory, err := spawnLMDBActor(manager, nil, environment, batchSize)
		if err != nil {
			return nil, err
		}
		return &LMDB{
			environment:   environment,
			clientFactory: clientFactory,
		}, nil
	}
}

func setupEnvironment(path string, mode fs.FileMode, numReaders, numDBs uint, flags EnvironmentFlag) (*environment, error) {
	environment, err := newEnvironment()
	if err != nil {
		return nil, err
	}
	if err := environment.setMaxReaders(numReaders); err != nil {
		return nil, err
	}
	if err := environment.setMaxNumberOfDBs(numDBs); err != nil {
		return nil, err
	}
	if err := environment.open(path, flags|NoTLS, mode); err != nil {
		environment.close()
		return nil, err
	}

	mapSize, err := environment.getMapSize()
	if err != nil {
		environment.close()
		return nil, err
	}

	if remainder := mapSize % environment.pageSize; remainder != 0 {
		mapSize = (mapSize + environment.pageSize) - remainder
		if err := environment.setMapSize(mapSize); err != nil {
			environment.close()
			return nil, err
		}
	}
	environment.mapSize = mapSize

	return environment, nil
}

func readOnlyLMDB(environment *environment) *LMDB {
	environment.readOnly = true
	return &LMDB{
		environment:   environment,
		clientFactory: readOnlyLMDBClientFactory(environment),
	}
}

// Terminates the actor for Update transactions (if it's
// running). Waits for all concurrently running View transactions to
// finish, and then shuts down the LMDB environment.
//
// There is no guarantee that Update transactions that were submitted
// before TerminateSync was called will be run and committed. If you
// want to ensure that all such pending transactions are committed,
// you must perform this coordination yourself: waiting for all
// concurrent calls to Update to complete (and block new ones from
// starting) before you call TerminateSync.
//
// Note that this does not call mdb_env_sync. So if you've opened the
// database with NoSync or NoMetaSync or MapAsync then you're probably
// going to lose some data.
func (self *LMDB) TerminateSync() {
	self.clientFactory.newLMDBClient().TerminateSync()
	if !self.environment.readOnly {
		self.clientFactory.resizingLock.Lock()
		defer self.clientFactory.resizingLock.Unlock()
	}
	self.environment.close()
}

// NewLMDBClient returns a new client which allows you to submit updates
// or view the LMDB. If the LMDB was created with the ReadOnly flag
// then Updates will not be possible, and only View will work.
//
// Each client should only be used by a single go-routine.
func (self *LMDB) NewLMDBClient() *LMDBClient {
	return self.clientFactory.newLMDBClient()
}
