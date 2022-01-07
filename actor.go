package golmdb

/*
#include <lmdb.h>
*/
import "C"
import (
	"errors"
	"runtime"
	"sync"

	"github.com/rs/zerolog"
	"wellquite.org/actors"
	"wellquite.org/actors/mailbox"
)

type readWriteTxnMsg struct {
	actors.MsgSyncBase
	txnFun func(*ReadWriteTxn) error // input
	err    error                     // output
}

func readOnlyLMDBClient(environment *environment) *LMDBClient {
	environment.readOnly = true
	readersThrottle := make(chan struct{}, environment.numReaders)
	for idx := 0; idx < int(environment.numReaders); idx++ {
		readersThrottle <- struct{}{}
	}
	return &LMDBClient{
		environment:     environment,
		readersThrottle: readersThrottle,
	}
}

func spawnLMDBActor(manager actors.ManagerClient, log *zerolog.Logger, environment *environment, batchSize uint) (*LMDBClient, error) {
	server := &server{
		batchSize:   int(batchSize),
		environment: environment,
	}

	var err error
	var clientBase *actors.ClientBase
	if manager == nil {
		clientBase, err = actors.Spawn(*log, server, "golmdb")
	} else {
		clientBase, err = manager.Spawn(server, "golmdb")
	}
	if err != nil {
		return nil, err
	}

	readersThrottle := make(chan struct{}, environment.numReaders)
	for idx := 0; idx < int(environment.numReaders); idx++ {
		readersThrottle <- struct{}{}
	}

	return &LMDBClient{
		ClientBase:      clientBase,
		environment:     environment,
		readersThrottle: readersThrottle,
		resizingLock:    &server.resizingLock,
		readWriteTxnMsgPool: &sync.Pool{
			New: func() interface{} {
				return &readWriteTxnMsg{}
			},
		},
	}, nil
}

// --- Client side API ---

// A client to the whole LMDB database. The client allows you to run
// Views (read-only transactions), Updates (read-write transactions),
// and close/terminate the database. A single client is safe for any
// number of go-routines to use concurrently.
type LMDBClient struct {
	*actors.ClientBase
	environment         *environment
	readersThrottle     chan struct{}
	resizingLock        *sync.RWMutex
	readWriteTxnMsgPool *sync.Pool
}

var _ actors.Client = (*LMDBClient)(nil)

// Run a View: a read-only transaction. The transaction will be run in
// the current go-routine, and it will only be run once. Multiple
// concurrent calls to View can proceed concurrently.
//
// As this is a read-only transaction, the transaction is aborted no
// matter what the fun returns. The error that the fun returns is
// returned from this method.
//
// Nested transactions are not supported.
func (self *LMDBClient) View(fun func(rotxn *ReadOnlyTxn) error) (err error) {
	if !self.environment.readOnly {
		self.resizingLock.RLock()
		defer self.resizingLock.RUnlock()
	}
	token := <-self.readersThrottle
	defer func() { self.readersThrottle <- token }()

	txn, err := self.environment.txnBegin(true)
	if err != nil {
		return err
	}
	readOnlyTxn := ReadOnlyTxn{txn: txn}
	// use a defer as it'll run even on a panic
	defer C.mdb_txn_abort(txn)
	return fun(&readOnlyTxn)
}

// Run an Update: a read-write transaction. The transaction will not
// be run in the current go-routine, and it may be run more than once,
// even if the fun itself returns a nil error. Only a single Update
// transaction can occur at a time, and golmdb will take care of this
// for you. An Update transaction can proceed concurrently with one or
// more View transactions.
//
// If the fun returns nil, then the transaction will be committed. If
// the fun returns any non-nil err then the transaction will be
// aborted. Any non-nil error returned by the fun is returned from
// this method.
//
// Nested transactions are not supported.
func (self *LMDBClient) Update(fun func(rwtxn *ReadWriteTxn) error) error {
	if self.environment.readOnly {
		return errors.New("Cannot update: LMDB has been opened in ReadOnly mode")
	}

	msg := self.readWriteTxnMsgPool.Get().(*readWriteTxnMsg)
	msg.txnFun = fun

	if self.SendSync(msg, true) {
		err := msg.err
		self.readWriteTxnMsgPool.Put(msg)
		return err
	} else {
		self.readWriteTxnMsgPool.Put(msg)
		return errors.New("golmdb server is terminated")
	}
}

// Terminates the actor for Update transactions (if it's running), and
// then shuts down the LMDB database.
//
// You must make sure that all concurrently running transactions have
// finished before you call this method: this method will not wait for
// concurrent View transactions to finish (or prevent new ones from
// starting), and it will not wait for calls to Update to complete.
// It is your responsibility to make sure all users of the client are
// finished and shutdown before calling TerminateSync.
//
// Note that this does not call mdb_env_sync. So if you've opened the
// database with NoSync or NoMetaSync or MapAsync then you will need
// to call Sync() before TerminateSync()
func (self *LMDBClient) TerminateSync() {
	if !self.environment.readOnly {
		self.ClientBase.TerminateSync()
	}
	self.environment.close()
}

// Manually sync the database to disk.
//
// See http://www.lmdb.tech/doc/group__mdb.html#ga85e61f05aa68b520cc6c3b981dba5037
//
// Unless you're using MapAsync or NoSync or NoMetaSync flags when
// opening the LMDB database, you do not need to worry about calling
// this. If you are using any of those flags then LMDB will not be
// syncing data to disk on every transaction commit, which raises the
// possibility of data loss or corruption in the event of a crash or
// unexpected exit. Nevertheless, those flags are sometimes useful,
// for example when rapidly loading a data set into the database. An
// explicit call to Sync is then needed to flush everything through
// onto disk.
func (self *LMDBClient) Sync(force bool) error {
	return self.environment.sync(force)
}

// Copy the entire database to a new path, optionally compacting it.
//
// See http://www.lmdb.tech/doc/group__mdb.html#ga3bf50d7793b36aaddf6b481a44e24244
//
// This can be done with the database in use: it allows you to take
// backups of the dataset without stopping anything. However, as the
// docs note, this is essentially a read-only transaction to read the
// entire database and copy it out. If that takes a long time (because
// it's a large database) and there are updates to the database going
// on at the same time, then the original database can grow in size
// due to needing to keep the old data around so that the read-only
// transaction doing the copy sees a consistent snapshot of the entire
// database.
func (self *LMDBClient) Copy(path string, compact bool) error {
	return self.environment.copy(path, compact)
}

// --- Server side ---

type server struct {
	actors.ServerBase

	batchSize    int
	batch        []*readWriteTxnMsg
	resizingLock sync.RWMutex
	environment  *environment
	readWriteTxn ReadWriteTxn
}

var _ actors.Server = (*server)(nil)

func (self *server) Init(log zerolog.Logger, mailboxReader *mailbox.MailboxReader, selfClient *actors.ClientBase) (err error) {
	// this is required for the writer - even though we use NoTLS
	runtime.LockOSThread()
	return self.ServerBase.Init(log, mailboxReader, selfClient)
}

func (self *server) HandleMsg(msg mailbox.Msg) error {
	switch msgT := msg.(type) {
	case *readWriteTxnMsg:
		self.batch = append(self.batch, msgT)
		if len(self.batch) == self.batchSize || self.MailboxReader.IsEmpty() {
			batch := self.batch
			self.batch = self.batch[:0]
			if self.Log.Trace().Enabled() {
				self.Log.Trace().Int("batch size", len(batch)).Msg("running batch")
			}
			return self.runBatch(batch)
		}
		return nil

	default:
		return self.ServerBase.HandleMsg(msg)
	}
}

func (self *server) runBatch(batch []*readWriteTxnMsg) error {
	if len(batch) == 0 {
		return nil
	}

	readWriteTxn := &self.readWriteTxn

OUTER:
	for {
		txn, err := self.environment.txnBegin(false)

		if err == nil {
			readWriteTxn.txn = txn

			for idx, msg := range batch {
				if msg == nil {
					continue
				}

				err = msg.txnFun(readWriteTxn)

				if err != nil {
					readWriteTxn.txn = nil
					C.mdb_txn_abort(txn)

					if err == MapFull {
						break

					} else {
						// assume problem with the current msg only, so abandon
						// that one, and rerun everything else.
						msg.err = err
						msg.MarkProcessed()
						batch[idx] = nil

						continue OUTER
					}
				}
			}
		}

		if err == nil {
			readWriteTxn.txn = nil
			err = asError(C.mdb_txn_commit(txn))
		}

		if err == MapFull {
			// MapFull can come either from a Put, or from a Commit. We
			// need to increase the size, and then re-run the entire batch.
			err = self.increaseSize()
			if err == nil {
				continue OUTER
			}
		}

		for _, msg := range batch {
			if msg != nil {
				msg.err = err
				msg.MarkProcessed()
			}
		}

		return err
	}
}

func (self *server) Terminated(err error, caughtPanic interface{}) {
	if self.readWriteTxn.txn != nil { // this can happen if a txn fun panics
		C.mdb_txn_abort(self.readWriteTxn.txn)
		self.readWriteTxn.txn = nil
	}
	self.ServerBase.Terminated(err, caughtPanic)
}

func (self *server) increaseSize() error {
	self.resizingLock.Lock()
	defer self.resizingLock.Unlock()

	currentMapSize := self.environment.mapSize
	mapSize := uint64(float64(currentMapSize) * 1.5)
	if remainder := mapSize % self.environment.pageSize; remainder != 0 {
		mapSize = (mapSize + self.environment.pageSize) - remainder
	}

	if err := self.environment.setMapSize(mapSize); err != nil {
		self.Log.Error().Uint64("current size", currentMapSize).Uint64("new size", mapSize).Err(err).Msg("increasing map size")
		return err
	}
	if self.Log.Debug().Enabled() {
		self.Log.Debug().Uint64("current size", currentMapSize).Uint64("new size", mapSize).Msg("increasing map size")
	}
	self.environment.mapSize = mapSize
	return nil
}
