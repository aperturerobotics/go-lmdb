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

type lmdbClientFactory struct {
	environment *environment
	*actors.BackPressureClientBaseFactory
	readWriteTxnMsgPool *sync.Pool
	resizingLock        *sync.RWMutex
}

func readOnlyLMDBClientFactory(environment *environment) *lmdbClientFactory {
	return &lmdbClientFactory{
		environment: environment,
	}
}

func spawnLMDBActor(manager actors.ManagerClient, log *zerolog.Logger, environment *environment, batchSize uint) (*lmdbClientFactory, error) {
	server := &server{
		batchSize: int(batchSize),
		readWriteTxn: &ReadWriteTxn{
			ReadOnlyTxn: ReadOnlyTxn{
				environment: environment,
			},
		},
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

	return &lmdbClientFactory{
		environment:                   environment,
		BackPressureClientBaseFactory: actors.NewBackPressureClientBaseFactory(clientBase),
		readWriteTxnMsgPool: &sync.Pool{
			New: func() interface{} {
				return &readWriteTxnMsg{}
			},
		},
		resizingLock: &server.resizingLock,
	}, nil
}

func (self *lmdbClientFactory) newLMDBClient() *LMDBClient {
	if self.environment.readOnly {
		return &LMDBClient{
			readOnlyTxn: ReadOnlyTxn{environment: self.environment},
		}

	} else {
		return &LMDBClient{
			BackPressureClientBase: self.BackPressureClientBaseFactory.NewClient(),
			readOnlyTxn:            ReadOnlyTxn{environment: self.environment},
			readWriteTxnMsgPool:    self.readWriteTxnMsgPool,
			resizingLock:           self.resizingLock,
		}
	}
}

// --- Client side API ---

// Each client must only be used by a single go-routine.
type LMDBClient struct {
	*actors.BackPressureClientBase
	readOnlyTxn         ReadOnlyTxn
	resizingLock        *sync.RWMutex
	readWriteTxnMsgPool *sync.Pool
}

var _ actors.Client = (*LMDBClient)(nil)

// Run a View: a read-only transaction. The transaction will be run in
// the current go-routine, and it will only be run once. You must not
// call this after, or in a race with, a call to
// TerminateSync. Multiple concurrent Views can proceed concurrently.
//
// Do not attempt nested transactions: they are not supported.
func (self *LMDBClient) View(fun func(rotxn *ReadOnlyTxn) error) (err error) {
	if !self.readOnlyTxn.environment.readOnly {
		self.resizingLock.RLock()
		defer self.resizingLock.RUnlock()
	}

	if self.readOnlyTxn.txn == nil {
		txn, err := self.readOnlyTxn.environment.txnBegin(true)
		if err != nil {
			return err
		}
		self.readOnlyTxn.txn = txn
	} else {
		if err := asError(C.mdb_txn_renew(self.readOnlyTxn.txn)); err != nil {
			return err
		}
	}

	// use a defer as it'll run even on a panic
	defer C.mdb_txn_reset(self.readOnlyTxn.txn)
	return fun(&self.readOnlyTxn)
}

// Run an Update: a read-write transaction. The transaction will not
// be run in the current go-routine, and it may be run more than
// once. Only a single Update transaction can occur at a time, which
// golmdb will take care of for you. An Update transaction can proceed
// concurrently with View transactions.
//
// Do not attempt nested transactions: they are not supported.
func (self *LMDBClient) Update(fun func(rwtxn *ReadWriteTxn) error) error {
	if self.readOnlyTxn.environment.readOnly {
		return errors.New("Cannot update: LMDB has been opened in ReadOnly mode")
	}

	msg := self.readWriteTxnMsgPool.Get().(*readWriteTxnMsg)
	defer self.readWriteTxnMsgPool.Put(msg)
	msg.txnFun = fun

	if self.SendSync(msg, true) {
		return msg.err
	} else {
		return errors.New("golmdb server is terminated")
	}
}

// --- Server side ---

type server struct {
	actors.BackPressureServerBase

	batchSize    int
	batch        []*readWriteTxnMsg
	resizingLock sync.RWMutex
	readWriteTxn *ReadWriteTxn
}

var _ actors.Server = (*server)(nil)

func (self *server) Init(log zerolog.Logger, mailboxReader *mailbox.MailboxReader, selfClient *actors.ClientBase) (err error) {
	// this is required for the writer - even though we use NoTLS
	runtime.LockOSThread()
	return self.BackPressureServerBase.Init(log, mailboxReader, selfClient)
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
		return self.BackPressureServerBase.HandleMsg(msg)
	}
}

func (self *server) runBatch(batch []*readWriteTxnMsg) error {
	if len(batch) == 0 {
		return nil
	}

	readWriteTxn := self.readWriteTxn

OUTER:
	for {
		txn, err := readWriteTxn.environment.txnBegin(false)

		if err == nil {
			readWriteTxn.txn = txn

			for idx, msg := range batch {
				if msg == nil {
					continue
				}

				err = msg.txnFun(readWriteTxn)

				if err != nil {
					C.mdb_txn_abort(readWriteTxn.txn)
					readWriteTxn.txn = nil

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
			err = asError(C.mdb_txn_commit(readWriteTxn.txn))
			readWriteTxn.txn = nil
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

func (self *server) increaseSize() error {
	self.resizingLock.Lock()
	defer self.resizingLock.Unlock()

	currentMapSize := self.readWriteTxn.environment.mapSize
	mapSize := uint64(float64(currentMapSize) * 1.5)
	if remainder := mapSize % self.readWriteTxn.environment.pageSize; remainder != 0 {
		mapSize = (mapSize + self.readWriteTxn.environment.pageSize) - remainder
	}

	if err := self.readWriteTxn.environment.setMapSize(mapSize); err != nil {
		self.Log.Error().Uint64("current size", currentMapSize).Uint64("new size", mapSize).Err(err).Msg("increasing map size")
		return err
	}
	if self.Log.Debug().Enabled() {
		self.Log.Debug().Uint64("current size", currentMapSize).Uint64("new size", mapSize).Msg("increasing map size")
	}
	self.readWriteTxn.environment.mapSize = mapSize
	return nil
}
