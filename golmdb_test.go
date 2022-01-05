package golmdb_test

import (
	"encoding/binary"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/matryer/is"
	"github.com/rs/zerolog"
	"golang.org/x/term"

	"wellquite.org/golmdb"
)

func NewTestLogger(tb testing.TB) zerolog.Logger {
	consoleWriter := zerolog.NewConsoleWriter(zerolog.ConsoleTestWriter(tb))
	isTerminal := term.IsTerminal(int(os.Stdout.Fd()))
	consoleWriter.NoColor = !isTerminal
	return zerolog.New(consoleWriter).With().Timestamp().Logger()
}

func SetGlobalLogLevelDebug() {
	SetGlobalLogLevel(zerolog.DebugLevel)
}

func SetGlobalLogLevel(level zerolog.Level) {
	if testing.Verbose() {
		zerolog.SetGlobalLevel(level)
	} else {
		zerolog.SetGlobalLevel(zerolog.Disabled)
	}
}

func TestVersion(t *testing.T) {
	is := is.New(t)
	is.True(golmdb.Version != "")
}

func TestLicense(t *testing.T) {
	is := is.New(t)
	is.True(golmdb.License != "")
}

func TestError(t *testing.T) {
	is := is.New(t)
	is.True(golmdb.LMDBError(golmdb.KeyExist).Error() != "")
}

func TestOpenClose(t *testing.T) {
	SetGlobalLogLevelDebug()
	log := NewTestLogger(t)

	is := is.New(t)
	dir, err := os.MkdirTemp("", "golmdb")
	is.NoErr(err)
	defer os.RemoveAll(dir)

	lmdb, err := golmdb.NewLMDB(log, dir, 0666, 16, 4, golmdb.WriteMap, 16)
	is.NoErr(err)
	lmdb.TerminateSync()
}

func TestProvokeResize(t *testing.T) {
	SetGlobalLogLevelDebug()
	log := NewTestLogger(t)

	is := is.New(t)
	dir, err := os.MkdirTemp("", "golmdb")
	is.NoErr(err)
	defer os.RemoveAll(dir)

	cores := 2 * runtime.GOMAXPROCS(0)
	client, err := golmdb.NewLMDB(log, dir, 0666, 16, 4, golmdb.WriteMap, uint(cores))
	is.NoErr(err)

	var db golmdb.DBRef
	err = client.Update(func(txn *golmdb.ReadWriteTxn) (err error) {
		db, err = txn.DBRef("test", golmdb.Create)
		return
	})
	is.NoErr(err)

	n := 16384

	var wg sync.WaitGroup
	wg.Add(cores)

	for offset := 0; offset < cores; offset++ {
		offsetCopy := offset
		go func() {
			defer wg.Done()

			val := make([]byte, 1024)
			key := make([]byte, 8)

			var sumLatency, done time.Duration

			for idx := offsetCopy; idx < n; idx += cores {
				done++
				binary.BigEndian.PutUint64(key, uint64(idx))
				start := time.Now()
				err := client.Update(func(txn *golmdb.ReadWriteTxn) error {
					return txn.Put(db, key, val, 0)
				})
				sumLatency += time.Now().Sub(start)
				if err != nil {
					is.NoErr(err)
					return
				}
			}

			log.Info().Int("idx", offsetCopy).Int("done", int(done)).Str("avg latency", (sumLatency / done).String()).Send()
		}()
	}

	wg.Wait()

	client.TerminateSync()
}
