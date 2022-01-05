# GoLMDB

This is a high-level binding to [LMDB](https://www.symas.com/lmdb).

    go get wellquite.org/golmdb@latest

This binding uses [cgo](https://pkg.go.dev/cmd/cgo) and so to build,
you'll need a working cgo environment: a supported C compiler suite,
alongside the LMDB library and headers. LMDB is extremely widely used
and is available on "all" platforms, so it shouldn't be difficult to
get it on your platform. This binding has been predominately developed
with version 0.9.29, which is the current version at the time of
writing.

There are several Go bindings to LMDB available. All of them (that I
can find) are fairly low-level and tend to mirror the C API into
Go. This provides a lot of flexibility, but it leaves a lot of work to
do too.

This binding is high-level. It does not attempt to support all the
features of LMDB, nor expose the full low-level LMDB API. It provides:

* batching of updates: read-write transactions will be batched
  together automatically. This allows you to control (to some extent)
  the trade-off between latency and throughput: smaller batches can
  mean a longer queue of update transactions forming which can
  increase latency. But, larger batches can introduce their own delays
  as commits can be spread further apart.
* automatic resizing: LMDB returns an error if its database file fills
  up. LMDB also has an API to increase the size of its database file.
  Many modern file-systems support _sparse files_ which make it
  possible to create a huge empty file which only use up the amount of
  data that has been written. For such file-systems, it's very
  reasonable to start with a very large file. But not all file-systems
  support _sparse files_. So for better portability, it's preferable
  to support using smaller file sizes, and dynamically increase the
  size as necessary. This binding handles that automatically.
* minimal copy of data from Go to C and back again. See the docs on
  ReadWriteTxn.Put and ReadOnlyTxn.Get. In many cases, the value of a
  key-value pair can be written directly to disk without further
  copies being taken. Reads can access the data with just a single
  copy provided care is taken to not use the read data beyond the
  lifetime of the transaction.

Many of the more advanced flags from LMDB are still available, for
example flags to turn off syncing. These flags can make updates/writes
appear to be much faster. But they are also foot-guns: they can be
used safely in certain circumstances, but they're highly likely to
blow your foot off and destroy your data if you're not careful. This
binding will not save you from yourself! Refer back to the original
[LMDB docs](http://www.lmdb.tech/doc/group__mdb.html) if you're in any
doubt.
