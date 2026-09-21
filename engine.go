// Package mvcckv implements an embedded key-value storage engine with
// snapshot-isolation transactions, multi-version concurrency control (MVCC)
// and a CRC-protected write-ahead log (WAL).
//
// Only the Go standard library is used.
package mvcckv

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// Sentinel errors returned by the engine and its transactions.
var (
	// ErrWriteConflict is returned from Commit when another concurrent writer
	// has already committed a change to one of the same keys
	// (first-committer-wins). The losing transaction is rolled back.
	ErrWriteConflict = errors.New("mvcckv: write conflict")
	// ErrKeyNotFound is returned from Tx.Get for a key that is not present.
	ErrKeyNotFound = errors.New("mvcckv: key not found")
	// ErrTxClosed is returned when a finished transaction is used again.
	ErrTxClosed = errors.New("mvcckv: transaction already committed or rolled back")
	// ErrReadOnly is returned when a read-only transaction mutates data.
	ErrReadOnly = errors.New("mvcckv: transaction is read-only")
	// ErrEngineClosed is returned when a closed engine is used.
	ErrEngineClosed = errors.New("mvcckv: engine is closed")
)

// version is one entry on a key's version chain.
type version struct {
	key         string
	value       []byte
	createdTxID uint64 // commit timestamp that made this version visible
	deletedTxID uint64 // commit timestamp that superseded this version; 0 while live
	isDeleted   bool   // true for tombstone versions created by Delete
}

// writeEntry is a buffered mutation inside an open transaction.
type writeEntry struct {
	key     string
	value   []byte
	deleted bool
}

// Engine is the embedded MVCC key-value store.
type Engine struct {
	// data maps a key to its version chain, newest version first.
	data map[string][]*version

	// indexMu guards data, active and snapshotTS bookkeeping. It is the
	// fine-grained lock that allows concurrent readers to proceed while
	// writers are serialized only at Commit time.
	indexMu sync.RWMutex

	// active tracks beginID -> snapshotID for every in-flight transaction.
	active map[uint64]uint64

	// commitMu serializes commit ordering (first-committer-wins).
	commitMu sync.Mutex

	// lifecycleMu makes Begin/Close mutually exclusive with shutdown.
	lifecycleMu sync.RWMutex

	// nextTxID hands out unique, monotonic transaction ids at Begin.
	nextTxID atomic.Uint64
	// committedTS holds the timestamp of the latest published commit.
	committedTS atomic.Uint64

	walMu   sync.Mutex
	walFile *os.File
	walSize uint64

	dir    string
	closed atomic.Bool

	// recoveryWarnings records non-fatal recovery notes (truncation etc.).
	recoveryWarnings []string
}

// Open opens (or creates) an engine rooted at dirPath, replaying any existing
// WAL to rebuild the in-memory multi-version index.
func Open(dirPath string) (*Engine, error) {
	if err := os.MkdirAll(dirPath, 0o755); err != nil {
		return nil, err
	}

	e := &Engine{
		data:   make(map[string][]*version),
		active: make(map[uint64]uint64),
		dir:    dirPath,
	}

	if err := recoverWAL(dirPath, e); err != nil {
		return nil, err
	}

	walPath := filepath.Join(dirPath, walName)
	f, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, err
	}
	e.walFile = f

	if info, err := f.Stat(); err == nil {
		e.walSize = uint64(info.Size())
	}
	return e, nil
}

// Close flushes the WAL and releases all resources.
func (e *Engine) Close() error {
	e.lifecycleMu.Lock()
	defer e.lifecycleMu.Unlock()

	if !e.closed.CompareAndSwap(false, true) {
		return ErrEngineClosed
	}

	e.walMu.Lock()
	defer e.walMu.Unlock()

	var err error
	if e.walFile != nil {
		err = e.walFile.Close()
		e.walFile = nil
	}
	return err
}

// RecoveryWarnings returns non-fatal notes produced while opening the engine,
// for example a CRC-damaged WAL tail that was truncated during recovery.
func (e *Engine) RecoveryWarnings() []string {
	e.indexMu.RLock()
	defer e.indexMu.RUnlock()
	out := make([]string, len(e.recoveryWarnings))
	copy(out, e.recoveryWarnings)
	return out
}

// activeWatermark returns the smallest snapshot id among all in-flight
// transactions. Any version with createdTxID < watermark is older than every
// active snapshot and therefore eligible for garbage collection.
func (e *Engine) watermarkLocked() uint64 {
	var wm uint64 = ^uint64(0)
	for _, snapID := range e.active {
		if snapID < wm {
			wm = snapID
		}
	}
	return wm
}

// visibleLocked reports whether a version created at createdTxID is visible to
// a reader whose snapshot is snapID. A snapshot sees every version committed
// at or before the latest commit timestamp it observed at Begin.
func visibleLocked(createdTxID, snapID uint64) bool {
	return createdTxID <= snapID
}

// applyWrite installs a committed mutation as a new head version on the chain.
// The previous head, if any, is marked as superseded (soft-deleted) at this
// commit timestamp. Used by both live commits and WAL replay.
func applyWrite(e *Engine, key string, value []byte, deleted bool, commitTS uint64) {
	v := &version{
		key:         key,
		value:       value,
		createdTxID: commitTS,
		deletedTxID: 0,
		isDeleted:   deleted,
	}
	chain := e.data[key]
	if len(chain) > 0 {
		chain[0].deletedTxID = commitTS
	}
	e.data[key] = append([]*version{v}, chain...)
}
