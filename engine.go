package kvstore

import (
	"errors"
	"io"
	"os"
	"sort"
	"sync"
)

const walFileName = "wal.log"

// Sentinel errors.
var (
	// ErrKeyNotFound is returned when reading a key that has no visible version.
	ErrKeyNotFound = errors.New("kvstore: key not found")
	// ErrWriteConflict is returned on commit under first-committer-wins.
	ErrWriteConflict = errors.New("kvstore: write conflict")
	// ErrTxClosed is returned when a finished transaction is used again.
	ErrTxClosed = errors.New("kvstore: transaction already committed or rolled back")
	// ErrReadOnlyTx is returned when a read-only transaction performs a write.
	ErrReadOnlyTx = errors.New("kvstore: cannot write in a read-only transaction")
	// ErrEngineClosed is returned after the engine has been closed.
	ErrEngineClosed = errors.New("kvstore: engine is closed")
	// ErrWALCorrupt signals a CRC/checksum or framing failure in the WAL tail.
	ErrWALCorrupt = errors.New("kvstore: corrupt WAL record")

	errTruncated = errors.New("truncated frame")
	errCorrupt   = errors.New("corrupt frame")
)

// Options configures an Engine.
type Options struct {
	// SyncWAL fsyncs the WAL on every commit when true (default).
	SyncWAL bool
}

// DefaultOptions returns durable settings (WAL synced per commit).
func DefaultOptions() Options {
	return Options{SyncWAL: true}
}

// record is one version on a key's version chain (newest first).
type record struct {
	key         string
	value       []byte
	createdTxID uint64 // commit timestamp that installed this version
	deletedTxID uint64 // commit timestamp that superseded it; 0 while live
	isDeleted   bool   // true for a delete tombstone
}

// txWrite is one buffered mutation inside an open transaction.
type txWrite struct {
	isDelete bool
	value    []byte
}

// Engine is an embedded MVCC key/value store with WAL durability.
type Engine struct {
	mu sync.RWMutex

	// versions maps key -> version chain, newest version first.
	versions map[string][]*record
	// active tracks live transactions keyed by begin timestamp.
	active map[uint64]*Tx
	// nextTS is the global monotonic timestamp counter.
	nextTS uint64

	wal    *wal
	closed bool

	// walMu serializes all frame appends to the single WAL file.
	walMu sync.Mutex
}

// Open opens (or creates) an engine rooted at dir.
// If the directory contains a WAL, it is replayed. A truncated/corrupt WAL
// tail is discarded and returned as a warning wrapped around ErrWALCorrupt;
// the returned engine is still usable with all committed prefix state.
func Open(dir string) (*Engine, error) {
	return OpenWithOptions(dir, DefaultOptions())
}

// OpenWithOptions opens an engine with explicit options.
func OpenWithOptions(dir string, opts Options) (*Engine, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	w, err := openWAL(dir, opts.SyncWAL)
	if err != nil {
		return nil, err
	}

	e := &Engine{
		versions: make(map[string][]*record),
		active:   make(map[uint64]*Tx),
		nextTS:   1,
		wal:      w,
	}

	recovered, _, _, rerr := recoverWAL(w.path)
	if rerr != nil && !errors.Is(rerr, ErrWALCorrupt) {
		_ = w.close()
		return nil, rerr
	}

	// Apply committed transactions in commit-timestamp order so version chains
	// are reconstructed identically to the original run.
	commitIDs := make([]uint64, 0, len(recovered.committed))
	for cid := range recovered.committed {
		commitIDs = append(commitIDs, cid)
	}
	sort.Slice(commitIDs, func(i, j int) bool { return commitIDs[i] < commitIDs[j] })
	for _, cid := range commitIDs {
		e.applyCommitted(cid, recovered.committed[cid])
	}
	if recovered.nextID > e.nextTS {
		e.nextTS = recovered.nextID
	}

	// Continue appending after the recovered prefix.
	if _, err := w.f.Seek(0, io.SeekEnd); err != nil {
		_ = w.close()
		return nil, err
	}

	if rerr != nil {
		return e, rerr
	}
	return e, nil
}

// Close flushes and closes the engine. Further calls are idempotent.
func (e *Engine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	w := e.wal
	e.mu.Unlock()
	return w.close()
}

// Begin starts a transaction. A read-only tx takes a snapshot but never logs
// to the WAL; a read-write tx records a TxBegin frame.
func (e *Engine) Begin(readOnly bool) (*Tx, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, ErrEngineClosed
	}

	beginID := e.nextTS
	e.nextTS++

	tx := &Tx{
		engine:   e,
		beginID:  beginID,
		readOnly: readOnly,
		writes:   make(map[string]txWrite),
		done:     false,
	}
	e.active[beginID] = tx

	if !readOnly {
		e.walMu.Lock()
		err := e.wal.append(beginID, recBegin, "", nil)
		e.walMu.Unlock()
		if err != nil {
			delete(e.active, beginID)
			return nil, err
		}
	}
	return tx, nil
}

// visible reports whether a version created at createdTxID (a commit
// timestamp) is visible to a snapshot taken at beginID.
func visible(createdTxID, beginID uint64) bool {
	return createdTxID < beginID
}

// snapshotGet resolves the visible value for key under snapshot beginID.
// Caller must hold e.mu (at least RLock).
func (e *Engine) snapshotGet(key string, beginID uint64) ([]byte, bool) {
	chain := e.versions[key]
	for _, r := range chain {
		// The chain is newest-first. The first version installed before the
		// snapshot is exactly the snapshot's view; newer versions (including
		// newer tombstones) must be skipped entirely, never bypassed.
		if visible(r.createdTxID, beginID) {
			if r.isDeleted {
				return nil, false
			}
			v := make([]byte, len(r.value))
			copy(v, r.value)
			return v, true
		}
	}
	return nil, false
}

// watermarkLocked returns the earliest active begin timestamp, or 0 if none.
// Caller must hold e.mu.
func (e *Engine) watermarkLocked() uint64 {
	var wm uint64
	for id := range e.active {
		if wm == 0 || id < wm {
			wm = id
		}
	}
	return wm
}

// applyCommitted installs one committed transaction's coalesced mutations.
// Each written key gains a single new head version. Caller provides locking
// during recovery (single goroutine); runtime callers hold e.mu.Lock.
func (e *Engine) applyCommitted(commitID uint64, ops []walOp) {
	latest := make(map[string]walOp)
	var order []string
	for _, op := range ops {
		if _, ok := latest[op.key]; !ok {
			order = append(order, op.key)
		}
		latest[op.key] = op
	}
	for _, key := range order {
		op := latest[key]
		chain := e.versions[key]
		if len(chain) > 0 {
			chain[0].deletedTxID = commitID
		}
		r := &record{
			key:         key,
			createdTxID: commitID,
			isDeleted:   op.recType == recDelete,
		}
		if !r.isDeleted {
			r.value = append([]byte(nil), op.value...)
		}
		e.versions[key] = append([]*record{r}, chain...)
	}
}
