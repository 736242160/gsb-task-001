package mvcckv

import "sync"

// Tx is a single transaction. A transaction must not be used concurrently
// from multiple goroutines, but distinct transactions (including read-only
// snapshots) may run concurrently with writers.
type Tx struct {
	engine *Engine

	beginID  uint64 // unique id assigned at Begin (also used as WAL tx id)
	snapshot uint64 // snapshot id: last commit visible to this transaction
	readOnly bool

	mu       sync.Mutex
	finished bool

	// writes retains mutation order; touched is the set of mutated keys.
	writes  []writeEntry
	touched map[string]struct{}
}

// Begin starts a new transaction. Read-only transactions take a snapshot of
// the latest committed state; writable transactions additionally log a
// TxBegin record to the WAL.
func (e *Engine) Begin(readOnly bool) (*Tx, error) {
	e.lifecycleMu.RLock()
	defer e.lifecycleMu.RUnlock()
	if e.closed.Load() {
		return nil, ErrEngineClosed
	}

	beginID := e.nextTxID.Add(1)
	snapshot := e.committedTS.Load()

	e.indexMu.Lock()
	e.active[beginID] = snapshot
	e.indexMu.Unlock()

	tx := &Tx{
		engine:   e,
		beginID:  beginID,
		snapshot: snapshot,
		readOnly: readOnly,
		touched:  make(map[string]struct{}),
	}

	if !readOnly {
		e.walMu.Lock()
		err := e.appendFrame(beginID, RecTxBegin, "", nil)
		e.walMu.Unlock()
		if err != nil {
			e.removeActive(beginID)
			return nil, err
		}
	}

	return tx, nil
}

func (e *Engine) removeActive(beginID uint64) {
	e.indexMu.Lock()
	delete(e.active, beginID)
	e.indexMu.Unlock()
}

// Get returns the value of key as visible from the transaction's snapshot, or
// ErrKeyNotFound when the key does not exist or is deleted. Values returned
// belong to the caller and may be modified freely.
func (tx *Tx) Get(key string) ([]byte, error) {
	tx.mu.Lock()
	if tx.finished {
		tx.mu.Unlock()
		return nil, ErrTxClosed
	}
	// Newest local write wins within the transaction.
	for i := len(tx.writes) - 1; i >= 0; i-- {
		w := tx.writes[i]
		if w.key == key {
			tx.mu.Unlock()
			if w.deleted {
				return nil, ErrKeyNotFound
			}
			return append([]byte(nil), w.value...), nil
		}
	}
	tx.mu.Unlock()

	tx.engine.indexMu.RLock()
	defer tx.engine.indexMu.RUnlock()

	for _, v := range tx.engine.data[key] {
		if visibleLocked(v.createdTxID, tx.snapshot) {
			if v.isDeleted {
				return nil, ErrKeyNotFound
			}
			return append([]byte(nil), v.value...), nil
		}
	}
	return nil, ErrKeyNotFound
}

// Set stages a key/value write in the transaction and appends the Set record
// to the WAL (it only becomes durable and visible on Commit).
func (tx *Tx) Set(key string, value []byte) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.finished {
		return ErrTxClosed
	}
	if tx.readOnly {
		return ErrReadOnly
	}

	valueCopy := append([]byte(nil), value...)

	tx.engine.walMu.Lock()
	err := tx.engine.appendFrame(tx.beginID, RecSet, key, valueCopy)
	tx.engine.walMu.Unlock()
	if err != nil {
		tx.finished = true
		tx.engine.removeActive(tx.beginID)
		return err
	}

	tx.writes = append(tx.writes, writeEntry{key: key, value: valueCopy})
	tx.touched[key] = struct{}{}
	return nil
}

// Delete stages a tombstone for key and appends the Delete record to the WAL.
func (tx *Tx) Delete(key string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.finished {
		return ErrTxClosed
	}
	if tx.readOnly {
		return ErrReadOnly
	}

	tx.engine.walMu.Lock()
	err := tx.engine.appendFrame(tx.beginID, RecDelete, key, nil)
	tx.engine.walMu.Unlock()
	if err != nil {
		tx.finished = true
		tx.engine.removeActive(tx.beginID)
		return err
	}

	tx.writes = append(tx.writes, writeEntry{key: key, deleted: true})
	tx.touched[key] = struct{}{}
	return nil
}

// Commit atomically publishes the transaction. It first detects first-committer
// wins write conflicts, then fsyncs the WAL and only afterwards installs the
// new versions into the visible in-memory index. On conflict the transaction
// is automatically rolled back and ErrWriteConflict is returned.
func (tx *Tx) Commit() error {
	tx.mu.Lock()
	if tx.finished {
		tx.mu.Unlock()
		return ErrTxClosed
	}
	tx.finished = true
	tx.mu.Unlock()

	// The transaction leaves the active set no matter how commit ends.
	defer tx.engine.removeActive(tx.beginID)

	if len(tx.writes) == 0 {
		return nil
	}

	tx.engine.commitMu.Lock()
	defer tx.engine.commitMu.Unlock()

	// Write-conflict detection: a concurrent transaction already committed a
	// newer version of a key this transaction mutates.
	tx.engine.indexMu.RLock()
	for k := range tx.touched {
		chain := tx.engine.data[k]
		if len(chain) > 0 && chain[0].createdTxID > tx.snapshot {
			tx.engine.indexMu.RUnlock()
			return ErrWriteConflict
		}
	}
	tx.engine.indexMu.RUnlock()

	// Assign the commit timestamp, persist the commit boundary (this also
	// flushes all preceding Begin/Set/Delete frames), and only then publish.
	commitTS := tx.engine.nextTxID.Add(1)
	tx.engine.walMu.Lock()
	err := tx.engine.appendCommitFrameSync(tx.beginID, commitTS)
	tx.engine.walMu.Unlock()
	if err != nil {
		return err
	}

	tx.engine.indexMu.Lock()
	for _, w := range tx.writes {
		applyWrite(tx.engine, w.key, w.value, w.deleted, commitTS)
	}
	tx.engine.committedTS.Store(commitTS)
	tx.engine.indexMu.Unlock()

	return nil
}

// Rollback aborts the transaction. Its staged writes never become visible; a
// TxRollback marker is best-effort appended to the WAL.
func (tx *Tx) Rollback() error {
	tx.mu.Lock()
	if tx.finished {
		tx.mu.Unlock()
		return ErrTxClosed
	}
	tx.finished = true
	tx.mu.Unlock()

	tx.engine.removeActive(tx.beginID)

	if !tx.readOnly {
		tx.engine.walMu.Lock()
		_ = tx.engine.appendFrame(tx.beginID, RecTxRollback, "", nil)
		tx.engine.walMu.Unlock()
	}
	return nil
}
