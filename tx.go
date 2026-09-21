package kvstore

import (
	"encoding/binary"
)

// Tx is an MVCC transaction with snapshot-isolation semantics.
type Tx struct {
	engine   *Engine
	beginID  uint64
	readOnly bool

	// writes buffers the transaction's mutations until commit.
	writes map[string]txWrite

	done bool
}

// Get returns the value visible under this transaction's snapshot.
// The transaction's own buffered writes take precedence (read-your-writes).
func (t *Tx) Get(key string) ([]byte, error) {
	e := t.engine

	e.mu.RLock()
	if e.closed {
		e.mu.RUnlock()
		return nil, ErrEngineClosed
	}
	if t.done {
		e.mu.RUnlock()
		return nil, ErrTxClosed
	}
	if w, ok := t.writes[key]; ok {
		e.mu.RUnlock()
		if w.isDelete {
			return nil, ErrKeyNotFound
		}
		v := make([]byte, len(w.value))
		copy(v, w.value)
		return v, nil
	}
	v, ok := e.snapshotGet(key, t.beginID)
	e.mu.RUnlock()
	if !ok {
		return nil, ErrKeyNotFound
	}
	return v, nil
}

// Set buffers a put. It is durable only after a successful Commit.
func (t *Tx) Set(key string, value []byte) error {
	if err := t.checkWrite(); err != nil {
		return err
	}
	cp := make([]byte, len(value))
	copy(cp, value)
	t.writes[key] = txWrite{isDelete: false, value: cp}

	e := t.engine
	e.walMu.Lock()
	err := e.wal.append(t.beginID, recSet, key, cp)
	e.walMu.Unlock()
	if err != nil {
		delete(t.writes, key)
		return err
	}
	return nil
}

// Delete buffers a soft delete (tombstone) for key.
func (t *Tx) Delete(key string) error {
	if err := t.checkWrite(); err != nil {
		return err
	}
	t.writes[key] = txWrite{isDelete: true}

	e := t.engine
	e.walMu.Lock()
	err := e.wal.append(t.beginID, recDelete, key, nil)
	e.walMu.Unlock()
	if err != nil {
		delete(t.writes, key)
		return err
	}
	return nil
}

func (t *Tx) checkWrite() error {
	e := t.engine
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return ErrEngineClosed
	}
	if t.done {
		return ErrTxClosed
	}
	if t.readOnly {
		return ErrReadOnlyTx
	}
	return nil
}

// Commit makes buffered writes globally visible.
//
// Ordering is strictly: WAL TxCommit frame + fsync first, then publication
// into the in-memory index. Write-write conflicts use first-committer-wins:
// if any touched key gained a committed version at or after this snapshot,
// ErrWriteConflict is returned and the transaction is auto-rolled-back.
func (t *Tx) Commit() error {
	e := t.engine

	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return ErrEngineClosed
	}
	if t.done {
		e.mu.Unlock()
		return ErrTxClosed
	}

	if t.readOnly {
		t.done = true
		delete(e.active, t.beginID)
		e.mu.Unlock()
		return nil
	}

	// Conflict detection: a head version installed at or after this snapshot
	// means another concurrent writer committed first.
	for key := range t.writes {
		chain := e.versions[key]
		if len(chain) > 0 && chain[0].createdTxID >= t.beginID {
			t.done = true
			delete(e.active, t.beginID)
			idBuf := make([]byte, 8)
			binary.LittleEndian.PutUint64(idBuf, t.beginID)
			e.walMu.Lock()
			_ = e.wal.append(t.beginID, recRollback, "", idBuf)
			e.walMu.Unlock()
			e.mu.Unlock()
			return ErrWriteConflict
		}
	}

	commitID := e.nextTS
	e.nextTS++

	// Durability barrier. The engine lock stays held across the fsync and the
	// publication, so commit-timestamp allocation, WAL order, and global
	// visibility form one atomic first-committer-wins point: no other commit
	// can publish before this one is durable.
	idBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(idBuf, t.beginID)
	e.walMu.Lock()
	werr := e.wal.append(commitID, recCommit, "", idBuf)
	e.walMu.Unlock()
	if werr != nil {
		t.done = true
		delete(e.active, t.beginID)
		e.mu.Unlock()
		return werr
	}

	t.done = true
	delete(e.active, t.beginID)

	keys := make([]string, 0, len(t.writes))
	for key := range t.writes {
		keys = append(keys, key)
	}
	// Deterministic order is not required for correctness, but keeps chains tidy.
	sortStrings(keys)
	for _, key := range keys {
		w := t.writes[key]
		chain := e.versions[key]
		if len(chain) > 0 {
			chain[0].deletedTxID = commitID
		}
		r := &record{
			key:         key,
			createdTxID: commitID,
			isDeleted:   w.isDelete,
		}
		if !w.isDelete {
			r.value = append([]byte(nil), w.value...)
		}
		e.versions[key] = append([]*record{r}, chain...)
	}
	e.mu.Unlock()
	return nil
}

// Rollback discards all buffered writes and removes the active transaction.
func (t *Tx) Rollback() error {
	e := t.engine
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return ErrEngineClosed
	}
	if t.done {
		e.mu.Unlock()
		return ErrTxClosed
	}
	t.done = true
	readOnly := t.readOnly
	delete(e.active, t.beginID)
	t.writes = nil
	e.mu.Unlock()

	if !readOnly {
		e.logRollback(t.beginID)
	}
	return nil
}

func (e *Engine) logRollback(beginID uint64) {
	idBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(idBuf, beginID)
	e.walMu.Lock()
	_ = e.wal.append(beginID, recRollback, "", idBuf)
	e.walMu.Unlock()
}
