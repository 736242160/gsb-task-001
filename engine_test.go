package mvcckv

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func mustCommit(t *testing.T, tx *Tx) {
	t.Helper()
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func mustBegin(t *testing.T, e *Engine, readOnly bool) *Tx {
	t.Helper()
	tx, err := e.Begin(readOnly)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	return tx
}

func check(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func mustGet(t *testing.T, e *Engine, key, want string) {
	t.Helper()
	tx, err := e.Begin(true)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback()
	got, err := tx.Get(key)
	if err != nil {
		t.Fatalf("get %q: %v", key, err)
	}
	if string(got) != want {
		t.Fatalf("get %q = %q, want %q", key, got, want)
	}
}

func mustGetNotFound(t *testing.T, e *Engine, key string) {
	t.Helper()
	tx, err := e.Begin(true)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback()
	if _, err := tx.Get(key); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("get %q err = %v, want ErrKeyNotFound", key, err)
	}
}

func chainLen(e *Engine, key string) int {
	e.indexMu.RLock()
	defer e.indexMu.RUnlock()
	return len(e.data[key])
}

func frameOrDie(t *testing.T, txID uint64, rec RecordType, key string, value []byte) []byte {
	t.Helper()
	f, err := packFrame(txID, rec, key, value)
	if err != nil {
		t.Fatalf("pack: %v", err)
	}
	return f
}

func readFile(t *testing.T, p string) []byte {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read %s: %v", p, err)
	}
	return b
}

func TestSnapshotIsolation(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer e.Close()

	// Initial committed state: a=v1, k=old.
	tx := mustBegin(t, e, false)
	check(t, tx.Set("a", []byte("v1")))
	check(t, tx.Set("k", []byte("old")))
	mustCommit(t, tx)

	// Long-lived read transaction takes a snapshot.
	reader := mustBegin(t, e, true)

	if got, err := reader.Get("a"); err != nil || string(got) != "v1" {
		t.Fatalf("reader get a = %q, %v", got, err)
	}

	// A concurrent writer updates both keys and commits.
	w := mustBegin(t, e, false)
	check(t, w.Set("a", []byte("v2")))
	check(t, w.Set("k", []byte("new")))
	mustCommit(t, w)

	// The reader must still observe its original snapshot.
	if got, err := reader.Get("a"); err != nil || string(got) != "v1" {
		t.Fatalf("snapshot reader get a = %q, %v; want v1", got, err)
	}
	if got, err := reader.Get("k"); err != nil || string(got) != "old" {
		t.Fatalf("snapshot reader get k = %q, %v; want old", got, err)
	}
	check(t, reader.Rollback())

	// A fresh snapshot sees the newly committed values.
	mustGet(t, e, "a", "v2")
	mustGet(t, e, "k", "new")

	// Snapshot readers must not observe in-flight (uncommitted) writes.
	rtx := mustBegin(t, e, true)
	open := mustBegin(t, e, false)
	check(t, open.Set("z", []byte("hidden")))
	if _, err := rtx.Get("z"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("reader saw uncommitted key z: %v", err)
	}
	check(t, open.Rollback())
	check(t, rtx.Rollback())
}

func TestWriteConflict(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer e.Close()

	tx := mustBegin(t, e, false)
	check(t, tx.Set("k", []byte("v0")))
	mustCommit(t, tx)

	// Two concurrent writers mutate the same key.
	t1 := mustBegin(t, e, false)
	t2 := mustBegin(t, e, false)
	check(t, t1.Set("k", []byte("v1")))
	check(t, t2.Set("k", []byte("v2")))

	mustCommit(t, t1) // first committer wins
	err = t2.Commit()
	if !errors.Is(err, ErrWriteConflict) {
		t.Fatalf("t2 commit err = %v, want ErrWriteConflict", err)
	}
	// The losing transaction is finished; further use must fail.
	if err := t2.Set("x", []byte("y")); !errors.Is(err, ErrTxClosed) {
		t.Fatalf("set after conflict err = %v, want ErrTxClosed", err)
	}

	mustGet(t, e, "k", "v1")

	// Delete-vs-Set on the same key is also a conflict.
	d := mustBegin(t, e, false)
	s := mustBegin(t, e, false)
	check(t, d.Delete("k"))
	check(t, s.Set("k", []byte("v3")))
	mustCommit(t, d)
	if err := s.Commit(); !errors.Is(err, ErrWriteConflict) {
		t.Fatalf("set/delete conflict err = %v, want ErrWriteConflict", err)
	}
	mustGetNotFound(t, e, "k")

	// Disjoint keys never conflict, even when interleaved.
	a := mustBegin(t, e, false)
	b := mustBegin(t, e, false)
	check(t, a.Set("p", []byte("1")))
	check(t, b.Set("q", []byte("2")))
	mustCommit(t, a)
	mustCommit(t, b)
	mustGet(t, e, "p", "1")
	mustGet(t, e, "q", "2")
}

func TestCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Fully committed transactions.
	tx := mustBegin(t, e, false)
	check(t, tx.Set("committed", []byte("yes")))
	check(t, tx.Set("willdelete", []byte("bye")))
	mustCommit(t, tx)

	tx2 := mustBegin(t, e, false)
	check(t, tx2.Delete("willdelete"))
	mustCommit(t, tx2)

	// Simulate a crash: abandon the engine without Close and append an
	// incomplete (Begin/Set without Commit) transaction tail to the WAL.
	// Closing first mimics the process having died while leaving the on-disk
	// log containing an unresolved transaction.
	check(t, e.Close())
	walPath := filepath.Join(dir, walName)
	dirty := append(frameOrDie(t, 1001, RecTxBegin, "", nil),
		frameOrDie(t, 1001, RecSet, "uncommitted", []byte("nope"))...)
	wal, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	if _, err := wal.Write(dirty); err != nil {
		t.Fatalf("write dirty tail: %v", err)
	}
	check(t, wal.Sync())
	check(t, wal.Close())

	// Reopen: the dirty tail is discarded, committed data retained.
	e2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}

	mustGet(t, e2, "committed", "yes")
	mustGetNotFound(t, e2, "willdelete")
	mustGetNotFound(t, e2, "uncommitted")

	// New writes work and survive another reopen after a clean close.
	tx3 := mustBegin(t, e2, false)
	check(t, tx3.Set("after", []byte("ok")))
	mustCommit(t, tx3)
	check(t, e2.Close())

	e3, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen 2: %v", err)
	}
	defer e3.Close()
	mustGet(t, e3, "after", "ok")
	mustGet(t, e3, "committed", "yes")
}

func TestCorruptWALRecovery(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	tx := mustBegin(t, e, false)
	check(t, tx.Set("good", []byte("data")))
	mustCommit(t, tx)
	check(t, e.Close())

	// Append a torn frame (truncated payload) simulating a crash mid-write.
	walPath := filepath.Join(dir, walName)
	frame := frameOrDie(t, 1002, RecTxBegin, "", nil)
	frame = append(frame, frameOrDie(t, 1002, RecSet, "bad", []byte("x"))...)
	corrupt := append(readFile(t, walPath), frame[:len(frame)-3]...)
	if err := os.WriteFile(walPath, corrupt, 0o644); err != nil {
		t.Fatalf("corrupt wal: %v", err)
	}

	e2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen after corruption: %v", err)
	}
	defer e2.Close()

	if w := e2.RecoveryWarnings(); len(w) == 0 {
		t.Fatalf("expected a truncation recovery warning, got none")
	} else {
		t.Logf("recovery warning: %s", w[0])
	}
	mustGet(t, e2, "good", "data")
	mustGetNotFound(t, e2, "bad")

	// The log was truncated, so future appends are consistent.
	tx2 := mustBegin(t, e2, false)
	check(t, tx2.Set("more", []byte("z")))
	mustCommit(t, tx2)
	check(t, e2.Close())

	e3, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer e3.Close()
	mustGet(t, e3, "more", "z")
	mustGet(t, e3, "good", "data")
}

func TestVacuum(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer e.Close()

	// Produce three committed versions of "k".
	for _, v := range []string{"v1", "v2", "v3"} {
		tx := mustBegin(t, e, false)
		check(t, tx.Set("k", []byte(v)))
		mustCommit(t, tx)
	}
	if n := chainLen(e, "k"); n != 3 {
		t.Fatalf("chain len = %d, want 3", n)
	}

	// No active transaction: vacuum reclaims all but the newest live version.
	check(t, e.Vacuum())
	if n := chainLen(e, "k"); n != 1 {
		t.Fatalf("after vacuum chain len = %d, want 1", n)
	}
	mustGet(t, e, "k", "v3")

	// Take the snapshot before v4/v5 commit; it stays frozen at v3.
	old := mustBegin(t, e, true)

	for _, v := range []string{"v4", "v5"} {
		tx := mustBegin(t, e, false)
		check(t, tx.Set("k", []byte(v)))
		mustCommit(t, tx)
	}
	if got, err := old.Get("k"); err != nil || string(got) != "v3" {
		t.Fatalf("snapshot read = %q, %v; want v3", got, err)
	}
	check(t, e.Vacuum())
	// A version exists exactly at the watermark (v3, id 7); together with the
	// two newer versions the chain has 3 entries, nothing older is pinned.
	if n := chainLen(e, "k"); n != 3 {
		t.Fatalf("with active snapshot chain len = %d, want 3", n)
	}
	check(t, old.Rollback())

	// Deleting a key and vacuuming removes tombstone and index entry.
	d := mustBegin(t, e, false)
	check(t, d.Delete("k"))
	mustCommit(t, d)
	check(t, e.Vacuum())
	e.indexMu.RLock()
	_, present := e.data["k"]
	e.indexMu.RUnlock()
	if present {
		t.Fatalf("vacuum did not remove the fully-deleted key index")
	}
	mustGetNotFound(t, e, "k")
}

func TestConcurrentAccess(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer e.Close()

	var wg sync.WaitGroup
	keys := []string{"a", "b", "c", "d"}

	// Writers: disjoint keys always commit; conflicts are tolerated.
	for wi := 0; wi < 8; wi++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 30; i++ {
				k := keys[id%len(keys)]
				tx, err := e.Begin(false)
				if err != nil {
					t.Errorf("begin: %v", err)
					return
				}
				if err := tx.Set(k, []byte("x")); err != nil {
					t.Errorf("set: %v", err)
					return
				}
				if err := tx.Commit(); err != nil && !errors.Is(err, ErrWriteConflict) {
					t.Errorf("commit: %v", err)
					return
				}
			}
		}(wi)
	}

	// Concurrent snapshot readers.
	for ri := 0; ri < 4; ri++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 30; i++ {
				tx, err := e.Begin(true)
				if err != nil {
					t.Errorf("begin ro: %v", err)
					return
				}
				_, _ = tx.Get(keys[i%len(keys)])
				if err := tx.Rollback(); err != nil {
					t.Errorf("rollback: %v", err)
					return
				}
			}
		}()
	}

	// Periodic vacuum.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			_ = e.Vacuum()
		}
	}()

	wg.Wait()
}
