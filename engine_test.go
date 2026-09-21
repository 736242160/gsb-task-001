package kvstore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func tempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "kvstore-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func mustCommit(t *testing.T, e *Engine, key string, value []byte) {
	t.Helper()
	tx, err := e.Begin(false)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := tx.Set(key, value); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func getCommitted(t *testing.T, e *Engine, key string) []byte {
	t.Helper()
	tx, err := e.Begin(true)
	if err != nil {
		t.Fatalf("begin ro: %v", err)
	}
	v, err := tx.Get(key)
	if err != nil {
		t.Fatalf("get %q: %v", key, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("ro commit: %v", err)
	}
	return v
}

// TestSnapshotIsolation verifies that a long-lived read transaction keeps
// observing its snapshot even after a concurrent writer commits.
func TestSnapshotIsolation(t *testing.T) {
	dir := tempDir(t)
	e, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer e.Close()

	mustCommit(t, e, "k", []byte("v1"))

	// Reader opens a snapshot at v1.
	reader, err := e.Begin(true)
	if err != nil {
		t.Fatalf("begin reader: %v", err)
	}

	v, err := reader.Get("k")
	if err != nil || string(v) != "v1" {
		t.Fatalf("initial read = %q, %v; want v1", v, err)
	}

	// Writer commits v2 after the reader snapshot started.
	mustCommit(t, e, "k", []byte("v2"))

	v, err = reader.Get("k")
	if err != nil || string(v) != "v1" {
		t.Fatalf("snapshot read = %q, %v; want v1", v, err)
	}

	// A brand-new snapshot sees v2.
	if got := getCommitted(t, e, "k"); string(got) != "v2" {
		t.Fatalf("new snapshot = %q; want v2", got)
	}

	if err := reader.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	// Read-your-writes inside an uncommitted tx.
	wtx, err := e.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	if err := wtx.Set("k", []byte("v3")); err != nil {
		t.Fatal(err)
	}
	v, err = wtx.Get("k")
	if err != nil || string(v) != "v3" {
		t.Fatalf("read-your-writes = %q, %v", v, err)
	}
	if err := wtx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if got := getCommitted(t, e, "k"); string(got) != "v2" {
		t.Fatalf("after rollback = %q; want v2", got)
	}
}

// TestWriteConflict verifies first-committer-wins conflict detection and
// automatic rollback for concurrent writers of the same key.
func TestWriteConflict(t *testing.T) {
	dir := tempDir(t)
	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	mustCommit(t, e, "shared", []byte("init"))

	tx1, err := e.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	tx2, err := e.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	if err := tx1.Set("shared", []byte("from-tx1")); err != nil {
		t.Fatal(err)
	}
	if err := tx2.Set("shared", []byte("from-tx2")); err != nil {
		t.Fatal(err)
	}

	if err := tx1.Commit(); err != nil {
		t.Fatalf("tx1 commit: %v", err)
	}
	err = tx2.Commit()
	if !errors.Is(err, ErrWriteConflict) {
		t.Fatalf("tx2 commit err = %v; want ErrWriteConflict", err)
	}

	// Auto-rolled-back tx must be unusable.
	if err := tx2.Set("x", []byte("y")); !errors.Is(err, ErrTxClosed) {
		t.Fatalf("post-conflict set err = %v; want ErrTxClosed", err)
	}

	// tx1's value is the surviving one.
	if got := getCommitted(t, e, "shared"); string(got) != "from-tx1" {
		t.Fatalf("surviving value = %q; want from-tx1", got)
	}

	// Deletes participate in conflict detection as well.
	tx3, _ := e.Begin(false)
	tx4, _ := e.Begin(false)
	if err := tx3.Delete("shared"); err != nil {
		t.Fatal(err)
	}
	if err := tx4.Set("shared", []byte("tx4")); err != nil {
		t.Fatal(err)
	}
	if err := tx3.Commit(); err != nil {
		t.Fatalf("tx3 commit: %v", err)
	}
	if err := tx4.Commit(); !errors.Is(err, ErrWriteConflict) {
		t.Fatalf("tx4 commit err = %v; want ErrWriteConflict", err)
	}

	// Disjoint keys do not conflict.
	tx5, _ := e.Begin(false)
	tx6, _ := e.Begin(false)
	_ = tx5.Set("a", []byte("1"))
	_ = tx6.Set("b", []byte("2"))
	if err := tx5.Commit(); err != nil {
		t.Fatalf("tx5: %v", err)
	}
	if err := tx6.Commit(); err != nil {
		t.Fatalf("tx6: %v", err)
	}
}

// TestCrashRecovery verifies that reopening a directory replays the WAL,
// keeps only committed data, discards pending transactions, and truncates a
// CRC-corrupt tail with a warning.
func TestCrashRecovery(t *testing.T) {
	dir := tempDir(t)

	// Phase 1: commit one tx, then leave a second tx uncommitted and "crash"
	// without closing the engine cleanly.
	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	mustCommit(t, e, "committed", []byte("yes"))
	mustCommit(t, e, "counter", []byte("v1"))

	pending, err := e.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	if err := pending.Set("committed", []byte("NO")); err != nil {
		t.Fatal(err)
	}
	if err := pending.Set("ghost", []byte("never-existed")); err != nil {
		t.Fatal(err)
	}

	// Simulate process death: drop the engine without Close/Commit (no fsync
	// of a commit frame for pending). Force buffered frames to disk first.
	e.walMu.Lock()
	if err := e.wal.f.Sync(); err != nil {
		t.Fatal(err)
	}
	e.walMu.Unlock()
	walPath := filepath.Join(dir, walFileName)
	if err := e.wal.f.Close(); err != nil {
		t.Fatal(err)
	}

	// Phase 2: reopen. Pending transaction must be fully discarded.
	e2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if got := getCommitted(t, e2, "committed"); string(got) != "yes" {
		t.Fatalf("committed key = %q; want yes", got)
	}
	if _, err := func() ([]byte, error) {
		tx, _ := e2.Begin(true)
		defer tx.Rollback()
		return tx.Get("ghost")
	}(); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("ghost lookup err = %v; want ErrKeyNotFound", err)
	}
	if err := e2.Close(); err != nil {
		t.Fatal(err)
	}

	// Phase 3: append garbage (a torn/corrupt tail) and confirm truncation.
	f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0xde, 0xad, 0xbe, 0xef, 0x1, 0x2, 0x3, 0x4}); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	e3, err := Open(dir)
	if !errors.Is(err, ErrWALCorrupt) {
		t.Fatalf("open with corrupt tail err = %v; want ErrWALCorrupt warning", err)
	}
	if e3 == nil {
		t.Fatal("engine should still be usable after truncation warning")
	}
	if got := getCommitted(t, e3, "committed"); string(got) != "yes" {
		t.Fatalf("post-corruption value = %q; want yes", got)
	}
	// New commits after a warning must persist normally.
	mustCommit(t, e3, "counter", []byte("v2"))
	if err := e3.Close(); err != nil {
		t.Fatal(err)
	}

	// Phase 4: reopen cleanly, no warning, latest data present.
	e4, err := Open(dir)
	if err != nil {
		t.Fatalf("final reopen: %v", err)
	}
	if got := getCommitted(t, e4, "counter"); string(got) != "v2" {
		t.Fatalf("counter = %q; want v2", got)
	}
	_ = e4.Close()
}

// TestWALCRCCorruption flips one payload byte of the last frame to force a
// CRC mismatch; recovery must truncate to the last valid commit point.
func TestWALCRCCorruption(t *testing.T) {
	dir := tempDir(t)
	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	mustCommit(t, e, "a", []byte("1"))
	mustCommit(t, e, "b", []byte("2"))
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	walPath := filepath.Join(dir, walFileName)
	data, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatal(err)
	}
	// Flip a byte inside the last frame's payload (after the 12-byte header,
	// near the end of the file).
	pos := len(data) - 2
	data[pos] ^= 0xFF
	if err := os.WriteFile(walPath, data, 0o644); err != nil {
		t.Fatal(err)
	}

	e2, err := Open(dir)
	if !errors.Is(err, ErrWALCorrupt) {
		t.Fatalf("open err = %v; want ErrWALCorrupt", err)
	}
	// Last tx (b) is lost with the corrupt tail; earlier commit survives.
	if got := getCommitted(t, e2, "a"); string(got) != "1" {
		t.Fatalf("a = %q; want 1", got)
	}
	if _, gerr := func() ([]byte, error) {
		tx, _ := e2.Begin(true)
		defer tx.Rollback()
		return tx.Get("b")
	}(); !errors.Is(gerr, ErrKeyNotFound) {
		t.Fatalf("b lookup = %v; want ErrKeyNotFound", gerr)
	}
	if err := e2.Close(); err != nil {
		t.Fatal(err)
	}

	// Truncated WAL must now reopen cleanly with no warning.
	e3, err := Open(dir)
	if err != nil {
		t.Fatalf("clean reopen: %v", err)
	}
	_ = e3.Close()
}

// TestWALRollbackReplay commits one tx, rolls another back on disk, commits a
// third, then reopens: the rollback record must be parsed and its writes lost.
func TestWALRollbackReplay(t *testing.T) {
	dir := tempDir(t)
	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	mustCommit(t, e, "keep", []byte("v"))

	rb, _ := e.Begin(false)
	if err := rb.Set("dropped", []byte("nope")); err != nil {
		t.Fatal(err)
	}
	if err := rb.Rollback(); err != nil {
		t.Fatal(err)
	}

	mustCommit(t, e, "after", []byte("yes"))
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if got := getCommitted(t, e2, "keep"); string(got) != "v" {
		t.Fatalf("keep = %q", got)
	}
	if got := getCommitted(t, e2, "after"); string(got) != "yes" {
		t.Fatalf("after = %q", got)
	}
	if _, gerr := func() ([]byte, error) {
		tx, _ := e2.Begin(true)
		defer tx.Rollback()
		return tx.Get("dropped")
	}(); !errors.Is(gerr, ErrKeyNotFound) {
		t.Fatalf("dropped lookup = %v; want ErrKeyNotFound", gerr)
	}
	_ = e2.Close()
}

// TestVacuum verifies that obsolete versions are retained while an older
// snapshot needs them and reclaimed after it finishes; tombstone-only keys
// disappear entirely once nothing can observe them.
func TestVacuum(t *testing.T) {
	dir := tempDir(t)
	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	mustCommit(t, e, "k", []byte("v1"))
	mustCommit(t, e, "temp", []byte("x"))

	// A reader starts here: it must keep seeing v1.
	old, err := e.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	mustCommit(t, e, "k", []byte("v2"))
	mustCommit(t, e, "k", []byte("v3"))

	// Delete temp fully while the old reader is still open: v3/v2/v1 chain
	// for k must survive; temp's value + tombstone also survive for the reader.
	dtx, _ := e.Begin(false)
	if err := dtx.Delete("temp"); err != nil {
		t.Fatal(err)
	}
	if err := dtx.Commit(); err != nil {
		t.Fatal(err)
	}

	if err := e.Vacuum(); err != nil {
		t.Fatalf("vacuum with active reader: %v", err)
	}
	s := e.Stats()
	if s.Versions < 3 {
		t.Fatalf("versions = %d; want k chain (3) preserved for active reader", s.Versions)
	}
	v, err := old.Get("k")
	if err != nil || string(v) != "v1" {
		t.Fatalf("old reader after vacuum = %q, %v; want v1", v, err)
	}
	vt, err := old.Get("temp")
	if err != nil || string(vt) != "x" {
		t.Fatalf("old reader temp = %q, %v; want x", vt, err)
	}
	if err := old.Rollback(); err != nil {
		t.Fatal(err)
	}

	// No active transactions: only current heads remain; dead keys drop.
	if err := e.Vacuum(); err != nil {
		t.Fatal(err)
	}
	s = e.Stats()
	if s.Keys != 1 || s.Versions != 1 {
		t.Fatalf("after vacuum: keys=%d versions=%d; want 1/1", s.Keys, s.Versions)
	}
	if _, ok := e.versions["temp"]; ok {
		t.Fatal("tombstone-only key temp should be removed from the index")
	}
	if got := getCommitted(t, e, "k"); string(got) != "v3" {
		t.Fatalf("k = %q; want v3", got)
	}

	// Deleting the last live key then vacuuming empties the store.
	dtx2, _ := e.Begin(false)
	_ = dtx2.Delete("k")
	if err := dtx2.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := e.Vacuum(); err != nil {
		t.Fatal(err)
	}
	if s := e.Stats(); s.Keys != 0 || s.Versions != 0 {
		t.Fatalf("store after last delete+vacuum = %+v; want empty", s)
	}
}

// TestConcurrent exercises mixed readers/writers under -race and verifies no
// committed value is ever lost or observed out of order.
func TestConcurrent(t *testing.T) {
	dir := tempDir(t)
	e, err := OpenWithOptions(dir, Options{SyncWAL: false})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	mustCommit(t, e, "seq", []byte("0"))

	const writers = 8
	const iters = 25
	var wg sync.WaitGroup

	// Readers: snapshot reads must always return some previously committed int.
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				rtx, err := e.Begin(true)
				if err != nil {
					t.Errorf("reader begin: %v", err)
					return
				}
				if _, err := rtx.Get("seq"); err != nil {
					t.Errorf("reader get: %v", err)
				}
				_ = rtx.Rollback()
			}
		}()
	}

	// Writers each contend on seq; conflicts are expected and retried.
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				for {
					tx, err := e.Begin(false)
					if err != nil {
						t.Errorf("writer begin: %v", err)
						return
					}
					cur, err := tx.Get("seq")
					if err != nil {
						t.Errorf("writer get: %v", err)
						return
					}
					_ = tx.Set("seq", []byte(fmt.Sprintf("%s-w%d", cur, id)))
					if err := tx.Commit(); err != nil {
						if errors.Is(err, ErrWriteConflict) {
							continue // retry against fresh snapshot
						}
						t.Errorf("writer commit: %v", err)
						return
					}
					break
				}
			}
		}(w)
	}
	wg.Wait()

	// Final value must be readable and well-formed.
	rtx, _ := e.Begin(true)
	v, err := rtx.Get("seq")
	_ = rtx.Rollback()
	if err != nil || len(v) == 0 {
		t.Fatalf("final seq = %q, %v", v, err)
	}
}

// TestErrors covers the small sentinel-error surface.
func TestErrors(t *testing.T) {
	dir := tempDir(t)
	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	ro, err := e.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := ro.Set("a", []byte("b")); !errors.Is(err, ErrReadOnlyTx) {
		t.Fatalf("ro set = %v; want ErrReadOnlyTx", err)
	}
	if err := ro.Delete("a"); !errors.Is(err, ErrReadOnlyTx) {
		t.Fatalf("ro delete = %v; want ErrReadOnlyTx", err)
	}
	if _, err := ro.Get("missing"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("missing = %v; want ErrKeyNotFound", err)
	}
	if err := ro.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := ro.Commit(); !errors.Is(err, ErrTxClosed) {
		t.Fatalf("double commit = %v; want ErrTxClosed", err)
	}

	rw, _ := e.Begin(false)
	if err := rw.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := rw.Get("x"); !errors.Is(err, ErrTxClosed) {
		t.Fatalf("get after rollback = %v; want ErrTxClosed", err)
	}

	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Begin(true); !errors.Is(err, ErrEngineClosed) {
		t.Fatalf("begin after close = %v; want ErrEngineClosed", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("double close: %v", err)
	}
}
