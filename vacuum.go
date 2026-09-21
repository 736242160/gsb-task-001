package kvstore

import "sort"

func sortStrings(s []string) { sort.Strings(s) }

// Vacuum reclaims obsolete versions.
//
// Let wm be the earliest active transaction's begin timestamp (0 when no
// transactions are active).
//
// On each key's newest-first version chain it keeps:
//   - every version with createdTxID >= wm (an active snapshot may need it);
//   - the single newest version with createdTxID < wm (the agreed history for
//     all current and future snapshots);
//   - nothing older than that, because it is fully covered by a newer version
//     and cannot be observed by any snapshot that still exists.
//
// When no transaction is active (wm == 0), only the current head survives; a
// lone head that is a soft-deleted tombstone means the key itself is dropped,
// removing dead index entries entirely.
func (e *Engine) Vacuum() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrEngineClosed
	}

	wm := e.watermarkLocked()

	for key, chain := range e.versions {
		// No active transactions: no snapshot can see an old version.
		// Keep only the current head; a tombstone head means the key is gone.
		if wm == 0 {
			if len(chain) == 0 || chain[0].isDeleted {
				delete(e.versions, key)
			} else if len(chain) > 1 {
				e.versions[key] = chain[:1]
			}
			continue
		}

		// Walk newest-first. Keep every version >= wm; among versions below
		// wm keep only the very first (newest) one, which is the agreed view
		// for every snapshot at or after the watermark.
		kept := make([]*record, 0, len(chain))
		keptNewestBelow := false
		for _, r := range chain {
			if r.createdTxID >= wm {
				kept = append(kept, r)
				continue
			}
			if !keptNewestBelow {
				kept = append(kept, r)
				keptNewestBelow = true
			}
		}

		if len(kept) == 0 {
			delete(e.versions, key)
			continue
		}
		if len(kept) != len(chain) {
			e.versions[key] = kept
		}
	}
	return nil
}

// Stats reports lightweight engine counters (mainly for tests/observability).
type Stats struct {
	Keys      int // number of indexed keys
	Versions  int // total stored versions across all keys
	Watermark uint64
	ActiveTx  int
}

// Stats returns a point-in-time snapshot of internal counters.
func (e *Engine) Stats() Stats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	s := Stats{
		Keys:      len(e.versions),
		ActiveTx:  len(e.active),
		Watermark: e.watermarkLocked(),
	}
	for _, chain := range e.versions {
		s.Versions += len(chain)
	}
	return s
}
