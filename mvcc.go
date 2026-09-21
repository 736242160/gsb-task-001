package mvcckv

// Vacuum garbage-collects obsolete versions.
//
// A version is reclaimable when no active (or future) transaction can ever
// observe it as the newest visible version. The active-transaction watermark
// is the smallest snapshot id among in-flight transactions:
//
//   - every version with createdTxID >= watermark is retained, since it may be
//     the newest visible version for some active snapshot;
//   - the watermark snapshot itself is fully served by a retained version at
//     createdTxID == watermark; if no such version exists, the single newest
//     non-tombstone version below the watermark is retained for it;
//   - every other older version (superseded values and tombstones) is
//     reclaimed; if no visible version remains, the key index is removed.
func (e *Engine) Vacuum() error {
	e.lifecycleMu.RLock()
	closed := e.closed.Load()
	e.lifecycleMu.RUnlock()
	if closed {
		return ErrEngineClosed
	}

	e.indexMu.Lock()
	defer e.indexMu.Unlock()

	wm := e.watermarkLocked()

	for key, chain := range e.data {
		kept := make([]*version, 0, len(chain))
		haveAtWatermark := false
		var newestBelow *version // newest version older than the watermark

		for _, v := range chain { // newest first
			if v.createdTxID >= wm {
				kept = append(kept, v)
				if v.createdTxID == wm {
					haveAtWatermark = true
				}
				continue
			}
			if newestBelow == nil {
				newestBelow = v
			}
		}

		// When no retained version sits exactly at the watermark, the
		// watermark snapshot reads the newest older version -- but only if it
		// is a live value (a tombstone means the key is deleted for it).
		if !haveAtWatermark && newestBelow != nil && !newestBelow.isDeleted {
			kept = append(kept, newestBelow)
		}

		if len(kept) == 0 {
			delete(e.data, key)
		} else {
			e.data[key] = kept
		}
	}

	return nil
}
