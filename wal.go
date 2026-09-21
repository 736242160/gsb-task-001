package mvcckv

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

// walName is the on-disk file used for the write-ahead log.
const walName = "wal.log"

// WAL magic, identifying the self-describing log format: "MVCC".
var walMagic = [4]byte{'M', 'V', 'C', 'C'}

// RecordType tags every WAL frame.
type RecordType uint8

const (
	RecTxBegin    RecordType = 1
	RecSet        RecordType = 2
	RecDelete     RecordType = 3
	RecTxCommit   RecordType = 4
	RecTxRollback RecordType = 5
)

func (r RecordType) String() string {
	switch r {
	case RecTxBegin:
		return "TxBegin"
	case RecSet:
		return "Set"
	case RecDelete:
		return "Delete"
	case RecTxCommit:
		return "TxCommit"
	case RecTxRollback:
		return "TxRollback"
	default:
		return fmt.Sprintf("Unknown(%d)", uint8(r))
	}
}

// Fixed frame header layout (all integers little-endian):
//
//	magic[4] offset 0; crc32[4] offset 4; txID[8] offset 8;
//	recType[1] offset 16; keyLen[4] offset 17; valueLen[4] offset 21
//
// The CRC32 (IEEE) checksum covers every byte from the txID field onward.
const frameHeaderLen = 4 + 4 + 8 + 1 + 4 + 4

// packFrame serializes one WAL record into a self-describing binary frame.
func packFrame(txID uint64, rec RecordType, key string, value []byte) ([]byte, error) {
	if len(key) > 0xffffffff {
		return nil, fmt.Errorf("mvcckv: key too long: %d bytes", len(key))
	}
	if len(value) > 0xffffffff {
		return nil, fmt.Errorf("mvcckv: value too long: %d bytes", len(value))
	}

	frame := make([]byte, frameHeaderLen+len(key)+len(value))
	copy(frame[0:4], walMagic[:])
	binary.LittleEndian.PutUint32(frame[4:8], 0) // CRC placeholder
	binary.LittleEndian.PutUint64(frame[8:16], txID)
	frame[16] = byte(rec)
	binary.LittleEndian.PutUint32(frame[17:21], uint32(len(key)))
	binary.LittleEndian.PutUint32(frame[21:25], uint32(len(value)))
	copy(frame[frameHeaderLen:], key)
	copy(frame[frameHeaderLen+len(key):], value)

	checksum := crc32.ChecksumIEEE(frame[8:])
	binary.LittleEndian.PutUint32(frame[4:8], checksum)
	return frame, nil
}

// appendFrame writes a single frame. Callers must hold walMu (or be in
// single-threaded recovery setup).
func (e *Engine) appendFrame(txID uint64, rec RecordType, key string, value []byte) error {
	frame, err := packFrame(txID, rec, key, value)
	if err != nil {
		return err
	}
	if _, err := e.walFile.Write(frame); err != nil {
		return err
	}
	e.walSize += uint64(len(frame))
	return nil
}

// appendCommitFrameSync writes the TxCommit frame (encoding the commit
// timestamp in its 8-byte value) and fsyncs the WAL. Durability is guaranteed
// before the commit becomes visible in the in-memory index.
func (e *Engine) appendCommitFrameSync(beginID, commitTS uint64) error {
	var value [8]byte
	binary.LittleEndian.PutUint64(value[:], commitTS)
	frame, err := packFrame(beginID, RecTxCommit, "", value[:])
	if err != nil {
		return err
	}
	if _, err := e.walFile.Write(frame); err != nil {
		return err
	}
	if err := e.walFile.Sync(); err != nil {
		return err
	}
	e.walSize += uint64(len(frame))
	return nil
}

// replayResult describes what recovery found in the WAL.
type replayResult struct {
	lastCommitTS uint64 // highest commit timestamp observed
	maxBeginID   uint64 // highest transaction id (begin or commit) observed
	corrupt      bool   // a bad magic/CRC/header was encountered
	lastValidOff int64  // truncation point: end of the last committed prefix
}

var errCorruptWAL = fmt.Errorf("mvcckv: corrupt WAL frame")

// recoverWAL replays the WAL, rebuilding the committed in-memory index.
//
// Only transactions terminated by a TxCommit record are published. Records of
// transactions still in-flight at the tail (Begin with no Commit) are
// discarded. Any corrupt or torn frame at the tail causes the log to be
// truncated back to the end of the last fully committed prefix and a warning
// is logged.
func recoverWAL(dir string, e *Engine) error {
	path := filepath.Join(dir, walName)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	type pendingTx struct {
		writes []writeEntry
	}
	pending := make(map[uint64]*pendingTx)

	res := replayResult{lastValidOff: 0}
	var off int64

	truncateAt := func() error {
		if int64(len(data)) == res.lastValidOff && !res.corrupt {
			return nil
		}
		e.recoveryWarnings = append(e.recoveryWarnings, fmt.Sprintf(
			"WAL recovery: truncating %q at offset %d (log size %d); uncommitted or corrupt tail discarded",
			path, res.lastValidOff, len(data)))
		f, terr := os.OpenFile(path, os.O_RDWR, 0o644)
		if terr != nil {
			return terr
		}
		defer f.Close()
		if terr := f.Truncate(res.lastValidOff); terr != nil {
			return terr
		}
		return f.Sync()
	}

	for off < int64(len(data)) {
		if int64(len(data))-off < frameHeaderLen {
			res.corrupt = true
			break
		}

		header := data[off : off+frameHeaderLen]
		frameOff := off

		if string(header[0:4]) != string(walMagic[:]) {
			res.corrupt = true
			break
		}
		checksum := binary.LittleEndian.Uint32(header[4:8])
		txID := binary.LittleEndian.Uint64(header[8:16])
		rec := RecordType(header[16])
		keyLen := binary.LittleEndian.Uint32(header[17:21])
		valLen := binary.LittleEndian.Uint32(header[21:25])

		frameLen := int64(frameHeaderLen) + int64(keyLen) + int64(valLen)
		if off+frameLen > int64(len(data)) {
			res.corrupt = true
			break
		}
		frame := data[off : off+frameLen]
		if crc32.ChecksumIEEE(frame[8:]) != checksum {
			res.corrupt = true
			break
		}

		key := string(frame[frameHeaderLen : frameHeaderLen+int(keyLen)])
		value := frame[frameHeaderLen+int(keyLen) : frameHeaderLen+int(keyLen)+int(valLen)]
		off += frameLen

		switch rec {
		case RecTxBegin:
			if _, ok := pending[txID]; !ok {
				pending[txID] = &pendingTx{}
			}
			if txID > res.maxBeginID {
				res.maxBeginID = txID
			}

		case RecSet:
			p, ok := pending[txID]
			if !ok {
				res.corrupt = true
				off = frameOff
				goto done
			}
			valCopy := append([]byte(nil), value...)
			p.writes = append(p.writes, writeEntry{key: key, value: valCopy, deleted: false})

		case RecDelete:
			p, ok := pending[txID]
			if !ok {
				res.corrupt = true
				off = frameOff
				goto done
			}
			p.writes = append(p.writes, writeEntry{key: key, deleted: true})

		case RecTxCommit:
			p, ok := pending[txID]
			if !ok || valLen != 8 || keyLen != 0 {
				res.corrupt = true
				off = frameOff
				goto done
			}
			commitTS := binary.LittleEndian.Uint64(value)
			// Apply this transaction in the order it appears in the log.
			for _, w := range p.writes {
				applyWrite(e, w.key, w.value, w.deleted, commitTS)
			}
			delete(pending, txID)
			if commitTS > res.lastCommitTS {
				res.lastCommitTS = commitTS
			}
			if txID > res.maxBeginID {
				res.maxBeginID = txID
			}
			// This frame completes a fully valid, committed prefix.
			res.lastValidOff = off

		case RecTxRollback:
			if _, ok := pending[txID]; !ok {
				res.corrupt = true
				off = frameOff
				goto done
			}
			delete(pending, txID)
			res.lastValidOff = off

		default:
			// Unknown record type means we cannot interpret what follows.
			res.corrupt = true
			off = frameOff
			goto done
		}
	}

done:
	if res.corrupt {
		if err := truncateAt(); err != nil {
			return err
		}
	} else if len(pending) > 0 {
		// Clean shutdown never leaves pending transactions. Drop the dangling
		// tail (Begin/Set/Delete frames without a Commit) and warn.
		if err := truncateAt(); err != nil {
			return err
		}
	}

	e.nextTxID.Store(res.maxBeginID + 1)
	e.committedTS.Store(res.lastCommitTS)
	return nil
}
