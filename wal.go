package kvstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// WAL record types.
const (
	recBegin    byte = 1
	recSet      byte = 2
	recDelete   byte = 3
	recCommit   byte = 4
	recRollback byte = 5
)

// walMagic prefixes every WAL frame (ASCII "KVWL").
const walMagic uint32 = 0x4B56574C

const (
	walHeaderLen = 4 + 4 + 4 // magic + crc32 + payloadLen
	walMaxKey    = 1 << 20   // 1 MiB
	walMaxValue  = 1 << 30   // 1 GiB
)

// wal handles append/fsync and sequential replay of the write-ahead log.
type wal struct {
	f    *os.File
	path string
	sync bool
}

func openWAL(dir string, doSync bool) (*wal, error) {
	path := dir + string(os.PathSeparator) + walFileName
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	return &wal{f: f, path: path, sync: doSync}, nil
}

// append encodes one record as a self-describing frame and appends it.
// Frame:   magic(4) | crc32(4) | payloadLen(4) | payload
// Payload: txID(8) | recType(1) | keyLen(4) key | valLen(4) value
func (w *wal) append(txID uint64, recType byte, key string, value []byte) error {
	if len(key) > walMaxKey || len(value) > walMaxValue {
		return errors.New("kvstore: key/value too large")
	}
	payload := make([]byte, 9+4+len(key)+4+len(value))
	binary.LittleEndian.PutUint64(payload[0:8], txID)
	payload[8] = recType
	off := 9
	binary.LittleEndian.PutUint32(payload[off:off+4], uint32(len(key)))
	off += 4
	copy(payload[off:], key)
	off += len(key)
	binary.LittleEndian.PutUint32(payload[off:off+4], uint32(len(value)))
	off += 4
	copy(payload[off:], value)

	frame := make([]byte, walHeaderLen+len(payload))
	binary.LittleEndian.PutUint32(frame[0:4], walMagic)
	binary.LittleEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
	binary.LittleEndian.PutUint32(frame[8:12], uint32(len(payload)))
	copy(frame[walHeaderLen:], payload)

	if _, err := w.f.Write(frame); err != nil {
		return err
	}
	if w.sync {
		return w.f.Sync()
	}
	return nil
}

func (w *wal) close() error {
	if w.f == nil {
		return nil
	}
	if w.sync {
		_ = w.f.Sync()
	}
	err := w.f.Close()
	w.f = nil
	return err
}

// walOp is a single buffered write operation during replay.
type walOp struct {
	recType byte
	key     string
	value   []byte
}

// recoveryResult holds the outcome of a WAL replay.
type recoveryResult struct {
	committed map[uint64][]walOp // commitID -> committed write ops
	nextID    uint64             // highest ID seen + 1
}

type frame struct {
	txID    uint64
	recType byte
	key     string
	value   []byte
}

// readFrame decodes one frame sequentially from r.
// io.EOF means a clean end of file; errTruncated means a torn tail frame.
func readFrame(r io.Reader) (frame, int64, error) {
	var consumed int64
	header := make([]byte, walHeaderLen)
	if _, err := io.ReadFull(r, header); err != nil {
		if errors.Is(err, io.EOF) {
			return frame{}, consumed, io.EOF
		}
		return frame{}, consumed, errTruncated
	}
	consumed += walHeaderLen

	magic := binary.LittleEndian.Uint32(header[0:4])
	crc := binary.LittleEndian.Uint32(header[4:8])
	payloadLen := binary.LittleEndian.Uint32(header[8:12])
	if magic != walMagic || payloadLen < 9 {
		return frame{}, consumed, errCorrupt
	}
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return frame{}, consumed, errTruncated
	}
	consumed += int64(payloadLen)

	if crc32.ChecksumIEEE(payload) != crc {
		return frame{}, consumed, errCorrupt
	}

	txID := binary.LittleEndian.Uint64(payload[0:8])
	recType := payload[8]
	if recType < recBegin || recType > recRollback {
		return frame{}, consumed, errCorrupt
	}
	off := 9
	if off+4 > len(payload) {
		return frame{}, consumed, errCorrupt
	}
	keyLen := int(binary.LittleEndian.Uint32(payload[off : off+4]))
	off += 4
	if off+keyLen > len(payload) {
		return frame{}, consumed, errCorrupt
	}
	key := string(payload[off : off+keyLen])
	off += keyLen
	if off+4 > len(payload) {
		return frame{}, consumed, errCorrupt
	}
	valLen := int(binary.LittleEndian.Uint32(payload[off : off+4]))
	off += 4
	if off+valLen != len(payload) {
		return frame{}, consumed, errCorrupt
	}
	value := make([]byte, valLen)
	copy(value, payload[off:off+valLen])

	return frame{txID: txID, recType: recType, key: key, value: value}, consumed, nil
}

// recoverWAL replays the WAL sequentially and rebuilds committed state.
// goodEnd is the offset after the last complete valid frame;
// commitEnd is the offset after the last valid commit/rollback point.
// Pending transactions (Begin without Commit) are discarded; a corrupt or
// torn tail truncates the file back to commitEnd and the wrapped warning is
// returned so callers can surface it.
func recoverWAL(path string) (recoveryResult, int64, int64, error) {
	res := recoveryResult{committed: make(map[uint64][]walOp), nextID: 1}
	var goodEnd, commitEnd int64

	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return res, 0, 0, nil
		}
		return res, 0, 0, err
	}
	defer f.Close()

	pending := make(map[uint64][]walOp) // begin_tx_id -> ops
	finished := make(map[uint64]bool)
	var warning error

	for {
		fr, n, rerr := readFrame(f)
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				break
			}
			// Torn/corrupt tail: stop and truncate to the last commit point.
			warning = fmt.Errorf("%w: %v", ErrWALCorrupt, rerr)
			break
		}
		goodEnd += n

		switch fr.recType {
		case recBegin:
			if finished[fr.txID] {
				warning = fmt.Errorf("%w: duplicate begin tx %d", ErrWALCorrupt, fr.txID)
				return abortRecovery(path, commitEnd, goodEnd-n, res, warning)
			}
			if _, ok := pending[fr.txID]; ok {
				warning = fmt.Errorf("%w: duplicate begin tx %d", ErrWALCorrupt, fr.txID)
				return abortRecovery(path, commitEnd, goodEnd-n, res, warning)
			}
			pending[fr.txID] = nil
		case recSet, recDelete:
			ops, ok := pending[fr.txID]
			if !ok || finished[fr.txID] {
				warning = fmt.Errorf("%w: write for unknown tx %d", ErrWALCorrupt, fr.txID)
				return abortRecovery(path, commitEnd, goodEnd-n, res, warning)
			}
			pending[fr.txID] = append(ops, walOp{recType: fr.recType, key: fr.key, value: fr.value})
		case recCommit:
			// Commit frame: txID holds commitID, 8-byte value holds beginID.
			if len(fr.value) != 8 {
				warning = fmt.Errorf("%w: malformed commit frame", ErrWALCorrupt)
				return abortRecovery(path, commitEnd, goodEnd-n, res, warning)
			}
			beginID := binary.LittleEndian.Uint64(fr.value)
			ops, ok := pending[beginID]
			if !ok || finished[beginID] {
				warning = fmt.Errorf("%w: commit for unknown tx %d", ErrWALCorrupt, beginID)
				return abortRecovery(path, commitEnd, goodEnd-n, res, warning)
			}
			res.committed[fr.txID] = ops
			delete(pending, beginID)
			finished[beginID] = true
			commitEnd = goodEnd
		case recRollback:
			if len(fr.value) != 8 {
				warning = fmt.Errorf("%w: malformed rollback frame", ErrWALCorrupt)
				return abortRecovery(path, commitEnd, goodEnd-n, res, warning)
			}
			beginID := binary.LittleEndian.Uint64(fr.value)
			if _, ok := pending[beginID]; !ok {
				warning = fmt.Errorf("%w: rollback for unknown tx %d", ErrWALCorrupt, beginID)
				return abortRecovery(path, commitEnd, goodEnd-n, res, warning)
			}
			delete(pending, beginID)
			finished[beginID] = true
			commitEnd = goodEnd
		}

		// Only Begin and Commit frames consume monotonic timestamps; the
		// txID of Set/Delete/Rollback frames is a begin ID and is already
		// covered by its corresponding Begin frame.
		if fr.recType == recBegin || fr.recType == recCommit {
			if fr.txID >= res.nextID {
				res.nextID = fr.txID + 1
			}
		}
	}

	if warning != nil {
		return abortRecovery(path, commitEnd, goodEnd, res, warning)
	}
	// Pending transactions at the tail: discard and truncate to commit point.
	if len(pending) > 0 && goodEnd != commitEnd {
		if err := os.Truncate(path, commitEnd); err != nil {
			return res, goodEnd, commitEnd, err
		}
	}
	return res, goodEnd, commitEnd, nil
}

func abortRecovery(path string, commitEnd, goodEnd int64, res recoveryResult, warning error) (recoveryResult, int64, int64, error) {
	if err := os.Truncate(path, commitEnd); err != nil {
		return res, goodEnd, commitEnd, err
	}
	return res, goodEnd, commitEnd, warning
}
