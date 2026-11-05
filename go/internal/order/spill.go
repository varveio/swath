// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/klauspost/compress/zstd"

	"github.com/varveio/swath/go/internal/schema"
)

// Spill keeps workers listing when the reorder budget is used up: instead
// of waiting for the drainer, an emitter whose range is not the one being
// drained writes its rows to the range's spill file and leaves a marker in
// the range's item list. The rows are already in key order, so the drainer
// replays the file front to back when it reaches the markers: no index, no
// merge. A range's file is deleted once drained; the run's spill directory
// is removed when Run returns, on success or failure.
//
// A file is a sequence of frames: a 4-byte little-endian payload length, a
// CRC-32C of the payload, and the payload, whose first byte is its type:
// 'R' rows, 'Z' zstd-compressed rows (level 1), 'T' the trailer (row count),
// written when the range ends and checked after replay.
//
// Disk budget. Spill never fills the disk: a frame is written only if the
// live spill bytes stay within Max and the spill filesystem keeps Reserve
// bytes free afterwards (room for the parts still to be published, which
// share the filesystem when the spill directory is under the output). A
// refused frame is not an error: the emitter waits on the reorder budget,
// as without spill (graceful backpressure). An ENOSPC on a spill write is
// treated the same way, after truncating the torn frame.

var spillCRC = crc32.MakeTable(crc32.Castagnoli)

type spiller struct {
	dir      string
	compress bool
	max      int64 // live spill bytes (0: no cap)
	reserve  int64 // free bytes to leave on the filesystem (0: none)
	live     atomic.Int64
	peak     atomic.Int64
	refused  atomic.Int64 // frames refused by the disk budget
	full     atomic.Bool  // the last frame was refused; cleared when a file is deleted
	seq      atomic.Uint64
	enc      sync.Pool // *zstd.Encoder
	dec      *zstd.Decoder

	rows, frames, bytes, files atomic.Int64
}

func newSpiller(parent string, compress bool, max, reserve int64) (*spiller, error) {
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return nil, fmt.Errorf("spill: %w", err)
	}
	dir, err := os.MkdirTemp(parent, "spill-")
	if err != nil {
		return nil, fmt.Errorf("spill: %w", err)
	}
	s := &spiller{dir: dir, compress: compress, max: max, reserve: reserve}
	if compress {
		if s.dec, err = zstd.NewReader(nil, zstd.WithDecoderConcurrency(1)); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *spiller) close() {
	if s.dec != nil {
		s.dec.Close()
	}
	_ = os.RemoveAll(s.dir)
}

// spillFile is one range's file. Its owner (the range's emitter) appends;
// the drainer reads after the owner has written the frames it reaches.
type spillFile struct {
	path    string
	written int64 // rows written (owner only)
	size    int64 // bytes written (owner only, then the drainer once complete)
	r       *bufio.Reader
	f       *os.File
	read    int64 // rows replayed (drainer only)
}

func (s *spiller) file() *spillFile {
	s.files.Add(1)
	return &spillFile{path: filepath.Join(s.dir, fmt.Sprintf("%08d.spill", s.seq.Add(1)))}
}

func (s *spiller) frame(typ byte, body []byte) []byte {
	payload := make([]byte, 0, len(body)+1)
	payload = append(append(payload, typ), body...)
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[4:], crc32.Checksum(payload, spillCRC))
	return append(hdr[:], payload...)
}

// write appends one frame of rows (and, when final, the trailer).
func (s *spiller) write(f *spillFile, rows []schema.Row, final bool) error {
	var buf []byte
	if len(rows) > 0 {
		body := schema.AppendRows(nil, rows)
		typ := byte('R')
		if s.compress {
			enc, _ := s.enc.Get().(*zstd.Encoder)
			if enc == nil {
				var err error
				if enc, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1)); err != nil {
					return err
				}
			}
			body = enc.EncodeAll(body, nil)
			s.enc.Put(enc)
			typ = 'Z'
		}
		buf = s.frame(typ, body)
	}
	if final {
		buf = append(buf, s.frame('T', binary.AppendUvarint(nil, uint64(f.written)))...)
	}
	if len(buf) == 0 {
		return nil
	}
	n := int64(len(buf))
	if !s.admit(n, final) {
		return errSpillFull
	}
	fh, err := os.OpenFile(f.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err == nil {
		_, err = fh.Write(buf)
		if err != nil && errors.Is(err, syscall.ENOSPC) && fh.Truncate(f.size) == nil {
			err = errSpillFull // the torn frame is gone: back off as if refused
		}
		if cerr := fh.Close(); err == nil {
			err = cerr
		}
	}
	if err != nil {
		s.live.Add(-n)
		if err == errSpillFull {
			s.refused.Add(1)
			s.full.Store(true)
			return err
		}
		return fmt.Errorf("spill: %w", err)
	}
	f.size += n
	f.written += int64(len(rows))
	if len(rows) > 0 {
		s.rows.Add(int64(len(rows)))
		s.frames.Add(1)
	}
	s.bytes.Add(n)
	return nil
}

// errSpillFull is a refusal by the disk budget, not a failure.
var errSpillFull = errors.New("spill: disk budget used")

// admit reserves n live bytes, or refuses the frame. A trailer is always
// admitted: it is a few bytes, and without it the range's file cannot be
// replayed. Concurrent emitters may each pass the free-space check before
// either has written; the overshoot is at most one frame per worker.
func (s *spiller) admit(n int64, final bool) bool {
	live := s.live.Add(n)
	if !final {
		if (s.max > 0 && live > s.max) || (s.reserve > 0 && freeBytes(s.dir) < n+s.reserve) {
			s.live.Add(-n)
			s.refused.Add(1)
			s.full.Store(true)
			return false
		}
	}
	for p := s.peak.Load(); live > p && !s.peak.CompareAndSwap(p, live); p = s.peak.Load() {
	}
	return true
}

// release returns a deleted file's bytes to the budget.
func (s *spiller) release(f *spillFile) {
	s.live.Add(-f.size)
	s.full.Store(false)
}

var errSpillFrame = errors.New("spill: corrupt frame")

// maxSpillFrame bounds a frame read back: an emitter batch is far smaller.
const maxSpillFrame = 256 << 20

func (f *spillFile) close() {
	if f.f != nil {
		f.f.Close()
		f.f = nil
	}
}

// next reads the next rows frame; at the trailer it checks the count,
// closes and deletes the file and returns nil rows.
func (s *spiller) next(f *spillFile) ([]schema.Row, error) {
	if f.r == nil {
		fh, err := os.Open(f.path)
		if err != nil {
			return nil, fmt.Errorf("spill: %w", err)
		}
		f.f, f.r = fh, bufio.NewReaderSize(fh, 1<<16)
	}
	var hdr [8]byte
	if _, err := io.ReadFull(f.r, hdr[:]); err != nil {
		f.close()
		return nil, fmt.Errorf("spill: %w", err)
	}
	size := binary.LittleEndian.Uint32(hdr[:4])
	if size == 0 || size > maxSpillFrame { // checked before allocating
		f.close()
		return nil, errSpillFrame
	}
	payload := make([]byte, size)
	if _, err := io.ReadFull(f.r, payload); err != nil || crc32.Checksum(payload, spillCRC) != binary.LittleEndian.Uint32(hdr[4:]) {
		f.close()
		return nil, errSpillFrame
	}
	body := payload[1:]
	switch payload[0] {
	case 'T':
		n, k := binary.Uvarint(body)
		f.close()
		if k <= 0 || int64(n) != f.read {
			return nil, fmt.Errorf("spill: %s holds %d rows, trailer says %d", f.path, f.read, n)
		}
		err := os.Remove(f.path)
		s.release(f)
		return nil, err
	case 'Z':
		var err error
		if body, err = s.dec.DecodeAll(body, nil); err != nil {
			f.close()
			return nil, errSpillFrame
		}
	case 'R':
	default:
		f.close()
		return nil, errSpillFrame
	}
	rows, err := schema.DecodeRows(body)
	if err != nil {
		f.close()
		return nil, err
	}
	f.read += int64(len(rows))
	return rows, nil
}
