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
// as without spill (graceful backpressure). An ENOSPC on a frame of rows is
// treated the same way, after truncating the torn frame; an ENOSPC that
// cannot be undone (opening the file, a failed truncation) or on a
// trailer stops the run with that error. The reserve is checked, not held:
// concurrent writers and the parts being published can use it up first.

var spillCRC = crc32.MakeTable(crc32.Castagnoli)

type spiller[P any] struct {
	codec    schema.Codec[P]
	dir      string
	compress bool
	max      int64 // live spill bytes (0: no cap)
	reserve  int64 // free bytes to leave on the filesystem (0: none)
	live     atomic.Int64
	peak     atomic.Int64
	refused  atomic.Int64 // frames refused by the disk budget
	full     atomic.Bool  // the last frame was refused; cleared when a file is deleted
	wake     func()       // rouses the run's waiters (credit returned); nil in tests
	verify   func() error // Options.SpillVerify; nil in tests
	g        *Gauges      // live bytes and open readers (nil in tests)
	seq      atomic.Uint64
	enc      sync.Pool // *zstd.Encoder
	dec      *zstd.Decoder

	mu   sync.Mutex
	open map[*spillFile]struct{} // files with an open reader, closed by close
	tick uint64                  // reader use order, for eviction (mu)

	rows, frames, bytes, files atomic.Int64
}

func newSpiller[P any](codec schema.Codec[P], parent string, compress bool, max, reserve int64) (*spiller[P], error) {
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return nil, fmt.Errorf("spill: %w", err)
	}
	dir, err := os.MkdirTemp(parent, "spill-")
	if err != nil {
		return nil, fmt.Errorf("spill: %w", err)
	}
	s := &spiller[P]{codec: codec, dir: dir, compress: compress, max: max, reserve: reserve, open: map[*spillFile]struct{}{}}
	if compress {
		// A decoded frame is bounded like a raw one: a damaged or hostile
		// frame cannot claim more memory than any frame written.
		if s.dec, err = zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(maxSpillDecoded)); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// close releases every reader still open (a run that failed downstream
// of a replay leaves one) and removes the spill directory.
func (s *spiller[P]) close() {
	s.mu.Lock()
	for f := range s.open {
		f.close()
	}
	s.open = map[*spillFile]struct{}{}
	s.mu.Unlock()
	if s.dec != nil {
		s.dec.Close()
	}
	if s.check() == nil { // else the pathname names another directory now
		_ = os.RemoveAll(s.dir)
	}
}

// check is the output's identity check (Options.SpillVerify) before a
// spill file is created, appended to or removed: nil without one.
func (s *spiller[P]) check() error {
	if s.verify == nil {
		return nil
	}
	return s.verify()
}

// closeFile closes f's reader and forgets it.
func (s *spiller[P]) closeFile(f *spillFile) {
	s.mu.Lock()
	if _, ok := s.open[f]; ok {
		delete(s.open, f)
		s.readers(-1)
	}
	s.mu.Unlock()
	f.close()
}

// maxReaders caps the open spill readers (one descriptor and a 64 KiB
// buffer each). A deep walk leaves a partly replayed file per spilled
// ancestor; past the cap the least recently used reader is closed and
// reopened at its position when the drainer comes back to it.
const maxReaders = 8

// MaxSpillReaders is maxReaders, for descriptor admission.
const MaxSpillReaders = maxReaders

func (s *spiller[P]) readers(d int64) {
	if s.g != nil {
		s.g.readers.add(d)
	}
}

// track reports the live spill bytes to the gauges.
func (s *spiller[P]) track() {
	if s.g != nil {
		s.g.spill.set(s.live.Load())
	}
}

// openReader opens f's reader at its position (mu not held), first closing
// the least recently used other reader when maxReaders are open.
func (s *spiller[P]) openReader(f *spillFile) error {
	s.mu.Lock()
	if len(s.open) >= maxReaders {
		var lru *spillFile
		for o := range s.open {
			if lru == nil || o.used < lru.used {
				lru = o
			}
		}
		delete(s.open, lru)
		s.readers(-1)
		lru.close()
		lru.r = nil
	}
	s.mu.Unlock()
	fh, err := os.Open(f.path)
	if err != nil {
		return fmt.Errorf("spill: %w", err)
	}
	if f.pos > 0 {
		if _, err := fh.Seek(f.pos, io.SeekStart); err != nil {
			fh.Close()
			return fmt.Errorf("spill: %w", err)
		}
	}
	f.f, f.r = fh, bufio.NewReaderSize(fh, 1<<16)
	s.mu.Lock()
	s.open[f] = struct{}{}
	s.readers(1)
	s.mu.Unlock()
	return nil
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
	pos     int64 // bytes replayed: where a closed reader reopens (drainer only)
	used    uint64
	// most is the row count of the largest frame written: a frame read
	// back that claims more is malformed, refused before any allocation.
	most atomic.Int64
}

func (s *spiller[P]) file() *spillFile {
	s.files.Add(1)
	return &spillFile{path: filepath.Join(s.dir, fmt.Sprintf("%08d.spill", s.seq.Add(1)))}
}

func (s *spiller[P]) frame(typ byte, body []byte) []byte {
	payload := make([]byte, 0, len(body)+1)
	payload = append(append(payload, typ), body...)
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[4:], crc32.Checksum(payload, spillCRC))
	return append(hdr[:], payload...)
}

// write appends one frame of rows (and, when final, the trailer).
func (s *spiller[P]) write(f *spillFile, rows []schema.Row[P], final bool) error {
	var buf []byte
	if len(rows) > 0 {
		if int64(len(rows)) > f.most.Load() {
			f.most.Store(int64(len(rows))) // the owner is the only writer
		}
		body := appendRows(nil, rows, s.codec)
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
	// Each frame opens the file by name: the output must still be the
	// directory the run owns (two stats per frame).
	err := s.check()
	if err == nil {
		var fh *os.File
		if fh, err = os.OpenFile(f.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644); err == nil {
			_, err = fh.Write(buf)
			if err != nil && errors.Is(err, syscall.ENOSPC) && fh.Truncate(f.size) == nil {
				err = errSpillFull // the torn frame is gone: back off as if refused
			}
			if cerr := fh.Close(); err == nil {
				err = cerr
			}
		}
	}
	if err != nil {
		s.live.Add(-n)
		s.track()
		if err == errSpillFull && final {
			// A trailer cannot wait: the range is complete and its file is
			// unreadable without it. The run stops (resumable).
			return fmt.Errorf("spill trailer: %w", syscall.ENOSPC)
		}
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
func (s *spiller[P]) admit(n int64, final bool) bool {
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
	s.track()
	return true
}

// release returns a deleted file's bytes to the budget and wakes every
// waiter: a worker held at admission, or an emitter whose frame was
// refused, may now spill.
func (s *spiller[P]) release(f *spillFile) {
	s.live.Add(-f.size)
	s.track()
	s.full.Store(false)
	if s.wake != nil {
		s.wake()
	}
}

var errSpillFrame = errors.New("spill: corrupt frame")

// maxSpillFrame bounds a frame read back: an emitter batch is far smaller.
const maxSpillFrame = 256 << 20

// maxSpillDecoded bounds a compressed frame's decoded size (a variable for
// tests).
var maxSpillDecoded uint64 = maxSpillFrame

func (f *spillFile) close() {
	if f.f != nil {
		f.f.Close()
		f.f = nil
	}
}

// next reads the next rows frame; at the trailer it checks the count,
// closes and deletes the file and returns nil rows.
func (s *spiller[P]) next(f *spillFile) ([]schema.Row[P], error) {
	if f.r == nil {
		if err := s.openReader(f); err != nil {
			return nil, err
		}
	}
	s.mu.Lock()
	s.tick++
	f.used = s.tick
	s.mu.Unlock()
	var hdr [8]byte
	if _, err := io.ReadFull(f.r, hdr[:]); err != nil {
		s.closeFile(f)
		return nil, fmt.Errorf("spill: %w", err)
	}
	size := binary.LittleEndian.Uint32(hdr[:4])
	if size == 0 || size > maxSpillFrame { // checked before allocating
		s.closeFile(f)
		return nil, errSpillFrame
	}
	payload := make([]byte, size)
	if _, err := io.ReadFull(f.r, payload); err != nil || crc32.Checksum(payload, spillCRC) != binary.LittleEndian.Uint32(hdr[4:]) {
		s.closeFile(f)
		return nil, errSpillFrame
	}
	f.pos += int64(len(hdr)) + int64(size)
	body := payload[1:]
	switch payload[0] {
	case 'T':
		n, k := binary.Uvarint(body)
		s.closeFile(f)
		if k <= 0 || int64(n) != f.read {
			return nil, fmt.Errorf("spill: %s holds %d rows, trailer says %d", f.path, f.read, n)
		}
		err := s.check()
		if err == nil {
			err = os.Remove(f.path)
		}
		s.release(f)
		return nil, err
	case 'Z':
		var err error
		if body, err = s.dec.DecodeAll(body, nil); err != nil {
			s.closeFile(f)
			return nil, errSpillFrame
		}
	case 'R':
	default:
		s.closeFile(f)
		return nil, errSpillFrame
	}
	rows, err := decodeRows(body, s.codec, uint64(f.most.Load()))
	if err != nil {
		s.closeFile(f)
		return nil, err
	}
	f.read += int64(len(rows))
	return rows, nil
}

// appendRows encodes rows for a frame: the count, then per row its key
// and the producer's payload codec. The format is private to one run.
func appendRows[P any](dst []byte, rows []schema.Row[P], c schema.Codec[P]) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(rows)))
	for i := range rows {
		dst = schema.AppendString(dst, rows[i].Key)
		dst = c.Append(dst, &rows[i].Data)
	}
	return dst
}

// decodeRows decodes a frame written by appendRows, of at most most rows
// (the largest frame its file was given): the claimed count is checked
// before the rows are allocated, so a malformed frame cannot allocate past
// what was spilled. The rows' strings view src, which the caller hands
// over (it must not be modified afterwards).
func decodeRows[P any](src []byte, c schema.Codec[P], most uint64) ([]schema.Row[P], error) {
	d := schema.NewDecoder(src)
	n := d.Uvarint()
	if d.Err() != nil || n > uint64(len(src)) || n > most {
		return nil, schema.ErrCodec
	}
	rows := make([]schema.Row[P], n)
	for i := range rows {
		rows[i].Key = d.Str()
		c.Decode(d, &rows[i].Data)
		if err := d.Err(); err != nil {
			return nil, err
		}
	}
	if d.Len() != 0 {
		return nil, schema.ErrCodec
	}
	return rows, nil
}

// Unlimited is what FreeBytes reports when it cannot measure.
const Unlimited = 1<<63 - 1

// FreeBytes is the space available to an unprivileged writer on dir's
// filesystem, or Unlimited when it cannot be measured.
func FreeBytes(dir string) int64 { return freeBytes(dir) }
