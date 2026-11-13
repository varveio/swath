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
	"math/bits"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/varveio/swath/internal/faults"
	"github.com/varveio/swath/internal/schema"
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
// CRC-32C of the payload, and the payload, whose first byte is its type
// (the frame* constants): rows, delta-coded or plain, each optionally
// zstd-compressed (level 1), or the trailer (row count), written when the
// range ends and checked after replay. A run writes one rows type and its
// reader refuses every other, so a frame of an unknown or foreign format
// is never decoded.
//
// A delta frame (appendDelta) codes each key against the previous key in
// the frame (shared-prefix length, then the suffix) and each payload against
// the previous payload through the producer's schema.DeltaCodec (S3), or
// its stateless Codec (the other producers). The previous row is the
// frame's own: it resets at every frame, so a frame decodes alone, without
// the file before it (what replay after reopening a reader, and a durable
// spill, need). Decoded rows own their storage (an owning decoder, bounded
// by the frame's declared storage), so the read buffers are reused.
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

// spillEncoderOptions configure the pooled frame encoders. A frame is
// compressed alone (EncodeAll), so a window longer than a frame buys
// nothing. The library's default 8 MiB window keeps about 8.6 MiB of history
// per pooled encoder once it has coded a frame of 256 KiB or more, and a run
// pools up to one encoder per worker. A 1 MiB window with the lower-memory
// mode keeps about 1.5 MiB, at the same ratio and speed on spill frames
// (measured on synthetic key/ETag bodies of 256 KiB and 2 MiB: ratio 4.77 and
// 4.64 either way). Text output uses the same window (textout.zstdWindow).
var spillEncoderOptions = []zstd.EOption{
	zstd.WithEncoderLevel(zstd.SpeedFastest),
	zstd.WithEncoderConcurrency(1),
	zstd.WithWindowSize(1 << 20),
	zstd.WithLowerEncoderMem(true),
}

// Frame types: the payload's first byte.
const (
	frameRows      = 'R' // plain rows (appendRows): --spill-plain --no-spill-compress
	frameRowsZstd  = 'Z' // plain rows, zstd: --spill-plain
	frameDelta     = 'D' // delta rows (appendDelta): --no-spill-compress
	frameDeltaZstd = 'Y' // delta rows, zstd: the CLI's default
	frameTrailer   = 'T' // the row count, at the range's end
)

// A delta frame's payload coding (its header's third field): a run writes
// the one its producer has, and its reader refuses the other.
const (
	payloadStateless  = 0 // schema.Codec
	payloadContextual = 1 // schema.DeltaCodec
)

type spiller[P any] struct {
	schema   *schema.Schema[P]
	codec    schema.Codec[P]
	dc       schema.DeltaCodec[P] // the producer's contextual codec, or nil
	dir      string
	delta    bool // delta frames (else plain: --spill-plain)
	compress bool
	max      int64 // live spill bytes (0: no cap)
	reserve  int64 // free bytes to leave on the filesystem (0: none)
	live     atomic.Int64
	peak     atomic.Int64
	refused  atomic.Int64 // frames refused by the disk budget
	full     atomic.Bool  // the last frame was refused; cleared when a file is deleted
	wake     func()       // rouses the run's waiters (credit returned); nil in tests
	verify   func() error // Options.SpillVerify; nil in tests
	spilling func()       // Options.Spilling; nil in tests
	g        *Gauges      // live bytes and open readers (nil in tests)
	seq      atomic.Uint64
	enc      sync.Pool // *zstd.Encoder
	bufs     sync.Pool // *[]byte: writers' frame and body buffers
	dec      *zstd.Decoder
	// rbuf and zbuf are the drainer's read and decode buffers, reused from
	// frame to frame for delta frames (their rows own their storage); a
	// plain frame's rows view its buffers, so each gets fresh ones.
	rbuf, zbuf []byte

	mu   sync.Mutex
	open map[*spillFile]struct{} // files with an open reader, closed by close
	tick uint64                  // reader use order, for eviction (mu)
	// h is every read's descriptor, one per file (spillhandles.go).
	h *handles
	// frameBytes and frameRows are the rows frames written, by size.
	frameBytes, frameRows sizeHist

	rows, frames, bytes, files atomic.Int64
	// opens counts the opens of spill files attempted (a failed one
	// included): each frame written (an append), a reader, a frame read at
	// an offset, a trailer read, a seal and a recovered certificate's
	// replay. A resume's validation of the recorded certificates, before
	// the run, is not counted.
	opens         atomic.Int64
	logical       atomic.Int64 // schema.LogicalBytes of the rows written
	decodeRefused atomic.Int64 // frames refused at read for decoding past their bound
	decodeNs      atomic.Int64 // the drainer's own frame decoding (next)

	// cache, if set, is the attempt's durable spill (durable.go): a range
	// whose provider declared its span gets a durable file when a slot is
	// free. codecID names the row codec in a durable file's header.
	cache   *Cache
	codecID string
	// rdec decodes zstd frames of a recovery replay when this run does not
	// compress (made on first need).
	rdec *zstd.Decoder
}

func newSpiller[P any](sch *schema.Schema[P], parent string, plain, compress bool, max, reserve int64) (*spiller[P], error) {
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return nil, fmt.Errorf("spill: %w", err)
	}
	dir, err := os.MkdirTemp(parent, "spill-")
	if err != nil {
		return nil, fmt.Errorf("spill: %w", err)
	}
	s := &spiller[P]{schema: sch, codec: sch.Codec, dir: dir, delta: !plain, compress: compress, max: max, reserve: reserve, open: map[*spillFile]struct{}{}}
	s.dc, _ = sch.Codec.(schema.DeltaCodec[P])
	s.h = newHandles(s.readers)
	if compress {
		// A decoded frame is bounded like a raw one: a damaged or hostile
		// frame cannot claim more memory than any frame written.
		// One decoder for the drainer and one per spare processor, the
		// most concurrent reads (the read-ahead's or the segments'):
		// DecodeAll blocks past the concurrency. One alone on one
		// processor.
		if s.dec, err = zstd.NewReader(nil, zstd.WithDecoderConcurrency(1+spareProcs()), zstd.WithDecoderMaxMemory(maxSpillDecoded)); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// format names the frames the run writes, for the report.
func (s *spiller[P]) format() string {
	f := "plain"
	if s.delta {
		f = "delta"
	}
	if s.compress {
		f += "+zstd"
	}
	return f
}

// rowsType is the type of the rows frames the run writes and reads.
func (s *spiller[P]) rowsType() byte {
	switch {
	case s.delta && s.compress:
		return frameDeltaZstd
	case s.delta:
		return frameDelta
	case s.compress:
		return frameRowsZstd
	}
	return frameRows
}

// close releases every reader still open (a run that failed downstream
// of a replay leaves one) and removes the spill directory.
func (s *spiller[P]) close() {
	s.closeReaders()
	s.h.stop()
	if s.dec != nil {
		s.dec.Close()
	}
	if s.rdec != nil {
		s.rdec.Close()
	}
	if s.check() == nil { // else the pathname names another directory now
		_ = os.RemoveAll(s.dir)
	}
}

// closeReaders closes every reader still open (a failed run's, or the
// drainer's when the segments take over: they read on their own
// descriptors, so the spill descriptors admitted are theirs).
func (s *spiller[P]) closeReaders() {
	s.mu.Lock()
	for f := range s.open {
		s.unread(f)
	}
	s.open = map[*spillFile]struct{}{}
	s.mu.Unlock()
}

// check is the output's identity check (Options.SpillVerify) before a
// spill file is created, appended to or removed: nil without one. A failed
// check retires every cached read handle (handles.stop); the failure
// itself is what stops the run.
func (s *spiller[P]) check() error {
	if s.verify == nil {
		return nil
	}
	err := s.verify()
	if err != nil {
		s.h.stop()
	}
	return err
}

// closeFile closes f's reader and forgets it.
func (s *spiller[P]) closeFile(f *spillFile) {
	s.mu.Lock()
	delete(s.open, f)
	s.mu.Unlock()
	s.unread(f)
}

// unread drops f's sequential reader and releases its lease on f's handle.
func (s *spiller[P]) unread(f *spillFile) {
	if f.h != nil {
		s.h.release(f.h)
		f.h = nil
	}
	f.r = nil
}

// maxReaders caps the drainer's sequential spill readers (a lease on the
// file's handle and a 64 KiB buffer each). A deep walk leaves a partly
// replayed file per spilled ancestor; past the cap the least recently used
// reader is dropped and reopened at its position when the drainer comes
// back to it (its handle may still be open: spillhandles.go).
const maxReaders = 8

// MaxSpillReaders is the spill descriptors open at once, for descriptor
// admission: the drainer's readers and one per read-ahead read (readAt);
// the read handle cache keeps no more open (maxHandles).
const MaxSpillReaders = maxReaders + aheadReads

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

// openReader opens f's reader at its position (mu not held), on a lease
// of f's read handle (ReadAt from the position: the handle may serve other
// reads at once), first dropping the least recently used other reader when
// maxReaders are open.
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
		s.unread(lru)
	}
	s.mu.Unlock()
	h, err := s.lease(f)
	if err != nil {
		return fmt.Errorf("spill: %w", err)
	}
	f.h, f.r = h, bufio.NewReaderSize(&atReader{fh: h.fh, off: f.pos}, 1<<16)
	s.mu.Lock()
	s.open[f] = struct{}{}
	s.mu.Unlock()
	return nil
}

// atReader reads a file sequentially from off with ReadAt, so it shares
// the descriptor with reads at other offsets. Like a file's Read, and
// unlike io.SectionReader, it reports a short read as no error: the
// frontier's file grows while the drainer reads it, and bufio keeps an
// error, so an io.EOF beside the bytes before the end would hide the frames
// appended after them.
type atReader struct {
	fh  *os.File
	off int64
}

func (r *atReader) Read(p []byte) (int, error) {
	n, err := r.fh.ReadAt(p, r.off)
	r.off += int64(n)
	if n > 0 && err == io.EOF {
		err = nil
	}
	return n, err
}

// spillFile is one range's file. Its owner (the range's emitter) appends;
// the drainer reads after the owner has written the frames it reaches.
type spillFile struct {
	path    string
	written int64 // rows written (owner only)
	size    int64 // bytes written (owner only, then the drainer once complete)
	r       *bufio.Reader
	h       *handle // r's lease on the file's read handle
	drained bool    // its read handle retired for good (handles.mu)
	read    int64   // rows replayed (drainer only)
	pos     int64   // bytes replayed: where a closed reader reopens (drainer only)
	used    uint64
	// lastOff and lastSize locate the last rows frame written, lastRows
	// and lastLogical its rows and their logical bytes (owner only): its
	// marker records them, so the frame can be read, and the stream cut
	// into parts, on its own. trailerAt is where the trailer starts, once
	// written (0 before).
	lastOff, lastSize, lastRows, lastLogical, trailerAt int64
	// most is the row count of the largest frame written, and mostOwned
	// the largest storage a delta frame written declared: a frame read
	// back that claims more is malformed, refused before any allocation.
	most, mostOwned atomic.Int64

	// A durable file (d set): its version-2 frames are written through the
	// output root, crc is the CRC-32C of every byte written (owner only),
	// capturing is set from its C frame until the seal or an abandon.
	d                    *Cert
	crc                  uint32
	capturing, abandoned bool
	captureAt            int64 // the C frame's offset
}

func (s *spiller[P]) file() *spillFile {
	s.files.Add(1)
	return &spillFile{path: filepath.Join(s.dir, fmt.Sprintf("%08d.spill", s.seq.Add(1)))}
}

// durableFile is a new durable file for a range of logical bounds (lo,
// hi], or nil when no slot is free (the range spills disposably, as
// without durable spill): the slot and its history are taken without a
// wait.
func (s *spiller[P]) durableFile(lo, hi string, open bool) *spillFile {
	if s.cache == nil || !s.cache.reserve() {
		return nil
	}
	s.files.Add(1)
	id := randomID()
	return &spillFile{d: &Cert{id: id, name: fileName(id), lo: lo, hi: hi, open: open, slot: true}}
}

// capture writes f's C frame, cs: the frames after it are its
// certificate's rows. It is always admitted (a few bytes).
func (s *spiller[P]) capture(f *spillFile, cs string) error {
	off := f.size
	if err := s.writeFrames(f, nil, frame2(nil, frameCapture, schema.AppendString(nil, cs)), false); err != nil {
		return err
	}
	f.capturing, f.captureAt = true, off
	f.d.cs = cs
	return nil
}

// seal makes a durable file's data durable (its trailer written): the
// flusher syncs its name with the directory before the record.
func (s *spiller[P]) seal(f *spillFile) error {
	err := faults.At("spill-seal")
	if err == nil {
		err = s.check()
	}
	var fh *os.File
	if err == nil {
		s.opens.Add(1)
		fh, err = s.cache.root.OpenFile(s.cache.rel(f.d.name), os.O_WRONLY, 0)
	}
	if err == nil {
		err = errors.Join(faults.Sync(fh), fh.Close())
	}
	if err != nil {
		return fmt.Errorf("spill seal: %w", err)
	}
	f.d.bytes, f.d.crc = f.size, f.crc
	return nil
}

func (s *spiller[P]) frame(typ byte, body []byte) []byte {
	payload := make([]byte, 0, len(body)+1)
	payload = append(append(payload, typ), body...)
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[4:], crc32.Checksum(payload, spillCRC))
	return append(hdr[:], payload...)
}

// durableWritePoint is the crash point before a durable file's write, by
// what the write carries: the header (the first write, with the capture
// start when the capture starts at the range's start, and the first
// rows), the trailer and tail (the final one), or later frames (rows, or
// a capture start armed at a later fence).
func durableWritePoint(first, final bool) string {
	switch {
	case first:
		return "spill-write-header"
	case final:
		return "spill-write-trailer"
	}
	return "spill-write-frames"
}

// write appends one frame of rows (and, when final, the trailer).
func (s *spiller[P]) write(f *spillFile, rows []schema.Row[P], final bool) error {
	return s.writeFrames(f, rows, nil, final)
}

// writeFrames appends, for a durable file's first write, its header; then
// meta (whole frames, a capture start), the frame of rows and, when final,
// the trailer (with a durable file's tail).
func (s *spiller[P]) writeFrames(f *spillFile, rows []schema.Row[P], meta []byte, final bool) error {
	bp := s.buffer()
	defer s.bufs.Put(bp)
	buf := (*bp)[:0]
	if f.d != nil && f.size == 0 {
		buf = frame2(buf, frameHeader, s.cache.headerBody(f.d, s.codecID))
	}
	buf = append(buf, meta...)
	at := len(buf) // the rows frame's offset in buf
	var logical int64
	var framed int64 // bytes of the rows frame
	if len(rows) > 0 {
		if int64(len(rows)) > f.most.Load() {
			f.most.Store(int64(len(rows))) // the owner is the only writer
		}
		var owned int64
		for i := range rows {
			logical += s.schema.LogicalBytes(&rows[i])
			owned += int64(len(rows[i].Key)) + s.schema.Heap(&rows[i].Data)
		}
		if s.delta && owned > f.mostOwned.Load() {
			f.mostOwned.Store(owned)
		}
		typ := s.rowsType()
		// The frame is built in place: its header, then the body, coded
		// straight into the frame or, compressed, into a scratch buffer.
		buf = append(buf, 0, 0, 0, 0, 0, 0, 0, 0, typ)
		var bodyBuf *[]byte
		body := buf
		if s.compress {
			bodyBuf = s.buffer()
			defer s.bufs.Put(bodyBuf)
			body = (*bodyBuf)[:0]
		}
		if s.delta {
			body = appendDelta(body, rows, s.codec, s.dc, owned)
		} else {
			body = appendRows(body, rows, s.codec)
		}
		if s.compress {
			*bodyBuf = body
			enc, _ := s.enc.Get().(*zstd.Encoder)
			if enc == nil {
				var err error
				if enc, err = zstd.NewWriter(nil, spillEncoderOptions...); err != nil {
					return err
				}
			}
			buf = enc.EncodeAll(body, buf)
			s.enc.Put(enc)
		} else {
			buf = body
		}
		if f.d != nil {
			seal2(buf[at:])
		} else {
			binary.LittleEndian.PutUint32(buf[at:at+4], uint32(len(buf)-at-8))
			binary.LittleEndian.PutUint32(buf[at+4:at+8], crc32.Checksum(buf[at+8:], spillCRC))
		}
		framed = int64(len(buf) - at)
	}
	if final && f.d == nil {
		buf = append(buf, s.frame(frameTrailer, binary.AppendUvarint(nil, uint64(f.written)))...)
	}
	var captured int64
	if f.capturing {
		captured = int64(len(rows))
	}
	if final && f.d != nil {
		d := f.d
		off := f.size + int64(len(buf))
		prior := crc32.Update(f.crc, spillCRC, buf)
		rowsBefore, capBefore, lastBefore := d.rows, d.captured, d.last
		d.rows, d.captured = d.rows+int64(len(rows)), d.captured+captured
		if captured > 0 {
			d.last = rows[len(rows)-1].Key
		}
		buf = frame2(buf, frameTrailer, trailerBody(d, f.captureAt > 0 || f.capturing, off, prior))
		buf = append(buf, tail(off)...)
		d.rows, d.captured, d.last = rowsBefore, capBefore, lastBefore
	}
	*bp = buf
	if len(buf) == 0 {
		return nil
	}
	n := int64(len(buf))
	if !s.admit(n, final || len(rows) == 0) {
		return errSpillFull
	}
	// Each frame opens the file by name: the output must still be the
	// directory the run owns. Ownership is checked before each mutation
	// (the create or open, the append, an ENOSPC rollback's truncation),
	// two stats each, so a replacement while one was under way stops the
	// next as the fatal replacement, never as a refused frame. A durable
	// file is opened through the output root.
	var err error
	if f.d != nil {
		err = faults.At(durableWritePoint(f.size == 0, final))
	}
	if err == nil {
		err = s.check()
	}
	if err == nil {
		openW := func() (*os.File, error) {
			s.opens.Add(1)
			if hookOpenWrite != nil {
				if err := hookOpenWrite(f); err != nil {
					return nil, err
				}
			}
			if f.d != nil {
				return s.cache.root.OpenFile(s.cache.rel(f.d.name), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
			}
			return os.OpenFile(f.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		}
		var fh *os.File
		fh, err = openW()
		if lowFD(err) {
			// Refused for want of descriptors: the read handles' idle
			// descriptors are closed (the low-descriptor fallback) and the
			// open, nothing written yet, is retried once after the
			// ownership check.
			s.h.lowFD()
			if err = s.check(); err == nil {
				fh, err = openW()
			}
		}
		if err == nil {
			if err = s.check(); err == nil {
				_, err = faults.Write(fh, buf)
				if err != nil && errors.Is(err, syscall.ENOSPC) {
					if verr := s.check(); verr != nil {
						err = verr // replaced mid-write: fatal, nothing more touched
					} else {
						s.h.retire(f, evictTruncated) // no cached read handle outlives a truncation
						if s.truncate(f, fh) == nil {
							err = errSpillFull // the torn frame is gone: back off as if refused
						}
					}
				}
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
	if len(rows) > 0 {
		// The rows frame's place in the file (a durable file's header or
		// capture frame may precede it in buf): the read-ahead and the
		// segments read it there.
		f.lastOff, f.lastSize, f.lastRows, f.lastLogical = f.size+int64(at), framed, int64(len(rows)), logical
	}
	if final {
		f.trailerAt = f.size + int64(at) + framed
	}
	f.size += n
	f.written += int64(len(rows))
	if d := f.d; d != nil {
		f.crc = crc32.Update(f.crc, spillCRC, buf)
		s.cache.charge(d, n)
		d.rows += int64(len(rows))
		if captured > 0 {
			if d.captured == 0 {
				d.first = strings.Clone(rows[0].Key)
			}
			d.captured += captured
			d.last = strings.Clone(rows[len(rows)-1].Key)
		}
	}
	if len(rows) > 0 {
		s.rows.Add(int64(len(rows)))
		first := s.frames.Add(1) == 1
		s.logical.Add(logical)
		s.frameBytes.add(framed)
		s.frameRows.add(int64(len(rows)))
		if first && s.spilling != nil {
			s.spilling()
		}
	}
	s.bytes.Add(n)
	return nil
}

// truncate cuts f back to its size before a torn frame. The append
// descriptor fh truncates where it may; on Windows an O_APPEND handle has
// no FILE_WRITE_DATA access, which SetEndOfFile needs, so the file is
// reopened for writing without append (a durable one through the output
// root) and truncated there.
func (s *spiller[P]) truncate(f *spillFile, fh *os.File) error {
	if fh.Truncate(f.size) == nil {
		return nil
	}
	s.opens.Add(1)
	var w *os.File
	var err error
	if f.d != nil {
		w, err = s.cache.root.OpenFile(s.cache.rel(f.d.name), os.O_WRONLY, 0)
	} else {
		w, err = os.OpenFile(f.path, os.O_WRONLY, 0)
	}
	if err != nil {
		return err
	}
	return errors.Join(w.Truncate(f.size), w.Close())
}

// buffer is a writer's frame or body buffer from the pool (put back by the
// caller).
func (s *spiller[P]) buffer() *[]byte {
	if bp, ok := s.bufs.Get().(*[]byte); ok {
		return bp
	}
	return new([]byte)
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
		if (s.max > 0 && live > s.max) || (s.reserve > 0 && freeBytes(s.dir)-n < s.reserve) {
			s.live.Add(-n)
			s.refused.Add(1)
			s.full.Store(true)
			// Drained certificates hold credit until K covers them: the
			// oldest is retired now (its credit returns at its unlink,
			// through the waker); an undrained file never is.
			s.cache.evictOne()
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
// drained ends a file whose rows were all handed on without its reader
// reaching the trailer (the segments, which checked the trailers first):
// as next does at the trailer, a run-private file is removed and its
// credit returned, a durable one handed back to its cache.
func (s *spiller[P]) drained(f *spillFile) error {
	s.closeFile(f)
	s.h.retire(f, evictRetired)
	if f.d != nil {
		s.cache.drainedFile(f.d)
		return nil
	}
	err := s.check()
	if err == nil {
		err = os.Remove(f.path)
	}
	s.release(f)
	return err
}

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
	if f.d != nil {
		return s.nextDurable(f, hdr, size)
	}
	var payload []byte
	if s.delta {
		if uint32(cap(s.rbuf)) < size {
			s.rbuf = make([]byte, size)
		}
		payload = s.rbuf[:size]
	} else {
		payload = make([]byte, size)
	}
	if _, err := io.ReadFull(f.r, payload); err != nil || crc32.Checksum(payload, spillCRC) != binary.LittleEndian.Uint32(hdr[4:]) {
		s.closeFile(f)
		return nil, errSpillFrame
	}
	f.pos += int64(len(hdr)) + int64(size)
	body := payload[1:]
	typ := payload[0]
	if typ != frameTrailer && typ != s.rowsType() {
		s.closeFile(f)
		return nil, errSpillFrame
	}
	if typ == frameTrailer {
		n, k := binary.Uvarint(body)
		s.closeFile(f)
		s.h.retire(f, evictRetired)
		if k <= 0 || int64(n) != f.read {
			return nil, fmt.Errorf("spill: %s holds %d rows, trailer says %d", f.path, f.read, n)
		}
		err := s.check()
		if err == nil {
			err = os.Remove(f.path)
		}
		s.release(f)
		return nil, err
	}
	t := time.Now()
	rows, err := s.decodeFrame(typ, body, uint64(f.most.Load()), uint64(f.mostOwned.Load()), true)
	s.decodeNs.Add(int64(time.Since(t)))
	if err != nil {
		s.closeFile(f)
		return nil, err
	}
	f.read += int64(len(rows))
	return rows, nil
}

// readAt reads and decodes the rows frame of size bytes at off in f on a
// lease of f's read handle (the read-ahead and the segments: any
// goroutine, concurrently with the drainer, which never reads this frame
// itself). It returns the decoding time.
func (s *spiller[P]) readAt(f *spillFile, off, size int64) ([]schema.Row[P], time.Duration, error) {
	if size <= 8 || size-8 > maxSpillFrame { // checked before allocating
		return nil, 0, errSpillFrame
	}
	h, err := s.lease(f)
	if err != nil {
		return nil, 0, fmt.Errorf("spill: %w", err)
	}
	defer s.h.release(h)
	buf := make([]byte, size)
	if _, err := h.fh.ReadAt(buf, off); err != nil {
		return nil, 0, fmt.Errorf("spill: %w", err)
	}
	payload := buf[8:]
	if int64(binary.LittleEndian.Uint32(buf[:4])) != size-8 || !frameOK(f, [8]byte(buf[:8]), payload) || payload[0] != s.rowsType() {
		return nil, 0, errSpillFrame
	}
	t := time.Now()
	rows, err := s.decodeFrame(payload[0], payload[1:], uint64(f.most.Load()), uint64(f.mostOwned.Load()), false) // its own buffers: it runs beside the drainer
	return rows, time.Since(t), err
}

// lease is a lease on f's read handle (spillhandles.go), opening f on a
// miss: a durable file through the output root, as its writes do. Release
// it with s.h.release.
func (s *spiller[P]) lease(f *spillFile) (*handle, error) {
	return s.h.lease(f, func() (*os.File, error) {
		s.opens.Add(1)
		if hookOpen != nil {
			if err := hookOpen(f); err != nil {
				return nil, err
			}
		}
		if f.d != nil {
			return s.cache.root.Open(s.cache.rel(f.d.name))
		}
		return os.Open(f.path)
	})
}

// hookOpenWrite, if set, sees every write open of a spill file first, and
// fails it with its error (tests).
var hookOpenWrite func(f *spillFile) error

// hookOpen, if set, sees every read open of a spill file first, and fails
// it with its error (tests).
var hookOpen func(f *spillFile) error

// frameOK checks a frame's CRC: a durable file's frames cover their
// header's length too (check2), a run-private file's the payload alone.
func frameOK(f *spillFile, hdr [8]byte, payload []byte) bool {
	if f.d != nil {
		return check2(hdr, payload)
	}
	return crc32.Checksum(payload, spillCRC) == binary.LittleEndian.Uint32(hdr[4:])
}

// trailerRows reads f's trailer on a lease of its read handle (any goroutine,
// once the file is complete): the row count its owner wrote.
func (s *spiller[P]) trailerRows(f *spillFile) (int64, error) {
	if f.d != nil {
		return s.durableTrailerRows(f)
	}
	size := f.size - f.trailerAt
	if f.trailerAt <= 0 || size <= 9 || size > 32 {
		return 0, errSpillFrame
	}
	h, err := s.lease(f)
	if err != nil {
		return 0, fmt.Errorf("spill: %w", err)
	}
	defer s.h.release(h)
	buf := make([]byte, size)
	if _, err := h.fh.ReadAt(buf, f.trailerAt); err != nil {
		return 0, fmt.Errorf("spill: %w", err)
	}
	payload := buf[8:]
	if int64(binary.LittleEndian.Uint32(buf[:4])) != size-8 || crc32.Checksum(payload, spillCRC) != binary.LittleEndian.Uint32(buf[4:8]) || payload[0] != frameTrailer {
		return 0, errSpillFrame
	}
	n, k := binary.Uvarint(payload[1:])
	if k <= 0 {
		return 0, errSpillFrame
	}
	return int64(n), nil
}

// durableTrailerRows is trailerRows for a durable file: its trailer frame
// (check2), then the tail; the rows are the trailer's (parseTrailer).
func (s *spiller[P]) durableTrailerRows(f *spillFile) (int64, error) {
	if f.trailerAt <= 0 || f.trailerAt+9 > f.size {
		return 0, errSpillFrame
	}
	h, err := s.lease(f)
	if err != nil {
		return 0, fmt.Errorf("spill: %w", err)
	}
	defer s.h.release(h)
	fh := h.fh
	var hdr [8]byte
	if _, err := fh.ReadAt(hdr[:], f.trailerAt); err != nil {
		return 0, fmt.Errorf("spill: %w", err)
	}
	size := int64(binary.LittleEndian.Uint32(hdr[:4]))
	if size < 1 || f.trailerAt+8+size > f.size {
		return 0, errSpillFrame
	}
	payload := make([]byte, size)
	if _, err := fh.ReadAt(payload, f.trailerAt+8); err != nil {
		return 0, fmt.Errorf("spill: %w", err)
	}
	if !check2(hdr, payload) || payload[0] != frameTrailer {
		return 0, errSpillFrame
	}
	t, ok := parseTrailer(payload[1:])
	if !ok {
		return 0, errSpillFrame
	}
	return t.rows, nil
}

// skip passes over the frame ending at end holding rows rows that the
// read-ahead replayed (drainer only): the next sequential read, or the
// trailer's, starts after it, on a reader reopened there.
func (s *spiller[P]) skip(f *spillFile, end, rows int64) {
	if f.r != nil {
		s.closeFile(f)
	}
	f.pos = end // absolute: a durable file's capture frame may precede a rows frame
	f.read += rows
}

// appendRows encodes rows for a plain frame: the count, then per row its
// key and the producer's payload codec.
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

// appendDelta encodes rows as a delta frame's body: the row count, the
// storage its decoded rows may own (owned: the keys' bytes and the
// payloads' Heap, an upper bound on what decoding copies), the payload
// coding, then per row the length of the prefix its key shares with the
// previous key, the rest of the key (length-prefixed) and the payload,
// through dc against the previous payload when the producer has one, else
// through c. Lengths are bytes: keys are bytes, not UTF-8 text. The first
// row's previous key is empty and its previous payload a zero P.
func appendDelta[P any](dst []byte, rows []schema.Row[P], c schema.Codec[P], dc schema.DeltaCodec[P], owned int64) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(rows)))
	dst = binary.AppendUvarint(dst, uint64(owned))
	coding := byte(payloadStateless)
	if dc != nil {
		coding = payloadContextual
	}
	dst = append(dst, coding)
	var zero P
	prev, prevKey := &zero, ""
	for i := range rows {
		key := rows[i].Key
		shared := commonPrefix(prevKey, key)
		dst = binary.AppendUvarint(dst, uint64(shared))
		dst = schema.AppendString(dst, key[shared:])
		if dc != nil {
			dst = dc.AppendDelta(dst, prev, &rows[i].Data)
		} else {
			dst = c.Append(dst, &rows[i].Data)
		}
		prev, prevKey = &rows[i].Data, key
	}
	return dst
}

// commonPrefix is the length of the longest common byte prefix of a and b.
func commonPrefix(a, b string) int {
	n := min(len(a), len(b))
	i := 0
	for i+8 <= n && a[i:i+8] == b[i:i+8] {
		i += 8
	}
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

// decodeDelta decodes a frame written by appendDelta, of at most most rows
// whose storage is at most mostOwned bytes (the largest its file was
// given) and maxSpillDecoded: the header is checked before anything is
// allocated, and every byte the rows own is charged to the declared
// storage (schema.ErrDecodeCap past it), so a malformed frame cannot
// allocate past what was spilled however far its prefixes expand. Every
// length is checked before use, and a frame must end exactly at its last
// row. The rows own their storage: src can be reused once this returns.
func decodeDelta[P any](src []byte, c schema.Codec[P], dc schema.DeltaCodec[P], most, mostOwned uint64) ([]schema.Row[P], error) {
	n, k := binary.Uvarint(src)
	if k <= 0 {
		return nil, schema.ErrCodec
	}
	src = src[k:]
	owned, k := binary.Uvarint(src)
	if k <= 0 || len(src) == k {
		return nil, schema.ErrCodec
	}
	coding := src[k]
	src = src[k+1:]
	want := byte(payloadStateless)
	if dc != nil {
		want = payloadContextual
	}
	if coding != want || n > uint64(len(src)) || n > most {
		return nil, schema.ErrCodec
	}
	if owned > mostOwned || owned > maxSpillDecoded {
		return nil, schema.ErrDecodeCap
	}
	d := schema.NewOwningDecoder(src, int64(owned))
	rows := make([]schema.Row[P], n)
	var zero P
	prev, prevKey := &zero, ""
	for i := range rows {
		shared, suffix := d.Uvarint(), d.Uvarint()
		if d.Err() != nil || shared > uint64(len(prevKey)) || suffix > uint64(d.Len()) {
			return nil, schema.ErrCodec
		}
		b := d.Alloc(int(shared) + int(suffix))
		if err := d.Err(); err != nil {
			return nil, err
		}
		copy(b, prevKey[:shared])
		copy(b[shared:], d.Fixed(int(suffix)))
		rows[i].Key = schema.View(b)
		if dc != nil {
			dc.DecodeDelta(d, prev, &rows[i].Data)
		} else {
			c.Decode(d, &rows[i].Data)
		}
		if err := d.Err(); err != nil {
			return nil, err
		}
		prev, prevKey = &rows[i].Data, rows[i].Key
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

// FSType names the filesystem dir is on (Linux: "tmpfs", "ext4", "xfs",
// ..., or its statfs magic number in hex), or "" where it is not read. A
// spill on tmpfs or ramfs is held in memory, outside the process's RSS.
func FSType(dir string) string { return fsType(dir) }

// InMemory reports whether fsType names a filesystem held in memory.
func InMemory(fsType string) bool { return fsType == "tmpfs" || fsType == "ramfs" }

// sizeHist counts sizes in power-of-two buckets: bucket k holds the sizes
// in (2^(k-1), 2^k], bucket 0 sizes of at most 1.
type sizeHist struct {
	b   [64]atomic.Int64
	max atomic.Int64
}

func (h *sizeHist) add(n int64) {
	k := 0
	if n > 1 {
		k = bits.Len64(uint64(n - 1))
	}
	h.b[k].Add(1)
	for m := h.max.Load(); n > m && !h.max.CompareAndSwap(m, n); m = h.max.Load() {
	}
}

// SizeQuantiles summarise a size distribution: P50, P90 and P99 are the
// upper bounds of the power-of-two buckets holding those quantiles (a
// quantile is at most its bound and more than half of it); Max is exact.
type SizeQuantiles struct {
	P50 int64 `json:"p50"`
	P90 int64 `json:"p90"`
	P99 int64 `json:"p99"`
	Max int64 `json:"max"`
}

func (h *sizeHist) quantiles() SizeQuantiles {
	var c [64]int64
	var n int64
	for k := range h.b {
		c[k] = h.b[k].Load()
		n += c[k]
	}
	q := SizeQuantiles{Max: h.max.Load()}
	if n == 0 {
		return q
	}
	at := func(p int64) int64 { // the bound of the bucket holding the p-th percentile
		want, seen := (n*p+99)/100, int64(0)
		for k := range c {
			if seen += c[k]; seen >= want {
				return min(int64(1)<<k, q.Max)
			}
		}
		return q.Max
	}
	q.P50, q.P90, q.P99 = at(50), at(90), at(99)
	return q
}
