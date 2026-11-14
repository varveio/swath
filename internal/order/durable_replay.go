// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/klauspost/compress/zstd"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/faults"
	"github.com/varveio/swath/internal/schema"
)

// evictOne is evict on a cache that may be nil.
func (c *Cache) evictOne() {
	if c != nil {
		c.evict()
	}
}

// nextDurable is next for a durable file (live replay: every rows frame,
// those before the capture start included, in file order; the header and
// the capture start are skipped). At the trailer the reader is closed and
// the file handed to the cache, which unlinks a live-only file and keeps a
// certificate until K covers it; its credit returns at the unlink.
func (s *spiller[P]) nextDurable(f *spillFile, hdr [8]byte, size uint32) ([]schema.Row[P], error) {
	for {
		var payload []byte
		if s.delta {
			if uint32(cap(s.rbuf)) < size {
				s.rbuf = make([]byte, size)
			}
			payload = s.rbuf[:size]
		} else {
			payload = make([]byte, size)
		}
		if _, err := io.ReadFull(f.r, payload); err != nil || !check2(hdr, payload) {
			s.closeFile(f)
			return nil, errSpillFrame
		}
		f.pos += int64(len(hdr)) + int64(size)
		typ, body := payload[0], payload[1:]
		switch typ {
		case frameHeader, frameCapture:
			if _, err := io.ReadFull(f.r, hdr[:]); err != nil {
				s.closeFile(f)
				return nil, fmt.Errorf("spill: %w", err)
			}
			if size = binary.LittleEndian.Uint32(hdr[:4]); size == 0 || size > maxSpillFrame {
				s.closeFile(f)
				return nil, errSpillFrame
			}
			continue
		case frameTrailer:
			t, ok := parseTrailer(body)
			s.closeFile(f)
			s.h.retire(f, evictRetired)
			if !ok || t.rows != f.read {
				return nil, fmt.Errorf("spill: durable file %s holds %d rows, its trailer says %d", f.d.name, f.read, t.rows)
			}
			s.cache.drainedFile(f.d)
			return nil, nil
		}
		if typ != s.rowsType() {
			s.closeFile(f)
			return nil, errSpillFrame
		}
		rows, err := s.decodeFrame(typ, body, uint64(f.most.Load()), uint64(f.mostOwned.Load()), true)
		if err != nil {
			s.closeFile(f)
			return nil, err
		}
		f.read += int64(len(rows))
		return rows, nil
	}
}

// replayDecoder makes rdec, the zstd decoder of a certificate's frames when
// this run does not compress, unless it is made already, for as many
// decodes at once as s.dec. Only the drainer's goroutine makes it, before
// the segments read.
func (s *spiller[P]) replayDecoder() error {
	if s.rdec != nil {
		return nil
	}
	var err error
	s.rdec, err = zstd.NewReader(nil, zstd.WithDecoderConcurrency(1+spareProcs()), zstd.WithDecoderMaxMemory(maxSpillDecoded))
	return err
}

// decodeFrame decodes a rows frame of type typ within most rows and owned
// bytes of storage (reuse: the drainer's buffers may be reused, the rows
// owning their storage; a plain frame's rows view body, which must not be
// reused then).
func (s *spiller[P]) decodeFrame(typ byte, body []byte, most, owned uint64, reuse bool) ([]schema.Row[P], error) {
	delta := typ == frameDelta || typ == frameDeltaZstd
	if typ == frameRowsZstd || typ == frameDeltaZstd {
		dec := s.dec
		if dec == nil {
			if err := s.replayDecoder(); err != nil {
				return nil, err
			}
			dec = s.rdec
		}
		var dst []byte
		if delta && reuse {
			dst = s.zbuf[:0]
		}
		out, err := dec.DecodeAll(body, dst)
		if err != nil {
			return nil, errSpillFrame
		}
		if delta && reuse {
			s.zbuf = out
		}
		body = out
	}
	var rows []schema.Row[P]
	var err error
	if delta {
		rows, err = decodeDelta(body, s.codec, s.dc, most, owned)
	} else {
		rows, err = decodeRows(body, s.codec, most)
	}
	if err != nil && errors.Is(err, schema.ErrDecodeCap) {
		s.decodeRefused.Add(1)
	}
	return rows, err
}

// certFile is the reused certificate i, after the checks every replay
// makes first: recovery accepts only the run's codec (rejected.codec), so a
// difference here is a broken invariant, not a stale file.
func (o *run[R, P]) certFile(i int) (*Cert, error) {
	sp := o.sp
	if sp == nil || sp.cache == nil || i < 0 || i >= len(sp.cache.reused) {
		return nil, fmt.Errorf("order: replay of certificate %d without its durable spill", i)
	}
	d := sp.cache.reused[i]
	if d.codec != sp.codecID || d.codecVer != rowCodecVersion {
		return nil, &dataset.InvariantError{Reason: fmt.Sprintf("durable spill: %s holds rows of codec %q version %d, the run's is %q version %d", d.name, d.codec, d.codecVer, sp.codecID, rowCodecVersion)}
	}
	return d, nil
}

// replayCert is a recovery replay on the drainer: the reused certificate
// i's rows above max(cs, K), in file order, through e (the drainer's
// direct emitter), checked as walkCert checks them. The file is then
// handed back to the cache (retired once K covers it).
func (o *run[R, P]) replayCert(i int, e *emitter[R, P]) error {
	d, err := o.certFile(i)
	if err != nil {
		return err
	}
	if err := faults.At("spill-replay"); err != nil {
		return err
	}
	sp, c := o.sp, o.sp.cache
	sp.opens.Add(1)
	fh, err := dataset.OpenRegularIn(c.root, c.rel(d.name), 0)
	if err != nil {
		return fmt.Errorf("durable spill: replay %s: %w", d.name, err)
	}
	sp.readers(1)
	defer func() {
		fh.Close()
		sp.readers(-1)
	}()
	before := func() error { return faults.At("spill-replay-frame") } // mid replay: earlier frames' rows handed on
	emitted, skipped, err := o.walkCert(d, bufio.NewReaderSize(fh, 1<<16), before, func(_, _ int64, rows []schema.Row[P], skip int) error {
		return e.Rows(rows[skip:])
	})
	if err != nil {
		return err
	}
	o.replayed.Add(emitted)
	c.replayedCert(emitted, skipped)
	c.drainedFile(d)
	return nil
}

// replayedCert counts a certificate's replay, the drainer's or the
// segments': the rows handed on and those skipped (pre-capture, and at or
// below K).
func (c *Cache) replayedCert(emitted, skipped int64) {
	c.st.ReplayedRows.Add(emitted)
	c.st.ReplaySkippedRows.Add(skipped)
	c.st.ReplayedCerts.Add(1)
	c.st.RequestsSavedEst.Add((emitted + replayPageKeys - 1) / replayPageKeys)
}

// walkCert reads the reused certificate d's frames from r, from its start,
// as a recovery replay reads them: the pre-capture frames are skipped
// undecoded; each captured frame is decoded under the bounds validation
// measured, its keys checked to lie in (cs, ce] and ascend, or with Groups
// repeat their predecessor (a *dataset.InvariantError otherwise), and
// handed to frame with its offset and size in the file (its header
// included) and how many of its first rows lie at or below max(cs, K).
// before, if set, runs before each captured frame is decoded. At the
// trailer the captured rows are checked against its count. It returns the
// rows above max(cs, K) and those skipped (pre-capture included).
func (o *run[R, P]) walkCert(d *Cert, r io.Reader, before func() error, frame func(off, size int64, rows []schema.Row[P], skip int) error) (emitted, skipped int64, err error) {
	sp := o.sp
	low := d.cs
	if k := o.opt.After; k != nil && *k > low {
		low = *k
	}
	bad := func(f string, a ...any) error {
		return &dataset.InvariantError{Reason: fmt.Sprintf("durable spill: replay of %s: ", d.name) + fmt.Sprintf(f, a...)}
	}
	var captured, off int64
	capture := false
	prev, first := d.cs, true
	for {
		var hdr [8]byte
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return 0, 0, bad("%v", err)
		}
		size := binary.LittleEndian.Uint32(hdr[:4])
		if size == 0 || size > maxSpillFrame {
			return 0, 0, bad("frame of %d bytes", size)
		}
		payload := make([]byte, size)
		if _, err := io.ReadFull(r, payload); err != nil || !check2(hdr, payload) {
			return 0, 0, bad("damaged frame")
		}
		at := off
		off += int64(len(hdr)) + int64(size)
		typ, body := payload[0], payload[1:]
		switch typ {
		case frameHeader:
			continue
		case frameCapture:
			capture = true
			continue
		case frameTrailer:
			t, ok := parseTrailer(body)
			if !ok || !capture || t.captured != captured {
				return 0, 0, bad("%d captured rows, trailer says %d", captured, t.captured)
			}
			return emitted, skipped + t.rows - t.captured, nil
		case frameRows, frameRowsZstd, frameDelta, frameDeltaZstd:
		default:
			return 0, 0, bad("frame type %q", typ)
		}
		if !capture {
			continue // pre-capture: live replay's only
		}
		if before != nil {
			if err := before(); err != nil {
				return 0, 0, err
			}
		}
		rows, err := sp.decodeFrame(typ, body, uint64(d.most), uint64(d.owned), false)
		if err != nil {
			return 0, 0, bad("%v", err)
		}
		captured += int64(len(rows))
		n := 0
		for n < len(rows) && rows[n].Key <= low {
			n++
		}
		for j := range rows {
			k := rows[j].Key
			// Keys ascend; with Groups (versions) a key repeats over its
			// group's consecutive rows, but never cs's: cs is a complete
			// key, every row of it before the capture.
			if k < prev || k == prev && (first || !o.opt.Groups) || (!d.ceOpen && k > d.ce) {
				return 0, 0, bad("key %q after %q in (%q, %q]", k, prev, d.cs, d.ce)
			}
			prev, first = k, false
		}
		skipped += int64(n)
		if n < len(rows) {
			emitted += int64(len(rows) - n)
			if err := frame(at, off-at, rows, n); err != nil {
				return 0, 0, err
			}
		}
	}
}

// certScan is a reused certificate as the segments replay it (scanCert):
// its file, a unit per captured frame holding a row above max(cs, K), and
// the rows its replay hands on and skips.
type certScan[P any] struct {
	f                *spillFile
	units            []unit[P]
	emitted, skipped int64
}

// scanCert reads the reused certificate i for the segments, on a lease of
// its read handle, as walkCert checks a replay, keeping none of its rows:
// each captured frame holding a row above max(cs, K) becomes a unit that
// its segment reads again by offset (readAt), recording the rows above,
// their logical bytes, the budget bytes of the whole frame decoded and the
// rows to skip at its head. Neither the file nor its ledger record says
// how many rows a frame holds or their logical bytes, which the cut needs.
func (o *run[R, P]) scanCert(i int) (*certScan[P], error) {
	d, err := o.certFile(i)
	if err != nil {
		return nil, err
	}
	if err := faults.At("spill-replay"); err != nil {
		return nil, err
	}
	sp := o.sp
	f := &spillFile{path: d.name, d: d, size: d.bytes}
	f.most.Store(d.most)
	f.mostOwned.Store(d.owned)
	h, err := sp.lease(f)
	if err != nil {
		return nil, fmt.Errorf("durable spill: replay %s: %w", d.name, err)
	}
	defer sp.h.release(h)
	s := &certScan[P]{f: f}
	s.emitted, s.skipped, err = o.walkCert(d, bufio.NewReaderSize(&atReader{fh: h.fh}, 1<<16), nil, func(off, size int64, rows []schema.Row[P], skip int) error {
		if o.stopped.Load() {
			return context.Cause(o.ctx)
		}
		var l int64
		for j := skip; j < len(rows); j++ {
			l += o.schema.LogicalBytes(&rows[j])
		}
		n, u := int64(len(rows)-skip), o.measure(rows)
		s.units = append(s.units, unit[P]{f: f, at: spillAt{off: off, size: size, units: u, rows: n, logical: l}, n: n, logical: l, units: u, skip: skip})
		return nil
	})
	if err != nil {
		sp.h.retire(f, evictRetired)
		return nil, err
	}
	return s, nil
}

// scanCerts scans the certificates certs (scanCert), at most n at once,
// in their order; on a failure the first in that order is returned.
func (o *run[R, P]) scanCerts(certs []certAt[R, P], n int) ([]*certScan[P], error) {
	if o.sp.dec == nil {
		if err := o.sp.replayDecoder(); err != nil { // before any goroutine decodes
			return nil, err
		}
	}
	out := make([]*certScan[P], len(certs))
	errs := make([]error, len(certs))
	var next atomic.Int64
	var failed atomic.Bool
	var wg sync.WaitGroup
	for range min(n, len(certs)) {
		wg.Go(func() {
			for !failed.Load() && !o.stopped.Load() {
				j := int(next.Add(1) - 1)
				if j >= len(certs) {
					return
				}
				if out[j], errs[j] = o.scanCert(certs[j].cert); errs[j] != nil {
					failed.Store(true)
				}
			}
		})
	}
	wg.Wait()
	for j, err := range errs {
		if err != nil {
			o.dropScans(out)
			return nil, err
		}
		if out[j] == nil { // stopped before it was scanned
			o.dropScans(out)
			return nil, context.Cause(o.ctx)
		}
	}
	return out, nil
}

// dropScans retires the read handles of scanned certificates the segments
// will not read: the drainer replays them on its own reader.
func (o *run[R, P]) dropScans(scans []*certScan[P]) {
	for _, s := range scans {
		if s != nil {
			o.sp.h.retire(s.f, evictRetired)
		}
	}
}
