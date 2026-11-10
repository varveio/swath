// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/schema"
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

// decodeFrame decodes a rows frame of type typ within most rows and owned
// bytes of storage (reuse: the drainer's buffers may be reused, the rows
// owning their storage; a plain frame's rows view body, which must not be
// reused then).
func (s *spiller[P]) decodeFrame(typ byte, body []byte, most, owned uint64, reuse bool) ([]schema.Row[P], error) {
	delta := typ == frameDelta || typ == frameDeltaZstd
	if typ == frameRowsZstd || typ == frameDeltaZstd {
		dec := s.dec
		if dec == nil {
			if s.rdec == nil {
				var err error
				if s.rdec, err = zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(maxSpillDecoded)); err != nil {
					return nil, err
				}
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

// replayCert is a recovery replay: the reused certificate i's captured
// rows above max(cs, K), in file order, through e (the drainer's direct
// emitter); pre-capture frames are skipped undecoded. Every key is checked
// to lie in (cs, ce] and ascend, or with Groups repeat its predecessor (a
// *dataset.InvariantError otherwise); the
// frames decode under the bounds validation measured. The file is then
// handed back to the cache (retired once K covers it).
func (o *run[R, P]) replayCert(i int, e *emitter[R, P]) error {
	sp := o.sp
	if sp == nil || sp.cache == nil || i < 0 || i >= len(sp.cache.reused) {
		return fmt.Errorf("order: replay of certificate %d without its durable spill", i)
	}
	c := sp.cache
	d := c.reused[i]
	low := d.cs
	if k := o.opt.After; k != nil && *k > low {
		low = *k
	}
	if d.codec != sp.codecID || d.codecVer != rowCodecVersion {
		// Recovery accepts only the run's codec (rejected.codec): a
		// difference here is a broken invariant, not a stale file.
		return &dataset.InvariantError{Reason: fmt.Sprintf("durable spill: %s holds rows of codec %q version %d, the run's is %q version %d", d.name, d.codec, d.codecVer, sp.codecID, rowCodecVersion)}
	}
	if err := faults.At("spill-replay"); err != nil {
		return err
	}
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
	r := bufio.NewReaderSize(fh, 1<<16)
	bad := func(f string, a ...any) error {
		return &dataset.InvariantError{Reason: fmt.Sprintf("durable spill: replay of %s: ", d.name) + fmt.Sprintf(f, a...)}
	}
	captured, emitted, skipped := int64(0), int64(0), int64(0)
	capture := false
	prev, first := d.cs, true
	for {
		var hdr [8]byte
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return bad("%v", err)
		}
		size := binary.LittleEndian.Uint32(hdr[:4])
		if size == 0 || size > maxSpillFrame {
			return bad("frame of %d bytes", size)
		}
		payload := make([]byte, size)
		if _, err := io.ReadFull(r, payload); err != nil || !check2(hdr, payload) {
			return bad("damaged frame")
		}
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
				return bad("%d captured rows, trailer says %d", captured, t.captured)
			}
			skipped += t.rows - t.captured
			c.st.ReplayedRows.Add(emitted)
			c.st.ReplaySkippedRows.Add(skipped)
			c.st.ReplayedCerts.Add(1)
			c.st.RequestsSavedEst.Add((emitted + replayPageKeys - 1) / replayPageKeys)
			o.replayed.Add(emitted)
			c.drainedFile(d)
			return nil
		case frameRows, frameRowsZstd, frameDelta, frameDeltaZstd:
		default:
			return bad("frame type %q", typ)
		}
		if !capture {
			continue // pre-capture: live replay's only
		}
		if err := faults.At("spill-replay-frame"); err != nil { // mid replay: earlier frames' rows handed on
			return err
		}
		rows, err := sp.decodeFrame(typ, body, uint64(d.most), uint64(d.owned), false)
		if err != nil {
			return bad("%v", err)
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
				return bad("key %q after %q in (%q, %q]", k, prev, d.cs, d.ce)
			}
			prev, first = k, false
		}
		skipped += int64(n)
		if keep := rows[n:]; len(keep) > 0 {
			emitted += int64(len(keep))
			if err := e.Rows(keep); err != nil {
				return err
			}
		}
	}
}
