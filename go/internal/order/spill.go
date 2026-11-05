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

var spillCRC = crc32.MakeTable(crc32.Castagnoli)

type spiller struct {
	dir      string
	compress bool
	seq      atomic.Uint64
	enc      sync.Pool // *zstd.Encoder
	dec      *zstd.Decoder

	rows, frames, bytes, files atomic.Int64
}

func newSpiller(parent string, compress bool) (*spiller, error) {
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return nil, fmt.Errorf("spill: %w", err)
	}
	dir, err := os.MkdirTemp(parent, "spill-")
	if err != nil {
		return nil, fmt.Errorf("spill: %w", err)
	}
	s := &spiller{dir: dir, compress: compress}
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
		f.written += int64(len(rows))
		s.rows.Add(int64(len(rows)))
		s.frames.Add(1)
	}
	if final {
		buf = append(buf, s.frame('T', binary.AppendUvarint(nil, uint64(f.written)))...)
	}
	if len(buf) == 0 {
		return nil
	}
	fh, err := os.OpenFile(f.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("spill: %w", err)
	}
	_, err = fh.Write(buf)
	if cerr := fh.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("spill: %w", err)
	}
	s.bytes.Add(int64(len(buf)))
	return nil
}

var errSpillFrame = errors.New("spill: corrupt frame")

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
		return nil, fmt.Errorf("spill: %w", err)
	}
	payload := make([]byte, binary.LittleEndian.Uint32(hdr[:4]))
	if _, err := io.ReadFull(f.r, payload); err != nil || len(payload) == 0 || crc32.Checksum(payload, spillCRC) != binary.LittleEndian.Uint32(hdr[4:]) {
		return nil, errSpillFrame
	}
	body := payload[1:]
	switch payload[0] {
	case 'T':
		n, k := binary.Uvarint(body)
		if k <= 0 || int64(n) != f.read {
			return nil, fmt.Errorf("spill: %s holds %d rows, trailer says %d", f.path, f.read, n)
		}
		f.f.Close()
		return nil, os.Remove(f.path)
	case 'Z':
		var err error
		if body, err = s.dec.DecodeAll(body, nil); err != nil {
			return nil, errSpillFrame
		}
	case 'R':
	default:
		return nil, errSpillFrame
	}
	rows, err := schema.DecodeRows(body)
	if err != nil {
		return nil, err
	}
	f.read += int64(len(rows))
	return rows, nil
}
