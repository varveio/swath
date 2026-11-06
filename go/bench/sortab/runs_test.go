// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sortab

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source/s3"
)

// A sorted run is a file of frames: a 4-byte little-endian length, a
// CRC-32C of the payload, then the payload: a uvarint row count and each
// row's key (schema.AppendString) and payload (the schema's codec, the
// encoding order-stage spill uses). sortout wrote Parquet runs with its
// own writer, which no longer exists; the payload codec is the port's.

const (
	frameRows = 4096
	maxFrame  = 256 << 20
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

type row = schema.Row[s3.Object]

// writeRun writes ascending rows to path and syncs it; it returns the
// bytes written.
func writeRun(path string, rows []*row, codec schema.Codec[s3.Object]) (int64, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return 0, err
	}
	w := bufio.NewWriterSize(f, 1<<20)
	var n int64
	var payload []byte
	for len(rows) > 0 {
		k := min(frameRows, len(rows))
		payload = binary.AppendUvarint(payload[:0], uint64(k))
		for _, r := range rows[:k] {
			payload = schema.AppendString(payload, r.Key)
			payload = codec.Append(payload, &r.Data)
		}
		rows = rows[k:]
		var hdr [8]byte
		binary.LittleEndian.PutUint32(hdr[:4], uint32(len(payload)))
		binary.LittleEndian.PutUint32(hdr[4:], crc32.Checksum(payload, castagnoli))
		if _, err := w.Write(hdr[:]); err != nil {
			f.Close()
			return n, err
		}
		if _, err := w.Write(payload); err != nil {
			f.Close()
			return n, err
		}
		n += int64(len(hdr) + len(payload))
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return n, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return n, err
	}
	return n, f.Close()
}

// runReader reads a run frame by frame; decoded strings view the frame,
// which is never reused.
type runReader struct {
	f     *os.File
	r     *bufio.Reader
	codec schema.Codec[s3.Object]
	buf   []row
	i     int
	eof   bool
	read  *int64 // bytes read, shared
}

func openRun(path string, codec schema.Codec[s3.Object], read *int64) (*runReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	rr := &runReader{f: f, r: bufio.NewReaderSize(f, 256<<10), codec: codec, read: read}
	if err := rr.fill(); err != nil {
		f.Close()
		return nil, err
	}
	return rr, nil
}

func (rr *runReader) fill() error {
	rr.i, rr.buf = 0, nil
	var hdr [8]byte
	if _, err := io.ReadFull(rr.r, hdr[:]); err != nil {
		if errors.Is(err, io.EOF) {
			rr.eof = true
			return nil
		}
		return err
	}
	n := binary.LittleEndian.Uint32(hdr[:4])
	if n > maxFrame {
		return fmt.Errorf("run frame of %d bytes", n)
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(rr.r, payload); err != nil {
		return err
	}
	*rr.read += int64(8 + n)
	if crc32.Checksum(payload, castagnoli) != binary.LittleEndian.Uint32(hdr[4:]) {
		return errors.New("run frame: checksum mismatch")
	}
	d := schema.NewDecoder(payload)
	k := d.Uvarint()
	if k > frameRows {
		return fmt.Errorf("run frame of %d rows", k)
	}
	rr.buf = make([]row, k)
	for j := range rr.buf {
		rr.buf[j].Key = d.Str()
		rr.codec.Decode(d, &rr.buf[j].Data)
	}
	if err := d.Err(); err != nil {
		return err
	}
	if d.Len() != 0 {
		return errors.New("run frame: trailing bytes")
	}
	return nil
}

func (rr *runReader) peek() *row {
	if rr.i >= len(rr.buf) {
		return nil
	}
	return &rr.buf[rr.i]
}

func (rr *runReader) advance() error {
	rr.i++
	if rr.i >= len(rr.buf) && !rr.eof {
		return rr.fill()
	}
	return nil
}

func (rr *runReader) Close() error { return rr.f.Close() }
