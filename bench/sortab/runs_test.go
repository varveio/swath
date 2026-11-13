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

	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source/s3"
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

// runWriter writes ascending rows to a run, a frame per frameRows rows.
type runWriter struct {
	f       *os.File
	w       *bufio.Writer
	codec   schema.Codec[s3.Object]
	payload []byte
	k       int   // rows in the open frame
	n       int64 // bytes written
	rows    int64
}

func createRun(path string, codec schema.Codec[s3.Object]) (*runWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &runWriter{f: f, w: bufio.NewWriterSize(f, 1<<20), codec: codec}, nil
}

func (rw *runWriter) add(r *row) error {
	rw.payload = schema.AppendString(rw.payload, r.Key)
	rw.payload = rw.codec.Append(rw.payload, &r.Data)
	rw.k++
	rw.rows++
	if rw.k == frameRows {
		return rw.flush()
	}
	return nil
}

func (rw *runWriter) flush() error {
	if rw.k == 0 {
		return nil
	}
	body := binary.AppendUvarint(nil, uint64(rw.k))
	body = append(body, rw.payload...)
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[:4], uint32(len(body)))
	binary.LittleEndian.PutUint32(hdr[4:], crc32.Checksum(body, castagnoli))
	if _, err := rw.w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := rw.w.Write(body); err != nil {
		return err
	}
	rw.n += int64(len(hdr) + len(body))
	rw.payload, rw.k = rw.payload[:0], 0
	return nil
}

// close flushes, syncs and closes the run.
func (rw *runWriter) close() error {
	err := rw.flush()
	if err == nil {
		err = rw.w.Flush()
	}
	if err == nil {
		err = rw.f.Sync()
	}
	return errors.Join(err, rw.f.Close())
}

// writeRun writes ascending rows to path and syncs it; it returns the
// bytes written.
func writeRun(path string, rows []*row, codec schema.Codec[s3.Object]) (int64, error) {
	rw, err := createRun(path, codec)
	if err != nil {
		return 0, err
	}
	for _, r := range rows {
		if err := rw.add(r); err != nil {
			rw.f.Close()
			return rw.n, err
		}
	}
	err = rw.close()
	return rw.n, err
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
	rr := &runReader{f: f, r: bufio.NewReaderSize(f, 64<<10), codec: codec, read: read}
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
