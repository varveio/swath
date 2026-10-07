//go:build nativezstd && cgo

// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"io"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/parquet-go/parquet-go/compress/zstd"
)

func TestNativeCodecFramesReuseCloseAndConcurrentCalls(t *testing.T) {
	codec, release, err := newNativeCodec()
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	var workers sync.WaitGroup
	for range 4 {
		workers.Go(func() {
			for _, input := range [][]byte{nil, {0}, bytes.Repeat([]byte("frame"), 4096)} {
				encoded, err := codec.Encode(nil, input)
				if err != nil {
					t.Error(err)
					return
				}
				if len(encoded) < 5 || encoded[4]&4 != 0 {
					t.Error("native frame unexpectedly carries a checksum")
					return
				}
				decoded, err := codec.Decode(nil, encoded)
				if err != nil || !bytes.Equal(input, decoded) {
					t.Errorf("native frame mismatch: %v", err)
					return
				}
				decoded, err = (&zstd.Codec{}).Decode(nil, encoded)
				if err != nil || !bytes.Equal(input, decoded) {
					t.Errorf("Go decoder frame mismatch: %v", err)
					return
				}
			}
		})
	}
	workers.Wait()
	if err := release(); err != nil {
		t.Fatal(err)
	}
	if _, err := codec.Encode(nil, []byte("closed")); err != io.ErrClosedPipe {
		t.Fatalf("released codec error=%v", err)
	}
}

func TestNativeCodecParquetAndOutputFailures(t *testing.T) {
	rows := valueParityRows()
	config := writerConfig{Layout: "served", KeyEncoding: "delta", IntegerEncoding: "delta", PageStatistics: false, PageRows: 16, GroupRows: 40, RowAPI: "columns", Codec: "native"}
	path := filepath.Join(t.TempDir(), "native.parquet")
	part, err := writePart(path, rows, config, diskOps())
	if err != nil {
		t.Fatal(err)
	}
	report, err := verifyPart(path, rows)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkGeometry(report, config); err != nil {
		t.Fatal(err)
	}
	if err := checkFlushReports(report, part.Encoding, config); err != nil {
		t.Fatal(err)
	}
	_, cuts := referenceCuts(t, rows, config)
	for _, cut := range cuts {
		var output bytes.Buffer
		result, err := encodeRows(&prefixFailure{output: &output, cut: cut.offset, err: io.ErrClosedPipe}, rows, config)
		if err == nil || !reflect.DeepEqual(result, encodeResult{}) {
			t.Fatalf("native output failure returned result=%+v error=%v", result, err)
		}
	}
}
