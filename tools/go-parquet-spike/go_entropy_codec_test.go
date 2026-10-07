// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/hex"
	"io"
	"math/rand"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

func TestEntropyCodecCompressesSkewedHexAndRoundTrips(t *testing.T) {
	random := rand.New(rand.NewSource(571))
	raw := make([]byte, 32768)
	_, _ = random.Read(raw)
	hexadecimal := []byte(hex.EncodeToString(raw))
	codec, cleanup, err := newWriterCodec("go-entropy")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	encoded, err := codec.Encode(nil, hexadecimal)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) >= len(hexadecimal)*3/4 {
		t.Fatalf("skewed hex remained mostly raw: input=%d encoded=%d", len(hexadecimal), len(encoded))
	}
	baseline, _, err := newWriterCodec("go")
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := baseline.Decode(nil, encoded)
	if err != nil || !bytes.Equal(decoded, hexadecimal) {
		t.Fatalf("Go baseline cannot decode entropy frame: %v", err)
	}
	var workers sync.WaitGroup
	for range 4 {
		workers.Go(func() {
			for _, input := range [][]byte{nil, {0}, hexadecimal[:1024]} {
				encoded, err := codec.Encode(nil, input)
				if err != nil {
					t.Error(err)
					return
				}
				if len(encoded) < 5 || encoded[4]&4 != 0 {
					t.Error("entropy codec changed empty frame or checksum policy")
					return
				}
				decoded, err := codec.Decode(nil, encoded)
				if err != nil || !bytes.Equal(decoded, input) {
					t.Errorf("entropy frame roundtrip: %v", err)
					return
				}
			}
		})
	}
	workers.Wait()
	if _, err := codec.Decode(nil, []byte("corrupt")); err == nil {
		t.Fatal("corrupt frame accepted")
	}
	if err := cleanup(); err != nil {
		t.Fatal(err)
	}
	if _, err := codec.Encode(nil, nil); err != io.ErrClosedPipe {
		t.Fatalf("closed encoder error=%v", err)
	}
	if _, err := codec.Decode(nil, encoded); err != io.ErrClosedPipe {
		t.Fatalf("closed decoder error=%v", err)
	}
}

func TestEntropyCodecParquetAndOutputFailures(t *testing.T) {
	rows := valueParityRows()
	config := writerConfig{
		Layout: "served", KeyEncoding: "delta", IntegerEncoding: "delta", PageStatistics: false,
		PageRows: 16, GroupRows: 40, RowAPI: "columns", Codec: "go-entropy", ETagEncoding: "delta", SinglePageOrder: "ascending",
	}
	path := filepath.Join(t.TempDir(), "entropy.parquet")
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
			t.Fatalf("entropy output failure returned measurements: %+v %v", result, err)
		}
	}
}
