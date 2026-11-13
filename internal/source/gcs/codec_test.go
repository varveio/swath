// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"bytes"
	"encoding/hex"
	"math"
	"testing"

	"github.com/varveio/swath/go/internal/schema"
)

// TestDeltaHistoricalBytes pins bytes captured from the provider codec before
// sharing the delta implementation, including integer wraparound and repeats.
func TestDeltaHistoricalBytes(t *testing.T) {
	rows := []Generation{
		{},
		{Object: Object{Size: 7, Mtime: -8, Generation: 123, Metageneration: 2, TimeCreated: 45, ETag: "etag", StorageClass: "STANDARD", MD5: "md5", CRC32C: "crc", ContentType: "text/plain"}, TimeDeleted: 99},
		{Object: Object{Size: 8, Mtime: -9, Generation: 124, Metageneration: 2, ETag: "etag2", StorageClass: "STANDARD", CRC32C: "crc", ContentType: "application/octet-stream"}},
		{Object: Object{Size: math.MaxInt64, Mtime: math.MinInt64, Generation: -1, Metageneration: math.MinInt64, TimeCreated: math.MaxInt64}, TimeDeleted: math.MinInt64},
		{Object: Object{Size: math.MinInt64, Mtime: math.MaxInt64, Generation: math.MaxInt64, Metageneration: math.MaxInt64, TimeCreated: math.MinInt64}, TimeDeleted: math.MaxInt64},
		{},
		{},
	}
	want := []string{
		"1f000000000000",
		"000e0ff601045ac6010465746167085354414e44415244036d6435036372630a746578742f706c61696e",
		"0a0201020059c50105657461673200186170706c69636174696f6e2f6f637465742d73747265616d",
		"04eeffffffffffffffff01edffffffffffffffff01f901fcffffffffffffffff01feffffffffffffffff01ffffffffffffffffff0100000000",
		"1f0201ffffffffffffffffff01010201",
		"1fffffffffffffffffff01fdffffffffffffffff01fdffffffffffffffff01fdffffffffffffffff01ffffffffffffffffff01fdffffffffffffffff01",
		"1f000000000000",
	}
	var prev Generation
	c := generationCodec{}
	for i, row := range rows {
		encoded, err := hex.DecodeString(want[i])
		if err != nil {
			t.Fatal(err)
		}
		if got := c.AppendDelta([]byte{0xaa}, &prev, &row); !bytes.Equal(got, append([]byte{0xaa}, encoded...)) {
			t.Fatalf("row %d: %x, want aa%s", i, got, want[i])
		}
		src := bytes.Clone(encoded)
		d := schema.NewOwningDecoder(src, 1024)
		var got Generation
		c.DecodeDelta(d, &prev, &got)
		if d.Err() != nil || d.Len() != 0 {
			t.Fatalf("row %d: %v, %d left", i, d.Err(), d.Len())
		}
		if got != row {
			t.Fatalf("row %d: %+v, want %+v", i, got, row)
		}
		prev = row
	}
}
