// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob

import (
	"bytes"
	"encoding/hex"
	"errors"
	"math"
	"testing"

	"github.com/varveio/swath/go/internal/schema"
)

// TestDeltaHistoricalBytes pins bytes captured from the provider codec before
// sharing the delta implementation, including integer wraparound and repeats.
func TestDeltaHistoricalBytes(t *testing.T) {
	rows := []Object{
		{},
		{Size: 7, Mtime: -8, CreationTime: 123, ETag: "etag", ContentMD5: "md5", ContentType: "text/plain", BlobType: "BlockBlob", AccessTier: "Hot"},
		{Size: 8, Mtime: -9, ETag: "etag2", ContentMD5: "md5", BlobType: "BlockBlob", AccessTier: "Cool"},
		{Size: math.MaxInt64, Mtime: math.MinInt64, CreationTime: -1},
		{Size: math.MinInt64, Mtime: math.MaxInt64, CreationTime: math.MaxInt64},
		{},
		{},
	}
	want := []string{
		"1f000000",
		"000e0ff6010465746167036d64350a746578742f706c61696e09426c6f636b426c6f6203486f74",
		"0a0201f5010565746167320004436f6f6c",
		"04eeffffffffffffffff01edffffffffffffffff010100000000",
		"1f0201ffffffffffffffffff01",
		"1fffffffffffffffffff01fdffffffffffffffff01fdffffffffffffffff01",
		"1f000000",
	}
	var prev Object
	c := objectCodec{}
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
		var got Object
		c.DecodeDelta(d, &prev, &got)
		if d.Err() != nil || d.Len() != 0 {
			t.Fatalf("row %d: %v, %d left", i, d.Err(), d.Len())
		}
		clear(src)
		if got != row {
			t.Fatalf("row %d: %+v, want %+v", i, got, row)
		}
		for end := range len(encoded) {
			d = schema.NewOwningDecoder(encoded[:end], 1024)
			c.DecodeDelta(d, &prev, &Object{})
			if !errors.Is(d.Err(), schema.ErrCodec) {
				t.Fatalf("row %d cut at %d: %v", i, end, d.Err())
			}
		}
		for bit := uint(5); bit < 8; bit++ {
			bad := bytes.Clone(encoded)
			bad[0] |= 1 << bit
			d = schema.NewDecoder(bad)
			c.DecodeDelta(d, &prev, &Object{})
			if !errors.Is(d.Err(), schema.ErrCodec) {
				t.Fatalf("row %d flag %d: %v", i, bit, d.Err())
			}
		}
		prev = row
	}
}
