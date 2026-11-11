// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob

import (
	"bytes"
	"encoding/hex"
	"math"
	"testing"

	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source/azblob/client"
)

// TestDeltaHistoricalBytes pins bytes captured from the provider codec before
// sharing the delta implementation, including integer wraparound and repeats:
// the first eight columns' group, then, for rows without the properties added
// later, their groups repeating the previous row's (unchanged) and an
// all-unset booleans byte.
func TestDeltaHistoricalBytes(t *testing.T) {
	const unchanged = "ff00000000" + "01" + "00"
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
		encoded, err := hex.DecodeString(want[i] + unchanged)
		if err != nil {
			t.Fatal(err)
		}
		if got := c.AppendDelta([]byte{0xaa}, &prev, &row); !bytes.Equal(got, append([]byte{0xaa}, encoded...)) {
			t.Fatalf("row %d: %x, want aa%s%s", i, got, want[i], unchanged)
		}
		src := bytes.Clone(encoded)
		d := schema.NewOwningDecoder(src, 1024)
		var got Object
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

// TestCodecProperties: rows with the later properties round-trip through
// the plain and the delta encoding, each of the three booleans in each
// state, and a booleans byte no Tri has is refused.
func TestCodecProperties(t *testing.T) {
	rows := []Object{
		{Size: 1, Props: client.Props{AccessTierInferred: client.True, ServerEncrypted: client.False, LeaseState: "available",
			LastAccessTime: 5, ContentEncoding: "gzip", TagCount: 2, Tags: `{"a":"b"}`, Metadata: `{"m":"1"}`}},
		{Size: 2, Props: client.Props{AccessTierInferred: client.True, ServerEncrypted: client.True, LeaseState: "available",
			ArchiveStatus: "rehydrate-pending-to-cool", RehydratePriority: "High", AccessTierChangeTime: -3, EncryptionScope: "s",
			CustomerProvidedKeySHA256: "k", ImmutabilityUntil: 9, ImmutabilityMode: "Unlocked", LegalHold: client.True, Tags: `{"a":"b"}`}},
		{Size: 3, Props: client.Props{LegalHold: client.False, Metadata: "{}"}},
		{},
	}
	c := objectCodec{}
	var prev Object
	for i, row := range rows {
		var got, gotDelta Object
		d := schema.NewOwningDecoder(c.Append(nil, &row), 1024)
		if c.Decode(d, &got); d.Err() != nil || d.Len() != 0 || got != row {
			t.Fatalf("row %d: %+v, %v, %d left", i, got, d.Err(), d.Len())
		}
		d = schema.NewOwningDecoder(c.AppendDelta(nil, &prev, &row), 1024)
		if c.DecodeDelta(d, &prev, &gotDelta); d.Err() != nil || d.Len() != 0 || gotDelta != row {
			t.Fatalf("row %d delta: %+v, %v, %d left", i, gotDelta, d.Err(), d.Len())
		}
		prev = row
	}
	for _, b := range []byte{0x03, 0x0c, 0x30, 0x40} {
		enc := c.Append(nil, &Object{})
		enc[len(enc)-1] = b
		var o Object
		d := schema.NewOwningDecoder(enc, 1024)
		if c.Decode(d, &o); d.Err() == nil {
			t.Errorf("booleans byte %#x accepted: %+v", b, o.Props)
		}
	}
}
