// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquetout

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"

	pq "github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
	"golang.org/x/sys/cpu"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/schema"
)

// partDigests are a part's bytes split by what the SeekParquet switch may
// change: Data, every column chunk's pages (dictionary and data pages, as
// the chunk metadata locates them); Meta, the footer's FileMetaData
// without its key/value metadata, created_by and the page index
// locations; Offsets, every OffsetIndex; Values, every ColumnIndex but the
// key column's; Keys, the key column's ColumnIndexes.
type partDigests struct{ Data, Meta, Offsets, Values, Keys string }

// footerOnlyGolden are the digests of the parts footerOnlyScenarios write,
// recorded with the swath-dataset format 2 writer (a1177c7f, parquet-go
// v0.32.0, klauspost/compress v1.19.1) on linux/amd64 with AVX2.
var footerOnlyGolden = map[string][]partDigests{
	"unique": {
		{Data: "2626b20b48e07c6363f58dae1c974e0d82619aa63834c42467be875eb40bcaea", Meta: "b2831a92f707b173044531061cf4092786ca4709bc79dc9e65c7695519906649", Offsets: "1f6bada2ee2378ea8b24bd445ecf75e188617c4ed5760ea7e764db80e72c4f3a", Values: "0bf0f8858dc591df5a10068543f218217565156794dea437af2363ad1b2975d3", Keys: "ed8e65dd97f16aa43ed9210936b012a947a5ee861776d6d00e75d9de8de9e35c"},
		{Data: "5302c228ead3bfd86e7b82ae9a9c5b3ca0747ad904d3ed77383bf6c9b8524e8c", Meta: "36b39ced2612dbff3c54569b720c9f8d693901b1ff38f3aa7dc826200f48ffda", Offsets: "51cbf6606faf6fc3593dde3ad79371fc7b849ad9e913e28a309845ba0342756e", Values: "a497c6dd0e323ba87d947fdfc2548d6e453fef5c5106afb9d254106d866632ba", Keys: "7a33dbf04cc2655e548d1c50bf64b4bf17c7ea07fe6522b8965a0e504de70b47"},
	},
	"grouped": {{Data: "16fe9928745fd00ed3d8b752ea9f2b38a4f35d32c1a82d7c4156c21460658c41", Meta: "5ffa53f7f84ea101b28d4b48b08f5e05ff1df28a85cc9581d15f7fb553cb42ce", Offsets: "b71219a3b31030e798ef434be128fca06326076ed9cbbe7775ac81546d357b4b", Values: "090823997d879c764370a4bf56853645ef4465d5eb279a76a1ce5d005a26e63e", Keys: "a33f329a55148003782ad9791913f4898943f88bb8bc143f1ee2756c47773353"}},
	"long":    {{Data: "fe7141fc6afc1b6346a876813a1d37d9505ce4c97bd12c5039ef95a52848af8a", Meta: "013d49896f6cd4129793134e13d19f4065b6675d3073ea494462c90821b90e03", Offsets: "4ee257c4e5d7e6eec8fdff7343869c5fc3c6bf0bb3fc6fac34a432929d6791ee", Values: "d882cc46a48207f5d6f0396b4aea0873a39c19867e56644bdeda53cbe0bdba68", Keys: "158a647bdd1ebdc4702f242f45572402e657b0079def7cb557c22c28e0b3fc3e"}},
}

// footerOnlyGeneric are the grouped part's digests on parquet-go's generic
// RLE path: every platform but amd64 with AVX2 (darwin/arm64, linux/arm64,
// and amd64 without AVX2 or built with the purego tag; CI #305). parquet-go
// v0.32.0's AVX2 kernel that ends a bit-packed run
// (encodeInt32IndexEqual8ContiguousAVX2) broadcasts each 128-bit lane's
// first value (VPSHUFD on a YMM register), so it also stops at a group of
// eight dictionary indexes whose two halves are each uniform; the generic
// code and the SSE kernel stop only at eight equal ones. Both encodings
// are valid and decode to the same values; the dictionary-encoded kind
// column of the grouped rows meets such a group, the other scenarios do
// not. Only the data pages and what locates them differ: Data, Meta (the
// chunk sizes and offsets) and Offsets.
var footerOnlyGeneric = partDigests{Data: "90eebb8a485c65eac46604a89805e4fa95c8ef844257463dfc7f56a274138235", Meta: "24abfb0b9b822e39f9be53b2481d8d0dfa53e607981f36cc8435290799cf8ddb", Offsets: "a3bf42f2bdf7dada5977dc185d2e249757a0e72afb6077fc353d85f0527953c7", Values: "090823997d879c764370a4bf56853645ef4465d5eb279a76a1ce5d005a26e63e", Keys: "a33f329a55148003782ad9791913f4898943f88bb8bc143f1ee2756c47773353"}

// rleAVX2 reports whether parquet-go encodes dictionary indexes with its
// AVX2 kernels here (its rle_amd64.go).
var rleAVX2 = runtime.GOARCH == "amd64" && cpu.X86.HasAVX2 && !puregoBuild

// footerOnlyScenarios write short string keys (unique and repeated) and
// binary keys longer than 1,024 bytes, some beginning with more than
// 1,024 0xFF bytes: the keys whose index maximum
// parquet-go shortens to an all-0xFF value that is below them.

var footerOnlySchemaIDs = map[string]string{"unique": testSchema.ID, "grouped": groupedSchema.ID, "long": "test/binary-key"}

func footerOnlyScenarios() map[string]func(t *testing.T) []dataset.Part {
	binary := testSchema
	binary.Info.ID, binary.Info.Key = footerOnlySchemaIDs["long"], schema.Binary
	var long []row
	for i := range 3000 {
		lead := strings.Repeat("a", 1100)
		if i >= 1500 {
			lead = strings.Repeat("\xff", 1100)
		}
		long = append(long, row{Key: lead + string(rune('A'+i/100%26)) + strings.Repeat("z", i%7) + string(rune('0'+i%10)) + strings.Repeat("\x00", i%100),
			Data: tr{Kind: "FILE", Size: int64(i), Mtime: int64(i) << 20, ETag: "e"}})
	}
	slices.SortFunc(long, func(a, b row) int { return strings.Compare(a.Key, b.Key) })
	long = slices.CompactFunc(long, func(a, b row) bool { return a.Key == b.Key })
	write := func(s *schema.Schema[tr], rows []row, maxRows int64) func(t *testing.T) []dataset.Part {
		return func(t *testing.T) []dataset.Part {
			t.Helper()
			out := t.TempDir()
			if err := os.MkdirAll(filepath.Join(out, dataset.DataDir), 0o755); err != nil {
				t.Fatal(err)
			}
			r, err := NewRouter(Options{Out: out, PartSize: 64 << 20, Encoders: 2, MaxRows: maxRows}, s)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < len(rows); i += 1500 {
				if err := r.Write(context.Background(), [][]row{rows[i:min(i+1500, len(rows))]}); err != nil {
					t.Fatal(err)
				}
			}
			parts, err := r.Close()
			if err != nil {
				t.Fatal(err)
			}
			return parts
		}
	}
	return map[string]func(t *testing.T) []dataset.Part{
		"unique":  write(&testSchema, testRows(6000, 11), 4000),
		"grouped": write(&groupedSchema, groupedRows(3000), 0),
		"long":    write(&binary, long, 0),
	}
}

func digestPart(t *testing.T, path string) (partDigests, *format.FileMetaData, []byte) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	pf, err := pq.OpenFile(bytes.NewReader(b), int64(len(b)))
	if err != nil {
		t.Fatal(err)
	}
	md := *pf.Metadata()
	sum := func(h []byte) string { s := sha256.Sum256(h); return hex.EncodeToString(s[:]) }
	var data, offsets, values, keys []byte
	md.RowGroups = slices.Clone(md.RowGroups)
	for g := range md.RowGroups {
		rg := &md.RowGroups[g]
		rg.Columns = slices.Clone(rg.Columns)
		for c := range rg.Columns {
			cc := &rg.Columns[c]
			start := cc.MetaData.DataPageOffset
			if cc.MetaData.DictionaryPageOffset > 0 {
				start = min(start, cc.MetaData.DictionaryPageOffset)
			}
			data = append(data, b[start:start+cc.MetaData.TotalCompressedSize]...)
			offsets = append(offsets, b[cc.OffsetIndexOffset:cc.OffsetIndexOffset+int64(cc.OffsetIndexLength)]...)
			index := b[cc.ColumnIndexOffset : cc.ColumnIndexOffset+int64(cc.ColumnIndexLength)]
			if c == 0 {
				keys = append(keys, index...)
			} else {
				values = append(values, index...)
			}
			cc.OffsetIndexOffset, cc.ColumnIndexOffset, cc.ColumnIndexLength = 0, 0, 0
		}
	}
	kv := md.KeyValueMetadata
	md.KeyValueMetadata, md.CreatedBy = nil, ""
	meta, err := thrift.Marshal(new(thrift.CompactProtocol), &md)
	if err != nil {
		t.Fatal(err)
	}
	md.KeyValueMetadata = kv
	return partDigests{Data: sum(data), Meta: sum(meta), Offsets: sum(offsets), Values: sum(values), Keys: sum(keys)}, pf.Metadata(), b
}

// TestFooterOnlyChange: the SeekParquet writer writes the same data
// pages, the same offset indexes, the same value-column indexes and the
// same footer as the format 2 writer did, apart from the footer stamps and
// the key column's index bounds, which are now exact; a part whose keys
// fit within 1,024 bytes has the same key index too.
func TestFooterOnlyChange(t *testing.T) {
	for name, write := range footerOnlyScenarios() {
		parts := write(t)
		var got []partDigests
		for _, p := range parts {
			d, _, _ := digestPart(t, p.Path)
			got = append(got, d)
		}
		want, ok := footerOnlyGolden[name]
		if !ok {
			t.Errorf("%s: no golden; this writer gives %#v", name, got)
			continue
		}
		if len(got) != len(want) {
			t.Fatalf("%s: %d parts, golden %d", name, len(got), len(want))
		}
		for i := range got {
			g, w := got[i], want[i]
			if name == "grouped" && !rleAVX2 {
				w = footerOnlyGeneric
			}
			if name == "long" {
				g.Keys, w.Keys = "", "" // keys over 1,024 bytes: the bounds are now exact (exactKeyBounds)
			}
			if g != w {
				t.Errorf("%s part %d: %+v, golden %+v", name, i, g, w)
			}
			kv := footer(t, parts[i].Path)
			stamps := map[string]string{dataset.StampFormatVersion: "0", dataset.StampDraftRevision: "1", dataset.StampSchema: footerOnlySchemaIDs[name],
				dataset.StampKeyColumn: "key", dataset.StampKeyUnique: strconv.FormatBool(name != "grouped")}
			if !maps.Equal(kv, stamps) {
				t.Errorf("%s part %d: footer key/value metadata %q, want %q", name, i, kv, stamps)
			}
			exactKeyBounds(t, parts[i].Path)
		}
	}
}

// exactKeyBounds checks that every page's key index bounds are its first
// and last key exactly.
func exactKeyBounds(t *testing.T, path string) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	pf, err := pq.OpenFile(bytes.NewReader(b), int64(len(b)))
	if err != nil {
		t.Fatal(err)
	}
	for g, rg := range pf.RowGroups() {
		cc := rg.ColumnChunks()[0]
		ci, err := cc.ColumnIndex()
		if err != nil {
			t.Fatal(err)
		}
		oi, err := cc.OffsetIndex()
		if err != nil {
			t.Fatal(err)
		}
		var keys []string
		pages := cc.Pages()
		for {
			p, err := pages.ReadPage()
			if err != nil {
				break
			}
			values := make([]pq.Value, p.NumValues())
			n, _ := p.Values().ReadValues(values)
			for _, v := range values[:n] {
				keys = append(keys, string(v.ByteArray()))
			}
		}
		_ = pages.Close()
		if int64(len(keys)) != rg.NumRows() || ci.NumPages() != oi.NumPages() {
			t.Fatalf("%s row group %d: %d keys of %d rows, %d/%d index pages", path, g, len(keys), rg.NumRows(), ci.NumPages(), oi.NumPages())
		}
		for i := range ci.NumPages() {
			end := rg.NumRows()
			if i+1 < ci.NumPages() {
				end = oi.FirstRowIndex(i + 1)
			}
			first, last := keys[oi.FirstRowIndex(i)], keys[end-1]
			if lo, hi := string(ci.MinValue(i).ByteArray()), string(ci.MaxValue(i).ByteArray()); lo != first || hi != last {
				t.Fatalf("%s row group %d page %d: bounds %d/%d bytes, keys %d/%d bytes: not exact", path, g, i, len(lo), len(hi), len(first), len(last))
			}
		}
	}
}
