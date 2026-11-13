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

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/schema"
)

// partDigests are a part's bytes split by what the SeekParquet switch may
// change: Data, every column chunk's pages (dictionary and data pages, as
// the chunk metadata locates them); Meta, the footer's FileMetaData
// without its key/value metadata, created_by and the page index
// locations; Offsets, every OffsetIndex; Values, every ColumnIndex but the
// key column's; Keys, the key column's ColumnIndexes.
type partDigests struct{ Data, Meta, Offsets, Values, Keys string }

// footerOnlyGolden are the digests of the parts footerOnlyScenarios write,
// a frozen golden written by the 2026-11-05 layout (a1177c7f, parquet-go
// v0.32.0, klauspost/compress v1.19.1) on linux/amd64 with AVX2, its pages
// compressed again at the Recipe's zstd level (swath/parquet-r4): Data,
// Meta and Offsets follow the pages; Values and Keys are the layout's.
var footerOnlyGolden = map[string][]partDigests{
	"unique": {
		{Data: "cf04179756b286dae6b6fb637daad103943731c3562aed0d9890e953921cd8b8", Meta: "947c40e4bcd0b7299d487e9714d84deaa1048c933d83d7b4c07b2343f5ae59e9", Offsets: "798b444ab42de13f01f4e196842f45de3ff77884e1c31a0e8827d7bcace536b8", Values: "0bf0f8858dc591df5a10068543f218217565156794dea437af2363ad1b2975d3", Keys: "ed8e65dd97f16aa43ed9210936b012a947a5ee861776d6d00e75d9de8de9e35c"},
		{Data: "2aada266be78d9f1eb668c59401e3db733be4f6a48ff8eb261d7c9a6a206b172", Meta: "fc70af5507d0ad803db679715b314a5766083ebac93eeefb05470da8be6b68f1", Offsets: "37b84d3cbf7c110d2ae58c4f35016d6a3a02a3b51ffab4facbbd4bb8df361391", Values: "a497c6dd0e323ba87d947fdfc2548d6e453fef5c5106afb9d254106d866632ba", Keys: "7a33dbf04cc2655e548d1c50bf64b4bf17c7ea07fe6522b8965a0e504de70b47"},
	},
	"grouped": {{Data: "9629f3b0012c4c3caa9b29e6d892f359d43241d82f33f0af619233e2995503cf", Meta: "7c8209698e80450acd4472dfcf7e286fd87bb977d9eee326d2fd84f00a22b428", Offsets: "56313c0ec77786561a4e07d9fe984ae3aff07b1be965ea4e371b253e141cc7c0", Values: "090823997d879c764370a4bf56853645ef4465d5eb279a76a1ce5d005a26e63e", Keys: "a33f329a55148003782ad9791913f4898943f88bb8bc143f1ee2756c47773353"}},
	"long":    {{Data: "75918583c245d092240a5771882b587ffc9dd16ab3027f5984d33e9ec7b842f9", Meta: "1a6add7ce5c480f32994081bf8be43a8642c9905b3409150d1c92cb9f663d952", Offsets: "86829989ef6a66466d8d7755cec158aac700a55e8588b0d9e1df697e5d761b54", Values: "d882cc46a48207f5d6f0396b4aea0873a39c19867e56644bdeda53cbe0bdba68", Keys: "158a647bdd1ebdc4702f242f45572402e657b0079def7cb557c22c28e0b3fc3e"}},
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
var footerOnlyGeneric = partDigests{Data: "a3efd2cbe7558aa86cb32dad5cedb7d0dcddbe50b6e539a271ea982714bcbbed", Meta: "81cd4f917dc53408cb5bd300677d62999d94e9d643aef56131586fd3f37b9024", Offsets: "d4e615b09d3828af88a912c502a748e59572b10a1b7a64b3d863b6b1242cffc1", Values: "090823997d879c764370a4bf56853645ef4465d5eb279a76a1ce5d005a26e63e", Keys: "a33f329a55148003782ad9791913f4898943f88bb8bc143f1ee2756c47773353"}

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
// same footer as the frozen golden (written by the 2026-11-05 layout)
// records, apart from the footer stamps and the key column's index bounds,
// which are now exact; a part whose keys fit within 1,024 bytes has the
// same key index too.
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
			stamps := map[string]string{seekparquet.StampFormatVersion: "0", seekparquet.StampDraftRevision: "1", seekparquet.StampSchemaID: footerOnlySchemaIDs[name],
				seekparquet.StampKeyColumn: "key", seekparquet.StampKeyUnique: strconv.FormatBool(name != "grouped")}
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
