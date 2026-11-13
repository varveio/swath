// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquetout

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
	pq "github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

// BenchmarkPageCodec compresses the pages of a real part, the part at
// SWATH_CODEC_BENCH_PART (skipped when unset), at the page level and at
// SpeedDefault, with the codec's other options: what each page's
// compression costs (ns/page on one state, so CPU time) and gives
// (compressed bytes a page and the ratio to the raw bytes). The pages are
// the part's own, decompressed: a dictionary page or a v2 data page's
// values (its levels are stored uncompressed).
func BenchmarkPageCodec(b *testing.B) {
	path := os.Getenv("SWATH_CODEC_BENCH_PART")
	if path == "" {
		b.Skip("SWATH_CODEC_BENCH_PART names no part")
	}
	pages, columns := rawPages(b, path)
	var raw int64
	for _, p := range pages {
		raw += int64(len(p))
	}
	for _, c := range []struct {
		name  string
		level zstd.EncoderLevel
	}{{"default", zstd.SpeedDefault}, {"fastest", zstd.SpeedFastest}} {
		b.Run(c.name, func(b *testing.B) {
			e, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(c.level), zstd.WithEncoderCRC(false),
				zstd.WithZeroFrames(true), zstd.WithAllLitEntropyCompression(true), zstd.WithLowerEncoderMem(true))
			if err != nil {
				b.Fatal(err)
			}
			var dst []byte
			var out int64
			perColumn := map[string]int64{}
			b.SetBytes(raw)
			b.ResetTimer()
			for b.Loop() {
				out = 0
				for i, p := range pages {
					dst = e.EncodeAll(p, dst[:0])
					out += int64(len(dst))
					perColumn[columns[i]] += int64(len(dst))
				}
			}
			n := float64(b.N) * float64(len(pages))
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/n, "ns/page")
			b.ReportMetric(float64(out)/float64(len(pages)), "B/page")
			b.ReportMetric(float64(raw)/float64(out), "ratio")
			for col, v := range perColumn {
				b.Logf("%s: %s %d B (of %d pages, %d raw B total)", c.name, col, v/int64(b.N), len(pages), raw)
			}
		})
	}
}

// rawPages is every page of the part at path, decompressed, and its
// column's path.
func rawPages(tb testing.TB, path string) (pages [][]byte, columns []string) {
	tb.Helper()
	f, err := os.ReadFile(path)
	if err != nil {
		tb.Fatal(err)
	}
	pf, err := pq.OpenFile(bytes.NewReader(f), int64(len(f)))
	if err != nil {
		tb.Fatal(err)
	}
	dec, err := zstd.NewReader(nil)
	if err != nil {
		tb.Fatal(err)
	}
	defer dec.Close()
	for _, rg := range pf.Metadata().RowGroups {
		for _, cc := range rg.Columns {
			m := cc.MetaData
			off := m.DataPageOffset
			if m.DictionaryPageOffset > 0 && m.DictionaryPageOffset < off {
				off = m.DictionaryPageOffset
			}
			for end := off + m.TotalCompressedSize; off < end; {
				r := bytes.NewReader(f[off:end])
				var h format.PageHeader
				if err := thrift.NewDecoder(new(thrift.CompactProtocol).NewReader(r)).Decode(&h); err != nil {
					tb.Fatal(err)
				}
				body := f[end-int64(r.Len()):][:h.CompressedPageSize]
				off = end - int64(r.Len()) + int64(h.CompressedPageSize)
				compressed := true
				if v2, ok := h.DataPageHeaderV2.Get(); ok {
					body = body[v2.RepetitionLevelsByteLength+v2.DefinitionLevelsByteLength:]
					if c, ok := v2.IsCompressed.Get(); ok {
						compressed = c
					}
				}
				if !compressed || len(body) == 0 {
					continue
				}
				p, err := dec.DecodeAll(body, nil)
				if err != nil {
					tb.Fatal(err)
				}
				pages, columns = append(pages, p), append(columns, strings.Join(m.PathInSchema, "."))
			}
		}
	}
	return pages, columns
}
