// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"context"
	"fmt"
	"testing"
)

// BenchmarkProviderEmit measures emission of an already listed and sorted
// directory, including the work to hand off child ranges.
func BenchmarkProviderEmit(b *testing.B) {
	const rowCount = 2048
	for _, tc := range []struct {
		name   string
		mixed  bool
		noDirs bool
	}{
		{name: "files"},
		{name: "mixed", mixed: true},
		{name: "mixed_no_dirs", mixed: true, noDirs: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			p, err := New(b.TempDir(), Options{Fields: Stat, NoDirs: tc.noDirs})
			if err != nil {
				b.Fatal(err)
			}
			r := p.Root()
			var st Stats
			d, ok, err := p.openPortable(context.Background(), r, &st)
			if !ok || err != nil {
				b.Fatalf("open root: ok=%v, err=%v, stats=%+v", ok, err, st)
			}
			defer func() {
				d.release()
				if open := p.keep.open.Load(); open != 0 {
					b.Errorf("kept %d directory descriptors after emission", open)
				}
			}()

			rows := make([]Row, rowCount)
			var childCount int
			var bytes int64
			for i := range rows {
				key := fmt.Sprintf("entry-%04d", i)
				if tc.mixed && i%16 == 15 {
					rows[i] = Row{Key: key + "/", Data: Entry{Kind: KindDir, Present: HasStat, Mode: 0755}}
					childCount++
				} else {
					size := int64(4096 + i)
					rows[i] = Row{Key: key, Data: Entry{Kind: KindFile, Present: HasStat, Mode: 0644, Size: size}}
					bytes += size
				}
			}
			wantRows := rowCount
			if tc.noDirs {
				wantRows -= childCount
			}
			var sink emitBenchmarkSink
			b.ReportAllocs()
			for b.Loop() {
				if err := p.emit(r, d, rows, &sink, &st); err != nil {
					b.Fatal(err)
				}
			}
			if sink.rows != b.N*wantRows || sink.children != b.N*childCount {
				b.Fatalf("emitted %d rows and %d children, want %d and %d", sink.rows, sink.children, b.N*wantRows, b.N*childCount)
			}
			if st.Files != int64(b.N)*int64(rowCount-childCount) || st.Dirs != int64(sink.children) || st.Bytes != int64(b.N)*bytes {
				b.Fatalf("wrong file, directory or byte counts after %d iterations: %+v", b.N, st)
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*rowCount), "ns/row")
		})
	}
}

// The sink consumes its input synchronously so each iteration can reuse the
// fixture after the emitter has transferred ownership.
type emitBenchmarkSink struct {
	rows     int
	children int
}

func (s *emitBenchmarkSink) Rows(rows []Row) error {
	s.rows += len(rows)
	return nil
}

func (s *emitBenchmarkSink) Child(r Range) error {
	s.children++
	r.at.release()
	return nil
}
