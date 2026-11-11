// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquetout_test

import (
	"math/rand/v2"
	"strings"
	"testing"

	pq "github.com/parquet-go/parquet-go"

	"github.com/varveio/swath/go/internal/parquetout"
	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source/s3"
)

// referenceFill is the fill this package's replaced: each value built on
// its own, with its levels.
func referenceFill[P any](s *schema.Schema[P], rows []schema.Row[P], column int) []pq.Value {
	out := make([]pq.Value, len(rows))
	if column == 0 {
		for i := range rows {
			out[i] = pq.ByteArrayValue([]byte(rows[i].Key)).Level(0, 0, 0)
		}
		return out
	}
	c := &s.Columns[column-1]
	def := 1
	if c.Required {
		def = 0
	}
	for i := range rows {
		d := &rows[i].Data
		var v pq.Value
		var ok bool
		switch {
		case c.Str != nil:
			var str string
			if str, ok = c.Str(d); ok {
				v = pq.ByteArrayValue([]byte(str))
			}
		case c.Bool != nil:
			var b bool
			if b, ok = c.Bool(d); ok {
				v = pq.BooleanValue(b)
			}
		default:
			var n int64
			if n, ok = c.Int(d); ok {
				v = pq.Int64Value(n)
				if c.Type == schema.Int32 || c.Type == schema.Uint32 {
					v = pq.Int32Value(int32(n))
				}
			}
		}
		if ok {
			out[i] = v.Level(0, def, column)
		} else {
			out[i] = pq.NullValue().Level(0, 0, column)
		}
	}
	return out
}

// TestFillMatchesReference: every column of S3 pages fills to the values
// the reference builds one by one (kind, bytes, levels, column), with
// repeated strings, runs of them, nulls and distinct neighbours.
func TestFillMatchesReference(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	pick := func(vs ...string) string { return strings.Clone(vs[r.IntN(len(vs))]) }
	mixed := s3Page(false)
	for i := range mixed {
		o := &mixed[i].Data
		o.StorageClass = pick("STANDARD", "STANDARD", "GLACIER", "")
		o.OwnerID, o.OwnerDisplayName = pick("", "", "owner"), pick("", "name")
		o.ChecksumAlgorithm, o.ChecksumType = pick("CRC32", "CRC64NVME", ""), pick("FULL_OBJECT", "COMPOSITE", "")
		if r.IntN(4) == 0 && i > 0 {
			o.ETag = mixed[i-1].Data.ETag
		}
	}
	for name, page := range map[string][]s3.Row{"repeats": s3Page(false), "distinct": s3Page(true), "mixed": mixed} {
		for column := range 1 + len(s3.Schema.Columns) {
			got := make([]pq.Value, len(page))
			parquetout.FillPage(&s3.Schema, page, column, got)
			want := referenceFill(&s3.Schema, page, column)
			for i := range want {
				g, w := got[i], want[i]
				if g.Kind() != w.Kind() || !pq.Equal(g, w) || g.RepetitionLevel() != w.RepetitionLevel() ||
					g.DefinitionLevel() != w.DefinitionLevel() || g.Column() != w.Column() {
					t.Fatalf("%s, column %d, row %d: %v, want %v", name, column, i, g, w)
				}
			}
		}
	}
}
