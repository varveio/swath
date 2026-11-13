// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/reader"
)

// Generation is one generation of a Cloud Storage object, live or
// noncurrent (objects.list with versions=true). Rows of one name are
// consecutive, generation ascending.
type Generation struct {
	Object
	// TimeDeleted is when the generation became noncurrent, UTC epoch
	// microseconds; 0 (null) for the live generation. A name can have
	// noncurrent generations and no live one.
	TimeDeleted int64
}

// GenerationRow is one listed generation.
type GenerationRow = schema.Row[Generation]

// GenerationSchema is the GCS generations schema: the gcs-objects/v1
// columns in the same order, then time_deleted. There is no is_latest
// column: the live generation is the one whose time_deleted is null.
var GenerationSchema = schema.Bind(reader.GCSGenerationsV1, schema.Schema[Generation]{
	Info: schema.Info{Name: "swath_listing", StorageClass: "storage_class"},
	Columns: append(lift(Schema.Columns),
		schema.Column[Generation]{Int: func(g *Generation) (int64, bool) { return g.TimeDeleted, g.TimeDeleted != 0 }}),
	Bytes: func(g *Generation) int64 { return 48 + objectHeap(&g.Object) }, // six integers of 8 bytes
	Heap:  func(g *Generation) int64 { return objectHeap(&g.Object) },
	Codec: generationCodec{},
})

// lift binds the current-object columns to a Generation's Object, so the
// two schemas share their columns' names, types and accessors.
func lift(cols []schema.Column[Object]) []schema.Column[Generation] {
	out := make([]schema.Column[Generation], len(cols))
	for i, c := range cols {
		out[i] = schema.Column[Generation]{Spec: c.Spec, Delta: c.Delta}
		switch {
		case c.Str != nil:
			out[i].Str = func(g *Generation) (string, bool) { return c.Str(&g.Object) }
		case c.Int != nil:
			out[i].Int = func(g *Generation) (int64, bool) { return c.Int(&g.Object) }
		case c.Bool != nil:
			out[i].Bool = func(g *Generation) (bool, bool) { return c.Bool(&g.Object) }
		}
	}
	return out
}

// generationCodec is the spill encoding of a Generation: the Object's,
// then the time deleted.
type generationCodec struct{}

func (generationCodec) Append(dst []byte, g *Generation) []byte {
	return binary.AppendVarint(objectCodec{}.Append(dst, &g.Object), g.TimeDeleted)
}

func (generationCodec) Decode(d *schema.Decoder, g *Generation) {
	objectCodec{}.Decode(d, &g.Object)
	g.TimeDeleted = d.Varint()
}

func generationInts(g *Generation) [6]*int64 {
	return [6]*int64{&g.Size, &g.Mtime, &g.Generation, &g.Metageneration, &g.TimeCreated, &g.TimeDeleted}
}

func generationStrings(g *Generation) [5]*string {
	return [5]*string{&g.ETag, &g.StorageClass, &g.MD5, &g.CRC32C, &g.ContentType}
}

func (generationCodec) AppendDelta(dst []byte, prev, g *Generation) []byte {
	pi, ci := generationInts(prev), generationInts(g)
	ps, cs := generationStrings(prev), generationStrings(g)
	return schema.AppendDelta(dst, schema.DeltaFields{Ints: pi[:], Strings: ps[:]}, schema.DeltaFields{Ints: ci[:], Strings: cs[:]})
}

func (generationCodec) DecodeDelta(d *schema.Decoder, prev, g *Generation) {
	pi, ci := generationInts(prev), generationInts(g)
	ps, cs := generationStrings(prev), generationStrings(g)
	schema.DecodeDelta(d, schema.DeltaFields{Ints: pi[:], Strings: ps[:]}, schema.DeltaFields{Ints: ci[:], Strings: cs[:]})
}

var _ schema.DeltaCodec[Generation] = generationCodec{}

// fetchGenerations is the generations engine's page fetch: objects.list
// with versions=true. A physical seek sends the exclusive successor of
// After where the bound policy accepts it, else After itself (the engine
// then drops After's rows: Description.InclusiveStart). A page whose
// generations of one name do not ascend is invalid.
func (p *provider[P]) fetchGenerations(ctx context.Context, q *keyrange.Query) (*keyrange.Page[Generation], error) {
	in := p.input(q, true)
	out, err := p.client.List(ctx, in)
	if err != nil {
		return nil, err
	}
	rows := make([]GenerationRow, len(out.Items))
	for i := range out.Items {
		o, r := &out.Items[i], &rows[i]
		r.Key = o.Name
		r.Data = Generation{Object: object(o), TimeDeleted: o.TimeDeleted}
		if i > 0 && r.Key == rows[i-1].Key && r.Data.Generation <= rows[i-1].Data.Generation {
			return nil, fmt.Errorf("%w: generation %d of %q after %d", objhttp.ErrInvalidPage, r.Data.Generation, r.Key, rows[i-1].Data.Generation)
		}
	}
	return &keyrange.Page[Generation]{Rows: rows, Prefixes: out.Prefixes, Truncated: out.NextPageToken != "",
		Token: out.NextPageToken}, nil
}
