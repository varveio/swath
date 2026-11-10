// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source/gcs/client"
	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/objhttp"
)

// TestGenerationSchema: the generations schema is gcs-objects/v1's columns
// in the same order, specs and encodings, then a nullable time_deleted;
// its accessors read the generation's Object; it declares key groups.
func TestGenerationSchema(t *testing.T) {
	s := &GenerationSchema
	if s.ID != "swath/gcs-generations/v1" || !s.Groups || Schema.Groups || s.Key != Schema.Key || s.MaxKeyBytes != Schema.MaxKeyBytes || s.Name != Schema.Name {
		t.Fatalf("info %+v", s.Info)
	}
	if len(s.Columns) != len(Schema.Columns)+1 {
		t.Fatalf("%d columns", len(s.Columns))
	}
	g := Generation{Object: Object{Size: 7, Mtime: 2, TimeCreated: 3, ETag: "e", Generation: 1656460817987973, Metageneration: 4,
		StorageClass: "STANDARD", MD5: "m", CRC32C: "c", ContentType: "text/plain"}, TimeDeleted: 9}
	for i, c := range Schema.Columns {
		gc := s.Columns[i]
		if gc.Spec != c.Spec || gc.Delta != c.Delta {
			t.Fatalf("column %d: %+v, want %+v", i, gc.Spec, c.Spec)
		}
		switch {
		case c.Int != nil:
			v, ok := c.Int(&g.Object)
			if w, wok := gc.Int(&g); v != w || ok != wok {
				t.Fatalf("%s: %d %v, want %d %v", c.Name, w, wok, v, ok)
			}
		case c.Str != nil:
			v, ok := c.Str(&g.Object)
			if w, wok := gc.Str(&g); v != w || ok != wok {
				t.Fatalf("%s: %q %v, want %q %v", c.Name, w, wok, v, ok)
			}
		default:
			t.Fatalf("%s: no accessor", c.Name)
		}
	}
	if v, ok := s.Columns[3].Int(&g); s.Columns[3].Name != "generation" || s.Columns[3].Type != schema.Int64 || v != 1656460817987973 || !ok {
		t.Fatalf("generation: %+v %d %v", s.Columns[3].Spec, v, ok)
	}
	td := s.Columns[len(s.Columns)-1]
	if td.Spec != (schema.Spec{Name: "time_deleted", Type: schema.TimestampMicros}) {
		t.Fatalf("time_deleted: %+v", td.Spec)
	}
	if v, ok := td.Int(&g); v != 9 || !ok {
		t.Fatalf("time_deleted %d %v", v, ok)
	}
	if _, ok := td.Int(&Generation{}); ok {
		t.Fatal("a live generation's time_deleted is not null")
	}
	if s.Bytes(&g) != Schema.Bytes(&g.Object)+8 || s.Heap(&g) != Schema.Heap(&g.Object) || s.Heap(&g) != 1+8+1+1+10 {
		t.Fatalf("bytes %d, heap %d", s.Bytes(&g), s.Heap(&g))
	}
}

// group is one name's generations as a listing gives them, with absent
// fields (no hashes on a composite or encrypted object, no time created,
// the live generation's null time deleted) and extremes.
func group() []GenerationRow {
	const k = "GRCm39/README.txt"
	return []GenerationRow{
		{Key: k, Data: Generation{Object: Object{Size: 39, Mtime: 1656460818008000, TimeCreated: 1656460818008000, ETag: "CMWw",
			Generation: 1656460817987973, Metageneration: 1, StorageClass: "STANDARD", MD5: "SgDy", CRC32C: "x6Aa", ContentType: "text/plain"},
			TimeDeleted: 1656460819506000}},
		{Key: k, Data: Generation{Object: Object{Size: 39, Mtime: 1656460819506000, TimeCreated: 1656460819506000, ETag: "CNG+",
			Generation: 1656460819439441, Metageneration: 1, StorageClass: "STANDARD", MD5: "SgDy", CRC32C: "x6Aa", ContentType: "text/plain"},
			TimeDeleted: 1688688031213000}},
		{Key: k, Data: Generation{Object: Object{Size: 772, Mtime: 1688688031213000, ETag: "CJ7R", Generation: 1688688031116446,
			Metageneration: 3, StorageClass: "NEARLINE", CRC32C: "AAAA"}}},
		{Key: k, Data: Generation{Object: Object{Size: math.MinInt64, Mtime: math.MaxInt64, Generation: math.MaxInt64,
			Metageneration: math.MinInt64, TimeCreated: -1}, TimeDeleted: math.MinInt64}},
		{Key: k, Data: Generation{Object: Object{Size: math.MaxInt64, Mtime: math.MinInt64, Generation: math.MinInt64}, TimeDeleted: math.MaxInt64}},
		{Key: k + "x", Data: Generation{}},
		{Key: k + "x", Data: Generation{}},
	}
}

// TestGenerationCodec: the spill codec and the delta codec round-trip
// every field of a group's rows, absent ones and repeats included; the
// decoded rows match every field. Detach goes through the codec too.
func TestGenerationCodec(t *testing.T) {
	rows := group()
	c := generationCodec{}
	var plain, delta []byte
	var zero Generation
	prev := &zero
	for i := range rows {
		plain = c.Append(plain, &rows[i].Data)
		delta = c.AppendDelta(delta, prev, &rows[i].Data)
		prev = &rows[i].Data
	}
	d := schema.NewDecoder(plain)
	for i := range rows {
		var g Generation
		c.Decode(d, &g)
		if g != rows[i].Data {
			t.Fatalf("plain row %d: %+v, want %+v", i, g, rows[i].Data)
		}
	}
	if d.Err() != nil || d.Len() != 0 {
		t.Fatalf("plain: %v, %d left", d.Err(), d.Len())
	}
	src := append([]byte(nil), delta...)
	d = schema.NewOwningDecoder(src, 1<<10)
	got := make([]Generation, len(rows))
	prev = &zero
	for i := range rows {
		c.DecodeDelta(d, prev, &got[i])
		prev = &got[i]
	}
	if d.Err() != nil || d.Len() != 0 {
		t.Fatalf("delta: %v, %d left", d.Err(), d.Len())
	}
	for i := range rows {
		if got[i] != rows[i].Data {
			t.Fatalf("delta row %d: %+v, want %+v", i, got[i], rows[i].Data)
		}
	}
	for i, r := range GenerationSchema.Detach(rows) {
		if r != rows[i] {
			t.Fatalf("detached row %d: %+v", i, r)
		}
	}
}

// TestExclusiveStart: the successor start lists strictly after a name,
// none of its generations included, where startOffset = name would list
// them all again; a 1,024-byte name has no accepted successor, and the
// provider falls back to the inclusive start (the engine's drop).
//
// Probed live on 2026-11-08 (anonymous, gcp-public-data--broad-references,
// versions=true): startOffset = README + NUL, with README =
// A.nancymaae/86718_ANA_hifiasm-v0.15.2.pri/cleanome/bwa/v2_2_1/README.txt
// (three generations, two noncurrent), answered 200 and started at the
// next name (.../bwa-mem2-2.2.1-...pri.tar), also under
// prefix=A.nancymaae/; startOffset = README started at README's first
// generation. A 1,025-byte startOffset (1,024 'a' + NUL, or + 'b') also
// answered 200: the 1,024-byte cap is AcceptBound's, not the API's. A
// pageToken carries its own position: sent with a lower startOffset, or
// none, the page was the same.
func TestExclusiveStart(t *testing.T) {
	const dir = "A.nancymaae/86718_ANA_hifiasm-v0.15.2.pri/cleanome/bwa/v2_2_1/"
	readme := dir + "README.txt"
	start, ok := ExclusiveStart(readme)
	if !ok || start != readme+"\x00" {
		t.Fatalf("%q %v", start, ok)
	}
	if s, ok := ExclusiveStart(strings.Repeat("a", maxName-1)); !ok || len(s) != maxName {
		t.Fatalf("a 1,023-byte name: %d bytes, %v", len(s), ok)
	}
	if _, ok := ExclusiveStart(strings.Repeat("a", maxName)); ok {
		t.Fatal("a 1,024-byte name's successor is sent")
	}
}

type generationAPI func(context.Context, *client.ListInput) (*client.Page, error)

func (f generationAPI) List(ctx context.Context, in *client.ListInput) (*client.Page, error) {
	return f(ctx, in)
}

func TestGenerationsSeamsAndEmptyPages(t *testing.T) {
	for _, generation := range []int64{1, 2, 3} {
		for _, sequential := range []bool{false, true} {
			t.Run(fmt.Sprintf("g%d/seq%v", generation, sequential), func(t *testing.T) {
				calls := 0
				api := generationAPI(func(_ context.Context, in *client.ListInput) (*client.Page, error) {
					calls++
					if !in.Versions || in.MaxResults != 1 {
						t.Fatalf("input=%+v", in)
					}
					switch in.PageToken {
					case "":
						return &client.Page{Items: []client.Object{{Name: "k", Generation: 2}}, NextPageToken: "empty"}, nil
					case "empty":
						return &client.Page{NextPageToken: "tail"}, nil
					case "tail":
						return &client.Page{Items: []client.Object{{Name: "k", Generation: generation}}}, nil
					default:
						t.Fatal(in.PageToken)
						return nil, nil
					}
				})
				p, err := NewGenerations(context.Background(), Options{Bucket: "b", Workers: 1, Client: api, Policy: keyrange.Policy{PageSize: 1, Sequential: sequential}})
				if err != nil {
					t.Fatal(err)
				}
				st, err := order.Count(context.Background(), p, nil, order.Options{Workers: 1, Groups: true})
				if generation <= 2 {
					if !errors.Is(err, objhttp.ErrInvalidPage) {
						t.Fatalf("rows=%d err=%v", st.Rows, err)
					}
				} else if err != nil || st.Rows != 2 || st.DistinctKeys != 1 {
					t.Fatalf("stats=%+v err=%v", st, err)
				}
				if calls != 3 {
					t.Fatal(calls)
				}
			})
		}
	}
}

func TestFetchGenerationsResumeStart(t *testing.T) {
	for _, n := range []int{10, 1024} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			name := strings.Repeat("a", n)
			start := name + "\x00"
			if n == 1024 {
				start = name
			}
			calls := 0
			api := generationAPI(func(_ context.Context, in *client.ListInput) (*client.Page, error) {
				calls++
				if in.StartOffset != start || !in.Versions || in.Bucket != "b" || in.MaxResults != 1 {
					t.Fatalf("input=%+v", in)
				}
				if n == 1024 {
					switch in.PageToken {
					case "":
						return &client.Page{Items: []client.Object{{Name: name, Generation: 1}}, NextPageToken: "second"}, nil
					case "second":
						return &client.Page{Items: []client.Object{{Name: name, Generation: 2}}, NextPageToken: "next"}, nil
					case "next":
					default:
						t.Fatal(in.PageToken)
					}
				} else if in.PageToken != "" {
					t.Fatal(in.PageToken)
				}
				return &client.Page{Items: []client.Object{{Name: "b", Generation: 3}}}, nil
			})
			p, err := NewGenerations(context.Background(), Options{Bucket: "b", Workers: 1, Client: api, Policy: keyrange.Policy{PageSize: 1}})
			if err != nil {
				t.Fatal(err)
			}
			if err := p.Restart(name); err != nil {
				t.Fatal(err)
			}
			st, err := order.Count(context.Background(), p, nil, order.Options{Workers: 1, Groups: true, After: &name})
			if err != nil || st.Rows != 1 || st.DistinctKeys != 1 {
				t.Fatalf("stats=%+v err=%v", st, err)
			}
			want := 1
			var drops int64
			if n == 1024 {
				want, drops = 3, 2
			}
			if calls != want || p.Stats().Counters.Value("bound_drops") != drops {
				t.Fatalf("calls=%d want=%d drops=%d want=%d", calls, want, p.Stats().Counters.Value("bound_drops"), drops)
			}
		})
	}
}

func TestFetchGenerationsWithinPageOrder(t *testing.T) {
	for _, g := range []int64{1, 2} {
		api := generationAPI(func(context.Context, *client.ListInput) (*client.Page, error) {
			return &client.Page{Items: []client.Object{{Name: "k", Generation: 2}, {Name: "k", Generation: g}}}, nil
		})
		p, err := NewGenerations(context.Background(), Options{Bucket: "b", Workers: 1, Client: api})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.fetchGenerations(context.Background(), &keyrange.Query{}); !errors.Is(err, objhttp.ErrInvalidPage) {
			t.Fatal(err)
		}
	}
}

func TestGenerationsConstructorRefusal(t *testing.T) {
	if _, err := New(context.Background(), Options{Bucket: "b", AllVersions: true}); err == nil || err.Error() != "gcs: Options.AllVersions lists generations: use NewGenerations" {
		t.Fatalf("wrong constructor: %v", err)
	}
}

// Both modes omit an unrepresentable exclusive end once per query, including
// continuations; only generations move a representable start past its key.
func TestListInputBounds(t *testing.T) {
	p, err := New(context.Background(), Options{Bucket: "b", Client: generationAPI(nil)})
	if err != nil {
		t.Fatal(err)
	}
	long := strings.Repeat("x", 1024)
	var omitted int64
	for _, versions := range []bool{false, true} {
		for _, c := range []struct {
			after, through, token string
			bounded               bool
			start, end            string
		}{
			{"a", "z", "", true, "a", "z\x00"},
			{"a", long, "", true, "a", ""},
			{"a", long, "next", true, "a", ""},
			{long, "", "", false, long, ""},
		} {
			q := &keyrange.Query{Prefix: "p/", Delimiter: "/", After: c.after, Through: c.through, Token: c.token, Bounded: c.bounded, Max: 7}
			in := p.input(q, versions)
			start := c.start
			if versions && len(start) < 1024 {
				start += "\x00"
			}
			if in.Bucket != "b" || in.Prefix != q.Prefix || in.Delimiter != q.Delimiter || in.MaxResults != q.Max || in.PageToken != q.Token || in.Versions != versions || in.StartOffset != start || in.EndOffset != c.end {
				t.Fatalf("versions=%v query=%+v input=%+v", versions, q, in)
			}
			if c.bounded && c.end == "" {
				omitted++
			}
			if got := p.Stats().Counters.Value("end_offset_omitted"); got != omitted {
				t.Fatalf("omitted=%d want=%d", got, omitted)
			}
		}
	}
}
