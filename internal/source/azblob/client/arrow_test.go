// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	amem "github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/varveio/swath/internal/source/objhttp"
)

// The recorded pages: the first page (maxresults=50) of the public
// container azureopendatastorage/holidaydatacontainer (Azure Open
// Datasets), flat and with delimiter=/, each asked anonymously with
// x-ms-version 2026-10-06 twice on 2026-11-10, once with
// "Accept: application/vnd.apache.arrow.stream, application/xml" (answered
// in Arrow) and once with "Accept: application/xml"; the bodies as sent.
// testdata/empty.arrow is the Arrow answer, asked the same way, to
// prefix=zzzznothing, which matches nothing.
var recorded = []struct {
	name, delimiter, arrow, xml string
}{
	{"flat", "", "testdata/flat.arrow", "testdata/flat.xml"},
	{"hierarchy", "/", "testdata/hierarchy.arrow", "testdata/hierarchy.xml"},
}

// listOnce lists one page from a server answering body with Content-Type
// ct, through a client with one attempt.
func listOnce(t *testing.T, ct string, body []byte, delimiter string) (*ListOutput, Formats, error) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", ct)
		w.Write(body)
	}))
	defer srv.Close()
	cl, err := newClient(context.Background(), Config{Endpoint: srv.URL, Account: "acct", Anonymous: true,
		Retry: objhttp.Retry{MaxAttempts: 1}, RequestTimeout: time.Minute, HTTPClient: &http.Client{}}, build{bodyCap: MaxBody, entra: defaultChain})
	if err != nil {
		t.Fatal(err)
	}
	out, err := cl.List(context.Background(), &ListInput{Container: "c", Delimiter: delimiter})
	return out, cl.Formats(), err
}

// TestArrowEquivalence: a recorded Arrow page and the XML page of the same
// listing convert to the same ListOutput, a hierarchy page's prefixes and
// blobs merged back into one sequence; each page counts in its format.
func TestArrowEquivalence(t *testing.T) {
	for _, r := range recorded {
		t.Run(r.name, func(t *testing.T) {
			ab, err := os.ReadFile(r.arrow)
			if err != nil {
				t.Fatal(err)
			}
			xb, err := os.ReadFile(r.xml)
			if err != nil {
				t.Fatal(err)
			}
			a, af, err := listOnce(t, arrowType, ab, r.delimiter)
			if err != nil {
				t.Fatal(err)
			}
			x, xf, err := listOnce(t, "application/xml", xb, r.delimiter)
			if err != nil {
				t.Fatal(err)
			}
			if len(a.Items) == 0 {
				t.Fatal("an empty Arrow page")
			}
			if r.delimiter != "" && !mixed(a.Items) {
				t.Fatal("hierarchy page without both prefixes and blobs")
			}
			if !reflect.DeepEqual(a, x) {
				for i := range min(len(a.Items), len(x.Items)) {
					if a.Items[i] != x.Items[i] {
						t.Errorf("item %d:\n arrow %+v\n xml   %+v", i, a.Items[i], x.Items[i])
					}
				}
				t.Fatalf("Arrow and XML pages differ: %d/%d items, markers %q/%q", len(a.Items), len(x.Items), a.NextMarker, x.NextMarker)
			}
			if n := int64(len(a.Items)); af != (Formats{ArrowPages: 1, ArrowItems: n}) || xf != (Formats{XMLPages: 1, XMLItems: n}) {
				t.Errorf("formats %+v / %+v", af, xf)
			}
		})
	}
}

// mixed reports items holding both a prefix and a blob.
func mixed(items []Item) bool {
	var p, b bool
	for _, it := range items {
		p, b = p || it.Prefix, b || !it.Prefix
	}
	return p && b
}

// arrowStream is an Arrow listing: a Name column, the schema metadata md
// (nil: none), batches record batches of two names each, in order.
func arrowStream(md map[string]string, batches int) []byte {
	var meta *arrow.Metadata
	if md != nil {
		m := arrow.MetadataFrom(md)
		meta = &m
	}
	schema := arrow.NewSchema([]arrow.Field{{Name: "Name", Type: arrow.BinaryTypes.String, Nullable: true}}, meta)
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	for i := range batches {
		b := array.NewStringBuilder(amem.DefaultAllocator)
		b.AppendValues([]string{fmt.Sprintf("n%02d", 2*i), fmt.Sprintf("n%02d", 2*i+1)}, nil)
		names := b.NewArray()
		rec := array.NewRecordBatch(schema, []arrow.Array{names}, 2)
		if err := w.Write(rec); err != nil {
			panic(err)
		}
		rec.Release()
		names.Release()
		b.Release()
	}
	if err := w.Close(); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// TestArrowEnvelope: what the Guard refuses of an Arrow answer, each a
// parse failure, never a page. The SDK's decoder alone reads a stream cut
// after a batch as a whole page with the schema's NextMarker (a gap: the
// next page starts past the missing rows), a stream without its batches as
// a short or empty page, and a schema without NextMarker as a final page;
// an empty body, a stream cut inside a message and an XML body under the
// Arrow type fail the decoder too. An Arrow body under an XML type is the
// XML envelope's refusal. The control cases are pages: a whole stream, the
// live empty listing (a schema without fields and one batch of no rows),
// and a stream missing only its end-of-stream marker (nothing dropped).
func TestArrowEnvelope(t *testing.T) {
	md := map[string]string{"NextMarker": "m", "NumberOfRecords": "4"}
	whole := arrowStream(md, 2)
	oneBatch := arrowStream(md, 1)
	empty := arrowStream(map[string]string{"NextMarker": "", "NumberOfRecords": "0"}, 0)
	page4 := &ListOutput{Items: []Item{{Name: "n00"}, {Name: "n01"}, {Name: "n02"}, {Name: "n03"}}, NextMarker: "m"}
	live, err := os.ReadFile("testdata/empty.arrow")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name, ct string
		body     []byte
		want     *ListOutput // nil: refused, ErrSyntax
	}{
		{"whole", arrowType, whole, page4},
		{"without its end-of-stream marker", arrowType, whole[:len(whole)-8], page4},
		{"no batches", arrowType, empty, &ListOutput{Items: []Item{}}},
		{"live empty listing", arrowType, live, &ListOutput{Items: []Item{}}},
		{"empty body", arrowType, nil, nil},
		{"cut after a batch", arrowType, oneBatch[:len(oneBatch)-8], nil},
		{"cut inside a message", arrowType, whole[:len(whole)-40], nil},
		{"schema only, cut", arrowType, whole[:200], nil},
		{"no batches, rows declared", arrowType, arrowStream(md, 0), nil},
		{"without NextMarker", arrowType, arrowStream(map[string]string{"NumberOfRecords": "4"}, 2), nil},
		{"without NumberOfRecords", arrowType, arrowStream(map[string]string{"NextMarker": "m"}, 2), nil},
		{"without metadata", arrowType, arrowStream(nil, 2), nil},
		{"NumberOfRecords not a number", arrowType, arrowStream(map[string]string{"NextMarker": "m", "NumberOfRecords": "four"}, 2), nil},
		{"XML under the Arrow type", arrowType, []byte(page("", "a")), nil},
		{"Arrow under the XML type", "application/xml", whole, nil},
	} {
		for _, d := range []string{"", "/"} {
			t.Run(c.name+"/delimiter="+d, func(t *testing.T) {
				got, _, err := listOnce(t, c.ct, c.body, d)
				if c.want == nil {
					if !errors.Is(err, objhttp.ErrSyntax) {
						t.Fatalf("page %+v, error %v: want ErrSyntax", got, err)
					}
					return
				}
				if err != nil || !reflect.DeepEqual(got, c.want) {
					t.Fatalf("page %+v, error %v; want %+v", got, err, c.want)
				}
			})
		}
	}
}

// column is one column of a test Arrow listing: its name, its type and its
// values, a JSON array as array.FromJSON reads it.
type column struct {
	name   string
	typ    arrow.DataType
	values string
}

// columnsPage is a one-batch final Arrow listing of cols, built from mem;
// everything it allocates is released before it returns.
func columnsPage(t *testing.T, mem amem.Allocator, cols []column) []byte {
	t.Helper()
	fields := make([]arrow.Field, len(cols))
	arrays := make([]arrow.Array, len(cols))
	for i, c := range cols {
		a, _, err := array.FromJSON(mem, c.typ, strings.NewReader(c.values))
		if err != nil {
			t.Fatalf("column %s %s %s: %v", c.name, c.typ, c.values, err)
		}
		defer a.Release()
		fields[i], arrays[i] = arrow.Field{Name: c.name, Type: c.typ, Nullable: true}, a
	}
	rows := arrays[0].Len()
	m := arrow.MetadataFrom(map[string]string{"NextMarker": "", "NumberOfRecords": strconv.Itoa(rows)})
	schema := arrow.NewSchema(fields, &m)
	rec := array.NewRecordBatch(schema, arrays, int64(rows))
	defer rec.Release()
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithAllocator(mem))
	if err := w.Write(rec); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// TestArrowColumnTypes: the ten columns behind what swath writes
// (arrowColumns), through the SDK's decoder. A page of the prefix a/, a
// blob bé with every property and a blob c with none (each optional value
// null) is read in Azure's types (utf8, uint64, timestamp[s]), with
// Content-Length int64 too and with both timestamps in each other unit,
// each value as listed: Content-MD5 back in base64, times in Unix
// microseconds, a finer unit truncated. ResourceType marks a/ a prefix on
// a hierarchy page. Each of the ten columns in a type its resolver does
// not read is refused, never decoded as nulls: a dictionary-encoded
// AccessTier would blank the tier, a dictionary-encoded ResourceType would
// list a/ as a blob. A column swath does not read may have any type,
// whether the SDK maps it (LeaseState, LastAccessTime) or not, and the
// optional columns may be absent.
func TestArrowColumnTypes(t *testing.T) {
	lm := time.Date(2026, 11, 10, 21, 7, 5, 123456789, time.UTC)
	ct := time.Date(2025, 1, 2, 3, 4, 5, 987654321, time.UTC)
	at := func(tm time.Time, u arrow.TimeUnit) string {
		return fmt.Sprintf("[null, %d, null]", tm.UnixNano()/int64(u.Multiplier()))
	}
	str := arrow.BinaryTypes.String
	dict := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: str}
	page := func(replace ...column) []column {
		cols := []column{
			{"Name", str, `["a/", "bé", "c"]`},
			{"ResourceType", str, `["blobprefix", "blob", "blob"]`},
			{"Etag", str, `[null, "0x8DE2001D2C3F4A5", null]`},
			{"Content-MD5", str, `[null, "1B2M2Y8AsgTpgAmY7PhCfg==", null]`},
			{"Content-Type", str, `[null, "text/plain; charset=utf-8", null]`},
			{"BlobType", str, `[null, "BlockBlob", null]`},
			{"AccessTier", str, `[null, "Cool", null]`},
			{"Content-Length", arrow.PrimitiveTypes.Uint64, `[null, 5000000000, null]`},
			{"Last-Modified", &arrow.TimestampType{Unit: arrow.Second}, at(lm, arrow.Second)},
			{"Creation-Time", &arrow.TimestampType{Unit: arrow.Second}, at(ct, arrow.Second)},
		}
		for _, r := range replace {
			if i := slices.IndexFunc(cols, func(c column) bool { return c.name == r.name }); i >= 0 {
				cols[i] = r
			} else {
				cols = append(cols, r)
			}
		}
		return cols
	}
	listed := func(unit time.Duration, hierarchy bool) *ListOutput {
		return &ListOutput{Items: []Item{{Name: "a/", Prefix: hierarchy},
			{Name: "bé", Size: 5000000000, ETag: "0x8DE2001D2C3F4A5", ContentMD5: "1B2M2Y8AsgTpgAmY7PhCfg==",
				ContentType: "text/plain; charset=utf-8", BlobType: "BlockBlob", AccessTier: "Cool",
				LastModified: lm.Truncate(max(unit, time.Microsecond)).UnixMicro(),
				CreationTime: ct.Truncate(max(unit, time.Microsecond)).UnixMicro()},
			{Name: "c"}}}
	}
	type read struct {
		name string
		cols []column
		unit time.Duration // the timestamps' unit; 0: Azure's, a second
	}
	reads := []read{
		{"Azure's types", page(), 0},
		{"Content-Length int64", page(column{"Content-Length", arrow.PrimitiveTypes.Int64, `[null, 5000000000, null]`}), 0},
		{"unread columns of any type", page(column{"LeaseStatus", dict, `["unlocked", "locked", null]`},
			column{"Cache-Control", arrow.PrimitiveTypes.Int64, `[1, 2, null]`}, column{"Unmapped", dict, `["x", "y", "x"]`}), 0},
	}
	for _, u := range []arrow.TimeUnit{arrow.Millisecond, arrow.Microsecond, arrow.Nanosecond} {
		typ := &arrow.TimestampType{Unit: u}
		reads = append(reads, read{"timestamps " + typ.String(),
			page(column{"Last-Modified", typ, at(lm, u)}, column{"Creation-Time", typ, at(ct, u)}), u.Multiplier()})
	}
	for _, r := range reads {
		for _, d := range []string{"", "/"} {
			t.Run(r.name+"/delimiter="+d, func(t *testing.T) {
				mem := amem.NewCheckedAllocator(amem.NewGoAllocator())
				defer mem.AssertSize(t, 0)
				got, _, err := listOnce(t, arrowType, columnsPage(t, mem, r.cols), d)
				if want := listed(cmp.Or(r.unit, time.Second), d != ""); err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("page %+v, error %v; want %+v", got, err, want)
				}
			})
		}
	}
	t.Run("only Name and ResourceType", func(t *testing.T) {
		mem := amem.NewCheckedAllocator(amem.NewGoAllocator())
		defer mem.AssertSize(t, 0)
		got, _, err := listOnce(t, arrowType, columnsPage(t, mem, page()[:2]), "/")
		want := &ListOutput{Items: []Item{{Name: "a/", Prefix: true}, {Name: "bé"}, {Name: "c"}}}
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("page %+v, error %v; want %+v", got, err, want)
		}
	})

	var refused []column
	for _, c := range page() {
		if c.typ == str {
			refused = append(refused, column{c.name, arrow.BinaryTypes.LargeString, c.values}, column{c.name, dict, c.values})
		}
	}
	refused = append(refused,
		column{"Content-Length", arrow.PrimitiveTypes.Int32, `[null, 7, null]`},
		column{"Content-Length", str, `[null, "5000000000", null]`},
		column{"Last-Modified", arrow.PrimitiveTypes.Int64, at(lm, arrow.Second)},
		column{"Last-Modified", str, `[null, "2026-11-10T21:07:05Z", null]`},
		column{"Creation-Time", arrow.PrimitiveTypes.Int64, at(ct, arrow.Second)},
		column{"Creation-Time", str, `[null, "2025-01-02T03:04:05Z", null]`},
	)
	for _, c := range refused {
		for _, d := range []string{"", "/"} {
			t.Run(c.name+" "+c.typ.String()+"/delimiter="+d, func(t *testing.T) {
				mem := amem.NewCheckedAllocator(amem.NewGoAllocator())
				defer mem.AssertSize(t, 0)
				got, _, err := listOnce(t, arrowType, columnsPage(t, mem, page(c)), d)
				if !errors.Is(err, objhttp.ErrSyntax) || !strings.Contains(err.Error(), strconv.Quote(c.name)) {
					t.Fatalf("page %+v, error %v: want ErrSyntax naming %s", got, err, c.name)
				}
			})
		}
	}
}
