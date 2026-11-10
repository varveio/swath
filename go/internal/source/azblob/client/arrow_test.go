// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	amem "github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/varveio/swath/go/internal/source/objhttp"
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
