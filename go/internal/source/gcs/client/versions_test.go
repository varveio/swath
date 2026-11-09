// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/source/objhttp"
)

// The captures in testdata/versions are live objects.list answers from
// gcp-public-data--broad-references (a public bucket with noncurrent
// generations), taken anonymously on 2026-11-08 with versions=true,
// projection=noAcl and the versionFields selector:
//
//	page20.json   maxResults=20, from the start
//	delim.json    maxResults=4, prefix=GRCm39/, delimiter=/
//	delim2.json   the same with delim.json's pageToken
//	single.json   maxResults=2, startOffset=GRCm39/README.txt
//	single2.json  the same with single.json's pageToken
//	empty.json    maxResults=3, startOffset=empty.test (a name whose one
//	              generation is noncurrent: history with no live one)
//
// They are kept byte for byte as the API sent them; the selector did not
// name kind, which the production selector now asks for, so capture
// injects it (withKind) for the client to read them as objects envelopes.
func capture(t *testing.T, name string, withKind bool) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "versions", name))
	if err != nil {
		t.Fatal(err)
	}
	if withKind {
		b = bytes.Replace(b, []byte("{"), []byte(`{"kind":"storage#objects",`), 1)
	}
	return b
}

// server serves *body to every request, recording the last query.
type server struct {
	*httptest.Server
	body  []byte
	query url.Values
}

func newServer(t *testing.T) *server {
	s := &server{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.query = r.URL.Query()
		w.Write(s.body)
	}))
	t.Cleanup(s.Close)
	return s
}

// parse lists body as one page through a one-attempt client.
func parse(t *testing.T, body []byte) (*Page, error) {
	t.Helper()
	s := newServer(t)
	s.body = body
	c, err := New(context.Background(), Config{Endpoint: s.URL + "/storage/v1/", Anonymous: true, Retry: objhttp.Retry{MaxAttempts: 1}})
	if err != nil {
		t.Fatal(err)
	}
	return c.List(context.Background(), &ListInput{Bucket: "b", Versions: true})
}

func usec(t *testing.T, s string) int64 {
	t.Helper()
	v, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t.Fatal(err)
	}
	return v.UnixMicro()
}

// expectedPage is the page a capture must read as, decoded here field by
// field from the wire's own spelling: sizes, generations and
// metagenerations are decimal strings, times RFC 3339 as microseconds,
// hashes, etags, storage class, content type, names, prefixes and the
// token verbatim. A capture member it does not know fails the test, so
// every field the captures carry is pinned.
func expectedPage(t *testing.T, b []byte) *Page {
	t.Helper()
	var w struct {
		NextPageToken string
		Prefixes      []string
		Items         []map[string]string
	}
	if err := json.Unmarshal(b, &w); err != nil {
		t.Fatal(err)
	}
	num := func(s string) int64 {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	at := func(s string) int64 {
		if s == "" {
			return 0
		}
		return usec(t, s)
	}
	p := &Page{Items: []Object{}, Prefixes: w.Prefixes, NextPageToken: w.NextPageToken}
	for _, o := range w.Items {
		for k := range o {
			switch k {
			case "name", "size", "generation", "metageneration", "contentType", "storageClass", "md5Hash", "crc32c",
				"etag", "timeCreated", "updated", "timeDeleted":
			default:
				t.Fatalf("capture member %q is not pinned", k)
			}
		}
		p.Items = append(p.Items, Object{Name: o["name"], Size: num(o["size"]), Updated: at(o["updated"]), TimeCreated: at(o["timeCreated"]),
			ETag: o["etag"], Generation: num(o["generation"]), Metageneration: num(o["metageneration"]), StorageClass: o["storageClass"],
			MD5: o["md5Hash"], CRC32C: o["crc32c"], ContentType: o["contentType"], TimeDeleted: at(o["timeDeleted"])})
	}
	return p
}

// toServer sends requests addressed to Google's endpoint to a test server,
// so the client takes its production path (the fields selector included).
type toServer struct{ to *url.URL }

func (r toServer) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.URL.Scheme, req.URL.Host, req.Host = r.to.Scheme, r.to.Host, r.to.Host
	return http.DefaultTransport.RoundTrip(req)
}

// TestVersionsCaptureParses: the captured pages read exactly as
// expectedPage spells them out (every item's every field, the prefixes and
// the token), in wire order (names ascending, generations ascending within
// a name) with timeDeleted on exactly the noncurrent generations; without
// kind they are refused. The versions=true request is TestRequests'.
func TestVersionsCaptureParses(t *testing.T) {
	for _, c := range []struct {
		file     string
		items    int
		prefixes int
		deleted  int
	}{
		{"page20.json", 20, 0, 6},
		{"delim.json", 4, 0, 2},
		{"delim2.json", 2, 2, 1},
		{"single.json", 2, 0, 2},
		{"single2.json", 2, 0, 0},
		{"empty.json", 3, 0, 1},
	} {
		p, err := parse(t, capture(t, c.file, true))
		if err != nil {
			t.Fatalf("%s: %v", c.file, err)
		}
		if _, err := parse(t, capture(t, c.file, false)); objhttp.Classify(err) != objhttp.ClassParse {
			t.Fatalf("%s without kind: %v", c.file, err)
		}
		if len(p.Items) != c.items || len(p.Prefixes) != c.prefixes || p.NextPageToken == "" {
			t.Fatalf("%s: %d items, %d prefixes, token %q", c.file, len(p.Items), len(p.Prefixes), p.NextPageToken)
		}
		if want := expectedPage(t, capture(t, c.file, false)); !reflect.DeepEqual(p, want) {
			t.Fatalf("%s:\nread %+v\nwant %+v", c.file, p, want)
		}
		deleted := 0
		for i, o := range p.Items {
			if o.TimeDeleted != 0 {
				deleted++
				if o.TimeDeleted < o.TimeCreated {
					t.Fatalf("%s: %q deleted before it was created", c.file, o.Name)
				}
			}
			if o.Generation == 0 || o.Updated == 0 || o.TimeCreated == 0 || o.MD5 == "" {
				t.Fatalf("%s: %+v lacks a field", c.file, o)
			}
			if i == 0 {
				continue
			}
			prev := p.Items[i-1]
			if o.Name < prev.Name || o.Name == prev.Name && o.Generation <= prev.Generation {
				t.Fatalf("%s: %q/%d after %q/%d", c.file, o.Name, o.Generation, prev.Name, prev.Generation)
			}
			// A generation becomes noncurrent when the next is written.
			if o.Name == prev.Name && prev.TimeDeleted != o.TimeCreated {
				t.Fatalf("%s: %q/%d deleted at %d, the next created at %d", c.file, o.Name, prev.Generation, prev.TimeDeleted, o.TimeCreated)
			}
		}
		if deleted != c.deleted {
			t.Fatalf("%s: %d noncurrent generations, want %d", c.file, deleted, c.deleted)
		}
	}
	p, err := parse(t, capture(t, "single.json", true))
	if err != nil {
		t.Fatal(err)
	}
	a, b := p.Items[0], p.Items[1]
	if a.Name != "GRCm39/README.txt" || a.Generation != 1656460817987973 || a.TimeDeleted != usec(t, "2022-06-29T00:00:19.506Z") ||
		b.Generation != 1656460819439441 || b.TimeDeleted != usec(t, "2023-07-07T00:00:31.213Z") || b.Metageneration != 1 {
		t.Fatalf("single.json: %+v, %+v", a, b)
	}

	// empty.test's only generation is noncurrent: the name has no live
	// generation, though its highest generation is on the page.
	p, err = parse(t, capture(t, "empty.json", true))
	if err != nil {
		t.Fatal(err)
	}
	if e := p.Items[0]; e.Name != "empty.test" || e.Size != 0 || e.Generation != 1646943531464293 ||
		e.TimeDeleted != usec(t, "2022-03-10T20:19:06Z") || p.Items[1].Name == e.Name {
		t.Fatalf("empty.json: %+v", p.Items)
	}
}

// TestTimeDeleted: timeDeleted is RFC 3339 read as microseconds
// (sub-microsecond digits truncated, offsets honoured), absent is 0, and
// a malformed one fails the attempt like any other field.
func TestTimeDeleted(t *testing.T) {
	for in, want := range map[string]int64{
		`{"kind":"storage#objects","items":[{"name":"a","generation":"1"}]}`:                                             0,
		`{"kind":"storage#objects","items":[{"name":"a","timeDeleted":"2026-02-19T00:01:22.061Z"}]}`:                     usec(t, "2026-02-19T00:01:22.061Z"),
		`{"kind":"storage#objects","items":[{"name":"a","timeDeleted":"1970-01-01T00:00:00.0000019Z"}]}`:                 1,
		`{"kind":"storage#objects","items":[{"name":"a","timeDeleted":"2026-02-19T01:01:22.061123+01:00"}]}`:             usec(t, "2026-02-19T00:01:22.061123Z"),
		`{"kind":"storage#objects","items":[{"name":"a","timeDeleted":"1969-12-31T23:59:59.999999Z","generation":"9"}]}`: -1,
	} {
		p, err := parse(t, []byte(in))
		if err != nil || p.Items[0].TimeDeleted != want {
			t.Fatalf("%s: %+v, %v; want %d", in, p, err, want)
		}
	}
	for _, in := range []string{
		`{"kind":"storage#objects","items":[{"name":"a","timeDeleted":"yesterday"}]}`,
		`{"kind":"storage#objects","items":[{"name":"a","timeDeleted":"2026-02-19"}]}`,
		`{"kind":"storage#objects","items":[{"name":"a","timeDeleted":12}]}`,
	} {
		if _, err := parse(t, []byte(in)); !errors.Is(err, objhttp.ErrSyntax) {
			t.Fatalf("%s: %v", in, err)
		}
	}
}
