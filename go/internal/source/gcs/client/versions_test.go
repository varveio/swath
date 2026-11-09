// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
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
	c, err := New(context.Background(), Config{Endpoint: s.URL + "/storage/v1/", Anonymous: true, MaxAttempts: 1})
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

// TestVersionsRequest: a Versions request sends versions=true and, on
// Google's endpoint, a field mask with timeDeleted; a current-object
// request is unchanged.
func TestVersionsRequest(t *testing.T) {
	s := newServer(t)
	s.body = []byte(`{"kind":"storage#objects"}`)
	u, _ := url.Parse(s.URL)
	c, err := New(context.Background(), Config{Anonymous: true, HTTPClient: &http.Client{Transport: toServer{u}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, versions := range []bool{false, true} {
		if _, err := c.List(context.Background(), &ListInput{Bucket: "b", StartOffset: "a\x00", Versions: versions}); err != nil {
			t.Fatal(err)
		}
		q := s.query
		want, wantV := fields, ""
		if versions {
			want, wantV = versionFields, "true"
		}
		if q.Get("fields") != want || q.Get("versions") != wantV || q.Get("startOffset") != "a\x00" {
			t.Fatalf("versions %v: query %v", versions, q)
		}
	}
	if versionFields == fields {
		t.Fatal("the versions field mask asks for nothing more")
	}
}

// toServer sends requests addressed to Google's endpoint to a test server,
// so the client takes its production path (the fields selector included).
type toServer struct{ to *url.URL }

func (r toServer) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.URL.Scheme, req.URL.Host, req.Host = r.to.Scheme, r.to.Host, r.to.Host
	return http.DefaultTransport.RoundTrip(req)
}

// TestVersionsCaptureParses: the captured pages parse in wire order
// (names ascending, generations ascending within a name) with timeDeleted
// on exactly the noncurrent generations, and a page served over HTTP takes
// the ordinary request path with versions=true.
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
		if _, err := parse(t, capture(t, c.file, false)); Classify(err) != objhttp.ClassParse {
			t.Fatalf("%s without kind: %v", c.file, err)
		}
		if len(p.Items) != c.items || len(p.Prefixes) != c.prefixes || p.NextPageToken == "" {
			t.Fatalf("%s: %d items, %d prefixes, token %q", c.file, len(p.Items), len(p.Prefixes), p.NextPageToken)
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

	srv := newServer(t)
	srv.body = capture(t, "single2.json", true)
	cl, err := New(context.Background(), Config{Endpoint: srv.URL + "/storage/v1/", Anonymous: true})
	if err != nil {
		t.Fatal(err)
	}
	p, err = cl.List(context.Background(), &ListInput{Bucket: "b", StartOffset: "GRCm39/README.txt", PageToken: "t", MaxResults: 2, Versions: true})
	if err != nil || len(p.Items) != 2 || p.Items[0].TimeDeleted != 0 || p.Items[0].Generation != 1688688031116446 {
		t.Fatalf("page %+v, %v", p, err)
	}
	if got := srv.query; got.Get("versions") != "true" || got.Get("pageToken") != "t" || got.Get("fields") != "" {
		t.Fatalf("query %v", got)
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
		if _, err := parse(t, []byte(in)); !errors.Is(err, ErrSyntax) {
			t.Fatalf("%s: %v", in, err)
		}
	}
}
