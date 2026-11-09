// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client_test

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/varveio/swath/go/internal/source/azblob/azblobtest"
	"github.com/varveio/swath/go/internal/source/azblob/client"
)

// newClient builds a client for cfg.
func newClient(t *testing.T, cfg client.Config) *client.Client {
	t.Helper()
	c, err := client.New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// corpus is azblobtest.Keys and hostileKeys, without duplicates.
func corpus() []string {
	seen := map[string]bool{}
	var out []string
	for _, k := range append(azblobtest.Keys(), client.HostileKeys()...) {
		if !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	return out
}

// tee keeps the body of the last 200 its transport hands out.
type tee struct {
	mu   sync.Mutex
	last []byte
}

func (t *tee) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := http.DefaultTransport.RoundTrip(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		return resp, err
	}
	b, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	t.last = b
	t.mu.Unlock()
	resp.Body = io.NopCloser(bytes.NewReader(b))
	return resp, nil
}

// wireEntries reads a listing body's entries in wire order, kinds
// interleaved, and its marker, with encoding/xml apart from the SDK's
// decoder: an Encoded name percent-decoded once with path semantics, as
// Azure encodes it.
func wireEntries(t *testing.T, body []byte) client.ListOutput {
	t.Helper()
	var w struct {
		Blobs struct {
			Entries []struct {
				XMLName xml.Name
				Name    struct {
					Encoded bool   `xml:"Encoded,attr"`
					Text    string `xml:",chardata"`
				} `xml:"Name"`
			} `xml:",any"`
		} `xml:"Blobs"`
		NextMarker string `xml:"NextMarker"`
	}
	if err := xml.Unmarshal(bytes.TrimPrefix(body, []byte("\xef\xbb\xbf")), &w); err != nil {
		t.Fatal(err)
	}
	out := client.ListOutput{NextMarker: w.NextMarker}
	for _, e := range w.Blobs.Entries {
		name := e.Name.Text
		if e.Name.Encoded {
			var err error
			if name, err = url.PathUnescape(name); err != nil {
				t.Fatal(err)
			}
		}
		out.Items = append(out.Items, client.Item{Name: name, Prefix: e.XMLName.Local == "BlobPrefix"})
	}
	return out
}

// TestWireOrder walks the replay's Azure route, flat and with a delimiter,
// from startFrom seeks, at page sizes 1 to 5,000, over the hostile corpus,
// anonymous, with a SAS and with Shared Key: every page's items are the
// wire's, in wire order with the two kinds interleaved (the SDK returns a
// hierarchy page's blobs and prefixes apart and merge restores them), with
// the wire's NextMarker; one attempt per page.
func TestWireOrder(t *testing.T) {
	r := azblobtest.Serve(t, corpus())
	ep := strings.TrimSuffix(r.Endpoint, "/"+r.Account)
	for _, auth := range []client.Config{{Anonymous: true}, {SAS: "sv=2026-10-06&sp=rl&sig=Fake%2BSig%3D"}, {SharedKey: "c2VjcmV0LWtleS1mb3ItdGVzdHM="}} {
		wire := &tee{}
		auth.Endpoint, auth.Account, auth.HTTPClient = ep, r.Account, &http.Client{Transport: wire}
		c := newClient(t, auth)
		for _, in := range []client.ListInput{
			{MaxResults: 1, Prefix: "u/"},
			{MaxResults: 2, Prefix: "e/"},
			{MaxResults: 7},
			{MaxResults: 5000},
			{},
			{Prefix: "data/year=2021/", MaxResults: 3},
			{StartFrom: "a/010", MaxResults: 5},
			{StartFrom: "u/\ue000", MaxResults: 2},
			{StartFrom: "pct%41", MaxResults: 1, Prefix: "p"},
			{Delimiter: "/", MaxResults: 3},
			{Delimiter: "/"},
			{Prefix: "u/", Delimiter: "/", MaxResults: 2},
			{Prefix: "u/", Delimiter: "/", StartFrom: "u/\U0001F600"},
			{Prefix: "e/", Delimiter: "/", MaxResults: 1},
			{Prefix: "n1/", Delimiter: "/", MaxResults: 4},
			{Prefix: "d/", Delimiter: "/", StartFrom: "d/p\ue000"},
			{Prefix: "r", Delimiter: "\U00010000", MaxResults: 3},
		} {
			in.Container = r.Container
			before := c.Counters().Attempts
			pages, items, prefixes := 0, 0, 0
			for {
				p, err := c.List(context.Background(), &in)
				if err != nil {
					t.Fatalf("%+v: %v", in, err)
				}
				pages++
				want := wireEntries(t, wire.last)
				names := make([]client.Item, len(p.Items))
				for i, it := range p.Items {
					names[i] = client.Item{Name: it.Name, Prefix: it.Prefix}
					items++
					if it.Prefix {
						prefixes++
					}
				}
				if p.NextMarker != want.NextMarker || fmt.Sprint(names) != fmt.Sprint(want.Items) {
					t.Fatalf("%+v page %d:\n got  %+v next %q\n wire %+v next %q", in, pages, names, p.NextMarker, want.Items, want.NextMarker)
				}
				if p.NextMarker == "" {
					break
				}
				in.Marker = p.NextMarker
			}
			if n := c.Counters().Attempts - before; n != int64(pages) {
				t.Errorf("%+v: %d attempts, %d pages", in, n, pages)
			}
			if items == 0 || in.Delimiter != "" && prefixes == 0 {
				t.Fatalf("%+v: %d items, %d prefixes: a walk that proves nothing", in, items, prefixes)
			}
		}
	}
}
