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
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/varveio/swath/internal/source/azblob/azblobtest"
	"github.com/varveio/swath/internal/source/azblob/client"
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
// interleaved, every property the client keeps, and its marker, with
// encoding/xml apart from the SDK's decoder: an Encoded name
// percent-decoded once with path semantics, as Azure encodes it; the
// times as HTTP dates; the rest as listed.
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
				Properties struct {
					Size         string `xml:"Content-Length"`
					LastModified string `xml:"Last-Modified"`
					CreationTime string `xml:"Creation-Time"`
					ETag         string `xml:"Etag"`
					ContentMD5   string `xml:"Content-MD5"`
					ContentType  string `xml:"Content-Type"`
					BlobType     string `xml:"BlobType"`
					AccessTier   string `xml:"AccessTier"`
					Inferred     string `xml:"AccessTierInferred"`
					LeaseState   string `xml:"LeaseState"`
					Encrypted    string `xml:"ServerEncrypted"`
					Encoding     string `xml:"Content-Encoding"`
				} `xml:"Properties"`
			} `xml:",any"`
		} `xml:"Blobs"`
		NextMarker string `xml:"NextMarker"`
	}
	micros := func(s string) int64 {
		if s == "" {
			return 0
		}
		tm, err := http.ParseTime(s)
		if err != nil {
			t.Fatal(err)
		}
		return tm.UnixMicro()
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
		it := client.Item{Name: name, Prefix: e.XMLName.Local == "BlobPrefix"}
		if p := e.Properties; !it.Prefix {
			if p.Size != "" {
				var err error
				if it.Size, err = strconv.ParseInt(p.Size, 10, 64); err != nil {
					t.Fatal(err)
				}
			}
			it.LastModified, it.CreationTime = micros(p.LastModified), micros(p.CreationTime)
			it.ETag, it.ContentMD5, it.ContentType, it.BlobType, it.AccessTier = p.ETag, p.ContentMD5, p.ContentType, p.BlobType, p.AccessTier
			it.AccessTierInferred, it.ServerEncrypted = wireTri(t, p.Inferred), wireTri(t, p.Encrypted)
			it.LeaseState, it.ContentEncoding = p.LeaseState, p.Encoding
		}
		out.Items = append(out.Items, it)
	}
	return out
}

// wireTri reads an XML boolean property, client.Unset when absent.
func wireTri(t *testing.T, s string) client.Tri {
	t.Helper()
	switch s {
	case "":
		return client.Unset
	case "true":
		return client.True
	case "false":
		return client.False
	}
	t.Fatalf("boolean property %q", s)
	return client.Unset
}

// epoch is the Last-Modified of the fixture's first key (azblobtest.Serve).
var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// TestWireOrder walks the replay's Azure route over the hostile corpus,
// flat, over Encoded names, with a delimiter (an astral one too) and from
// high-scalar startFrom seeks: every page's items are the
// wire's, in wire order with the two kinds interleaved (the SDK returns a
// hierarchy page's blobs and prefixes apart and merge restores them), with
// the wire's NextMarker; one attempt per page. Every item carries the
// properties the wire's XML holds, read apart from the SDK, and each blob
// the size and Last-Modified the fixture gave it (its byte-order index i:
// size i, i seconds after the epoch).
func TestWireOrder(t *testing.T) {
	keys := corpus()
	r := azblobtest.Serve(t, keys)
	slices.Sort(keys)
	index := make(map[string]int, len(keys))
	for i, k := range keys {
		index[k] = i
	}
	wire := &tee{}
	c := newClient(t, client.Config{Anonymous: true, Endpoint: strings.TrimSuffix(r.Endpoint, "/"+r.Account), Account: r.Account,
		HTTPClient: &http.Client{Transport: wire}})
	for _, in := range []client.ListInput{
		{MaxResults: 7},
		{MaxResults: 2, Prefix: "e/"},
		{Delimiter: "/", MaxResults: 3},
		{Prefix: "u/", Delimiter: "/", MaxResults: 2},
		{StartFrom: "u/\ue000", MaxResults: 2},
		{Prefix: "u/", Delimiter: "/", StartFrom: "u/\U0001F600"},
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
			for _, it := range p.Items {
				items++
				if it.Prefix {
					prefixes++
					continue
				}
				i, ok := index[it.Name]
				if !ok || it.Size != int64(i) || it.LastModified != epoch.Add(time.Duration(i)*time.Second).UnixMicro() ||
					it.BlobType != "BlockBlob" || it.ETag == "" {
					t.Fatalf("%+v page %d: %+v is not the fixture's key %d", in, pages, it, i)
				}
			}
			if p.NextMarker != want.NextMarker || fmt.Sprint(p.Items) != fmt.Sprint(want.Items) {
				t.Fatalf("%+v page %d:\n got  %+v next %q\n wire %+v next %q", in, pages, p.Items, p.NextMarker, want.Items, want.NextMarker)
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
