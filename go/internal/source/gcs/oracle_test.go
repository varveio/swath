// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"context"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/api/option"
	storage "google.golang.org/api/storage/v1"

	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/source/gcs/client"
	"github.com/varveio/swath/go/internal/source/gcs/gcstest"
)

// redirect sends requests addressed to Google's endpoint to the fake, so
// the client takes its production path (the fields selector included).
type redirect struct{ to *url.URL }

func (r redirect) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.URL.Scheme, req.URL.Host, req.Host = r.to.Scheme, r.to.Host, r.to.Host
	return http.DefaultTransport.RoundTrip(req)
}

// TestClientPagesMatchGoogleClient: the client reads every page as
// Google's generated JSON API client, called directly with full responses
// and its own conversion here, does (the oracle for the client's strict
// mapping and its fields selector), every field the schema keeps:
// names, prefixes, tokens, size, generation, metageneration, etag, update
// time, creation time, storage class, MD5, CRC32C, content type and
// deletion time, over pages of a current and a generations listing
// (versions=true), with keys that need escaping and one outside the Basic
// Multilingual Plane, every key listed. Our client is addressed to
// Google's endpoint, where it sends the production fields selector, which
// the fake applies, while the Google client reads full responses; a field
// the selector left out would differ.
func TestClientPagesMatchGoogleClient(t *testing.T) {
	// astral is a key outside the Basic Multilingual Plane (U+1F600: four
	// UTF-8 bytes, a surrogate pair in UTF-16).
	astral := "astral" + string(rune(0x1F600))
	keys := append(testKeys(), "sp ace", "pl+us", "pct%41", "amp&<>", "q\"'", "a?b", "a#b", "ctl\x01", "ctl\x02x", astral)
	f, ep := newFake(t, keys)
	ctx := context.Background()
	u, _ := url.Parse(ep)
	prod, err := client.New(ctx, client.Config{Anonymous: true, HTTPClient: &http.Client{Transport: redirect{u}}})
	if err != nil {
		t.Fatal(err)
	}
	svc, err := storage.NewService(ctx, option.WithEndpoint(ep), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	listed := map[string]bool{}
	// A current and a generations listing (versions=true, and on Google's
	// endpoint the generations field mask).
	for _, start := range []client.ListInput{{MaxResults: 7}, {Versions: true, MaxResults: 9}} {
		in := start
		in.Bucket = "bkt"
		for pages := 0; ; pages++ {
			a, err := prod.List(ctx, &in)
			if err != nil {
				t.Fatal(err)
			}
			if sel := f.Last().Get("fields"); sel == "" || in.Versions != strings.Contains(sel, "timeDeleted") {
				t.Fatalf("fields selector %q on Google's endpoint, versions %v", sel, in.Versions)
			}
			if got := f.Last().Get("versions") == "true"; got != in.Versions {
				t.Fatalf("versions=%v sent for %+v", got, in)
			}
			call := svc.Objects.List("bkt").Prefix(in.Prefix).Delimiter(in.Delimiter).StartOffset(in.StartOffset).
				EndOffset(in.EndOffset).PageToken(in.PageToken).Versions(in.Versions)
			if in.MaxResults > 0 {
				call = call.MaxResults(int64(in.MaxResults))
			}
			b, err := call.Do()
			if err != nil {
				t.Fatal(err)
			}
			if a.NextPageToken != b.NextPageToken || !slices.Equal(a.Prefixes, b.Prefixes) || len(a.Items) != len(b.Items) {
				t.Fatalf("%+v page %d: ours %d items %q next %q, Google %d items %q next %q", start, pages,
					len(a.Items), a.Prefixes, a.NextPageToken, len(b.Items), b.Prefixes, b.NextPageToken)
			}
			for i, o := range a.Items {
				g := b.Items[i]
				up, err := time.Parse(time.RFC3339Nano, g.Updated)
				if err != nil {
					t.Fatal(err)
				}
				created, err := time.Parse(time.RFC3339Nano, g.TimeCreated)
				if err != nil {
					t.Fatal(err)
				}
				want := client.Object{Name: g.Name, Size: int64(g.Size), Updated: up.UnixMicro(), TimeCreated: created.UnixMicro(), ETag: g.Etag,
					Generation: g.Generation, Metageneration: g.Metageneration, StorageClass: g.StorageClass,
					MD5: g.Md5Hash, CRC32C: g.Crc32c, ContentType: g.ContentType}
				if g.TimeDeleted != "" {
					d, err := time.Parse(time.RFC3339Nano, g.TimeDeleted)
					if err != nil {
						t.Fatal(err)
					}
					want.TimeDeleted = d.UnixMicro()
				}
				listed[o.Name] = true
				if o != want || g.StorageClass == "" || g.Md5Hash == "" || g.Metageneration == 0 {
					t.Fatalf("%+v page %d item %d: ours %+v, Google %+v", start, pages, i, o, want)
				}
			}
			if a.NextPageToken == "" {
				break
			}
			in.PageToken = a.NextPageToken
		}
	}
	for _, k := range keys {
		if !listed[k] {
			t.Errorf("%q never listed", k)
		}
	}
}

// TestRowsCarryEveryField: the adapter maps every listed field into the
// schema's row (through the engine, at several pages), timeCreated
// included.
func TestRowsCarryEveryField(t *testing.T) {
	keys := testKeys()
	_, ep := newFake(t, keys)
	p, err := New(context.Background(), Options{Bucket: "bkt", Endpoint: ep, Anonymous: true, Workers: 4})
	if err != nil {
		t.Fatal(err)
	}
	c := &collect{}
	if _, err := order.Run[Range, Object](context.Background(), p, c, &Schema, order.Options{Workers: 4, Budget: 1 << 20}); err != nil {
		t.Fatal(err)
	}
	if len(c.rows) != len(keys) {
		t.Fatalf("%d rows", len(c.rows))
	}
	updated := time.Date(2026, 11, 6, 1, 2, 3, 456789000, time.UTC).UnixMicro()
	for _, r := range c.rows {
		i := slices.Index(keys, r.Key)
		w := gcstest.Object(i, r.Key)
		gen, _ := strconv.ParseInt(w["generation"].(string), 10, 64)
		meta, _ := strconv.ParseInt(w["metageneration"].(string), 10, 64)
		created, err := time.Parse(time.RFC3339Nano, w["timeCreated"].(string))
		if err != nil {
			t.Fatal(err)
		}
		want := Object{Size: int64(len(r.Key)), Mtime: updated, TimeCreated: created.UnixMicro(), ETag: w["etag"].(string), Generation: gen, Metageneration: meta,
			StorageClass: w["storageClass"].(string), MD5: w["md5Hash"].(string), CRC32C: w["crc32c"].(string), ContentType: w["contentType"].(string)}
		if r.Data != want {
			t.Fatalf("%q: row %+v, want %+v", r.Key, r.Data, want)
		}
	}
}
