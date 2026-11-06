// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"context"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"testing"
	"time"

	"google.golang.org/api/option"
	storage "google.golang.org/api/storage/v1"

	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/source/gcs/client"
)

// redirect sends requests addressed to Google's endpoint to the fake, so
// the client takes its production path (the fields selector included).
type redirect struct{ to *url.URL }

func (r redirect) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.URL.Scheme, req.URL.Host, req.Host = r.to.Scheme, r.to.Host, r.to.Host
	return http.DefaultTransport.RoundTrip(req)
}

// TestClientPagesMatchGoogleClient: the hand-rolled objects.list call
// reads every page as Google's generated JSON API client does (test-only
// oracle; the binary does not link it), every field the schema keeps:
// names, prefixes, tokens, size, generation, metageneration, etag, update
// time, storage class, MD5, CRC32C and content type, across prefixes,
// delimiters and both offsets, with keys that need escaping. Our client
// runs twice: against the fake's endpoint (full responses) and addressed
// to Google's endpoint, where it sends the production fields selector,
// which the fake applies, while the Google client reads full responses;
// a field the selector left out would differ.
func TestClientPagesMatchGoogleClient(t *testing.T) {
	keys := append(testKeys(), "sp ace", "pl+us", "pct%41", "amp&<>", "q\"'", "a?b", "a#b", "ctl\x01", "ctl\x02x")
	f, ep := newFake(t, keys)
	ctx := context.Background()
	plain, err := client.New(ctx, client.Config{Endpoint: ep, Anonymous: true})
	if err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(ep)
	prod, err := client.New(ctx, client.Config{Anonymous: true, HTTPClient: &http.Client{Transport: redirect{u}}})
	if err != nil {
		t.Fatal(err)
	}
	svc, err := storage.NewService(ctx, option.WithEndpoint(ep), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	for _, start := range []client.ListInput{
		{MaxResults: 7},
		{},
		{Prefix: "data/year=2021/", MaxResults: 100},
		{StartOffset: "a/x", EndOffset: "data/year=2020/part-00100.parquet\x00", MaxResults: 50},
		{Delimiter: "/", MaxResults: 3},
		{Prefix: "a/", Delimiter: "/"},
		{StartOffset: "pct%41", EndOffset: "sp ace"},
		{StartOffset: "ctl\x01", MaxResults: 1},
	} {
		for name, ours := range map[string]*client.Client{"plain": plain, "production fields": prod} {
			in := start
			in.Bucket = "bkt"
			for pages := 0; ; pages++ {
				a, err := ours.List(ctx, &in)
				if err != nil {
					t.Fatal(err)
				}
				if name == "production fields" {
					if sel := f.last.Load().(url.Values).Get("fields"); sel == "" {
						t.Fatal("no fields selector on Google's endpoint")
					}
				}
				call := svc.Objects.List("bkt").Prefix(in.Prefix).Delimiter(in.Delimiter).StartOffset(in.StartOffset).
					EndOffset(in.EndOffset).PageToken(in.PageToken)
				if in.MaxResults > 0 {
					call = call.MaxResults(int64(in.MaxResults))
				}
				b, err := call.Do()
				if err != nil {
					t.Fatal(err)
				}
				if a.NextPageToken != b.NextPageToken || !slices.Equal(a.Prefixes, b.Prefixes) || len(a.Items) != len(b.Items) {
					t.Fatalf("%s %+v page %d: ours %d items %q next %q, Google %d items %q next %q", name, start, pages,
						len(a.Items), a.Prefixes, a.NextPageToken, len(b.Items), b.Prefixes, b.NextPageToken)
				}
				for i, o := range a.Items {
					g := b.Items[i]
					up, err := time.Parse(time.RFC3339Nano, g.Updated)
					if err != nil {
						t.Fatal(err)
					}
					want := client.Object{Name: g.Name, Size: int64(g.Size), Updated: up.UnixMicro(), ETag: g.Etag,
						Generation: g.Generation, Metageneration: g.Metageneration, StorageClass: g.StorageClass,
						MD5: g.Md5Hash, CRC32C: g.Crc32c, ContentType: g.ContentType}
					if o != want || g.StorageClass == "" || g.Md5Hash == "" || g.Metageneration == 0 {
						t.Fatalf("%s %+v page %d item %d: ours %+v, Google %+v", name, start, pages, i, o, want)
					}
				}
				if a.NextPageToken == "" {
					break
				}
				in.PageToken = a.NextPageToken
			}
		}
	}
}

// TestRowsCarryEveryField: the adapter maps every listed field into the
// schema's row (through the engine, at several pages).
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
		w := object(i, r.Key)
		gen, _ := strconv.ParseInt(w["generation"].(string), 10, 64)
		meta, _ := strconv.ParseInt(w["metageneration"].(string), 10, 64)
		want := Object{Size: int64(len(r.Key)), Mtime: updated, ETag: w["etag"].(string), Generation: gen, Metageneration: meta,
			StorageClass: w["storageClass"].(string), MD5: w["md5Hash"].(string), CRC32C: w["crc32c"].(string), ContentType: w["contentType"].(string)}
		if r.Data != want {
			t.Fatalf("%q: row %+v, want %+v", r.Key, r.Data, want)
		}
	}
}
