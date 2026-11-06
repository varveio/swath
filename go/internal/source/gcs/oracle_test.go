// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"context"
	"slices"
	"testing"
	"time"

	"google.golang.org/api/option"
	storage "google.golang.org/api/storage/v1"

	"github.com/varveio/swath/go/internal/source/gcs/client"
)

// TestClientPagesMatchGoogleClient: the hand-rolled objects.list call
// reads every page as Google's generated JSON API client does (test-only
// oracle; the binary does not link it): names, prefixes, tokens, sizes,
// generations and update times, across prefixes, delimiters and both
// offsets, with keys that need escaping.
func TestClientPagesMatchGoogleClient(t *testing.T) {
	keys := append(testKeys(), "sp ace", "pl+us", "pct%41", "amp&<>", "q\"'", "a?b", "a#b")
	_, ep := newFake(t, keys)
	ctx := context.Background()
	ours, err := client.New(ctx, client.Config{Endpoint: ep, Anonymous: true})
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
	} {
		in := start
		in.Bucket = "bkt"
		for pages := 0; ; pages++ {
			a, err := ours.List(ctx, &in)
			if err != nil {
				t.Fatal(err)
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
				t.Fatalf("%+v page %d: ours %d items %q next %q, Google %d items %q next %q", start, pages,
					len(a.Items), a.Prefixes, a.NextPageToken, len(b.Items), b.Prefixes, b.NextPageToken)
			}
			for i, o := range a.Items {
				g := b.Items[i]
				up, err := time.Parse(time.RFC3339Nano, g.Updated)
				if err != nil {
					t.Fatal(err)
				}
				if o.Name != g.Name || uint64(o.Size) != g.Size || o.Generation != g.Generation || o.ETag != g.Etag || o.Updated != up.UnixMicro() {
					t.Fatalf("%+v page %d item %d: ours %+v, Google %+v", start, pages, i, o, g)
				}
			}
			if a.NextPageToken == "" {
				break
			}
			in.PageToken = a.NextPageToken
		}
	}
}
