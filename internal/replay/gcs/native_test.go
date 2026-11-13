// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/listing"
	"github.com/varveio/swath/internal/replay/store"
	sourcegcs "github.com/varveio/swath/internal/source/gcs"
	"github.com/varveio/swath/internal/source/gcs/gcstest"
)

// TestNativeSchemaRendersTheBucketsMetadata lists a fake bucket with the
// Go lister (full object resources), serves the dataset it wrote, and
// requires every item to carry the bucket's own generation,
// metageneration, etag, storage class, content type and checksums, with
// size, updated and timeCreated exact, on the flat path and the rollup.
func TestNativeSchemaRendersTheBucketsMetadata(t *testing.T) {
	var keys []string
	for i := range 700 {
		keys = append(keys, fmt.Sprintf("d%d/k%04d", i%4, i))
	}
	keys = append(keys, "é/x", "top")
	slices.Sort(keys)
	url := gcstest.NewStrict(keys).Serve(t)
	out := filepath.Join(t.TempDir(), "ds")
	if _, err := listing.Run(context.Background(), listing.Config{
		Source: listing.SourceConfig{URI: "gs://bkt", Options: &sourcegcs.Options{Endpoint: url, Anonymous: true}},
		// A dataset is all these tests need: unjournaled where a journaled run is refused (Windows).
		Output: listing.OutputConfig{Dir: out, NoJournal: !dataset.DirSyncEnforced},
		Limits: listing.Limits{PartSize: 6 << 10, PartInterval: -1},
	}); err != nil {
		t.Fatal(err)
	}
	st, meta, err := store.Open(context.Background(), out, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if meta.SchemaID != NativeSchemaID {
		t.Fatalf("%+v", meta)
	}
	p := NewPager(st, meta.Identity)
	if !p.Native() {
		t.Fatal("the GCS schema is not rendered natively")
	}
	type item map[string]any
	var got []item
	for _, delimiter := range []string{"", "/"} {
		token, prefixes := "", 0
		for {
			q := "maxResults=97&projection=full"
			if delimiter != "" {
				q += "&delimiter=%2F"
			}
			if token != "" {
				q += "&pageToken=" + token
			}
			r, err := Parse("bkt", q)
			if err != nil {
				t.Fatal(err)
			}
			page, err := p.List(context.Background(), r)
			if err != nil {
				t.Fatal(err)
			}
			body, err := AppendPage(nil, r, page)
			if err != nil {
				t.Fatal(err)
			}
			var doc struct {
				Items    []item
				Prefixes []string
			}
			if err := json.Unmarshal(body, &doc); err != nil {
				t.Fatal(err)
			}
			if delimiter == "" {
				got = append(got, doc.Items...)
			} else if len(doc.Items) != 1 || doc.Items[0]["generation"] != fmt.Sprint(1700000000000000+slices.Index(keys, "top")) {
				t.Errorf("rollup items: %v", doc.Items)
			}
			prefixes += len(doc.Prefixes)
			if token = page.Token; token == "" {
				break
			}
		}
		if delimiter == "/" && prefixes != 5 {
			t.Errorf("rollup: %d prefixes", prefixes)
		}
	}
	if len(got) != len(keys) {
		t.Fatalf("%d items, want %d", len(got), len(keys))
	}
	for i, it := range got {
		k := keys[i]
		want := gcstest.Object(i, k)
		for _, f := range []string{"name", "size", "updated", "etag", "generation", "metageneration", "storageClass", "contentType", "md5Hash", "crc32c"} {
			if fmt.Sprint(it[f]) != fmt.Sprint(want[f]) {
				t.Fatalf("%s %s: %v, want %v", k, f, it[f], want[f])
			}
		}
		if acl, ok := it["acl"].([]any); !ok || len(acl) != 0 || it["bucket"] != "bkt" || it["kind"] != "storage#object" {
			t.Fatalf("%s: %v", k, it)
		}
		created, err := time.Parse(time.RFC3339Nano, want["timeCreated"].(string))
		if err != nil {
			t.Fatal(err)
		}
		if got := fmt.Sprint(it["timeCreated"]); got != created.UTC().Format("2006-01-02T15:04:05.000000Z") {
			t.Fatalf("%s timeCreated: %v, want %v", k, got, want["timeCreated"])
		}
	}
	if !strings.HasSuffix(fmt.Sprint(got[0]["updated"]), ".456789Z") {
		t.Errorf("updated %v", got[0]["updated"])
	}
}

// TestNativeNullContentTypeIsOmitted: live GCS lists an object stored
// without a content type with no contentType field (pinned by the live
// capture replay of cloud-samples-data, go/docs/internals/swath-replay.md, Live
// conformance), so a native row whose content_type is null renders none;
// a fixture without GCS metadata keeps the synthetic profile's value.
func TestNativeNullContentTypeIsOmitted(t *testing.T) {
	r, err := Parse("bkt", "maxResults=5")
	if err != nil {
		t.Fatal(err)
	}
	ct := "text/plain"
	for _, c := range []struct {
		native *Native
		want   string
	}{
		{&Native{}, ""},
		{&Native{ContentType: &ct}, `"contentType":"text/plain"`},
		{nil, `"contentType":"application/octet-stream"`},
	} {
		body, err := AppendPage(nil, r, &Page{Items: []Item{{Key: []byte("a"), Native: c.native}}})
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.Contains(string(body), `"contentType"`); got != (c.want != "") || c.want != "" && !strings.Contains(string(body), c.want) {
			t.Errorf("native %+v: %s", c.native, body)
		}
	}
}

// TestNativeTimeCreatedIsRenderedWhenStored: a native row's stored
// creation time is listed as timeCreated, just before updated as live GCS
// lists it, with updated's six fractional digits; a null (a part written
// before the column was recorded) and a fixture without GCS metadata list
// none; a time outside years 0 to 9999 is a fixture problem, as for
// updated.
func TestNativeTimeCreatedIsRenderedWhenStored(t *testing.T) {
	r, err := Parse("bkt", "maxResults=5")
	if err != nil {
		t.Fatal(err)
	}
	micros := func(v int64) *int64 { return &v }
	for _, c := range []struct {
		native *Native
		want   string
	}{
		{&Native{TimeCreated: micros(1_700_000_000_123_456)}, `"size":"0","timeCreated":"2023-11-14T22:13:20.123456Z","updated":"1970-01-01T00:00:00.000000Z"`},
		{&Native{TimeCreated: micros(-1)}, `"timeCreated":"1969-12-31T23:59:59.999999Z","updated"`},
		{&Native{}, `"size":"0","updated"`},
		{nil, `"size":"0","updated"`},
	} {
		body, err := AppendPage(nil, r, &Page{Items: []Item{{Key: []byte("a"), Native: c.native}}})
		if err != nil {
			t.Fatal(err)
		}
		stored := c.native != nil && c.native.TimeCreated != nil
		if !strings.Contains(string(body), c.want) || stored != strings.Contains(string(body), "timeCreated") {
			t.Errorf("native %+v: %s", c.native, body)
		}
	}
	for _, v := range []int64{-62_167_219_200_000_001, 253_402_300_800_000_000} { // before year 0, after year 9999
		_, err := AppendPage(nil, r, &Page{Items: []Item{{Key: []byte("a"), Native: &Native{TimeCreated: micros(v)}}}})
		var problem *FixtureProblem
		if !errors.As(err, &problem) || problem.Reason != "timestamp_out_of_range" {
			t.Errorf("timeCreated %d: %v", v, err)
		}
	}
}
