// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/listing"
	"github.com/varveio/swath/go/internal/replay/store"
	sourcegcs "github.com/varveio/swath/go/internal/source/gcs"
	"github.com/varveio/swath/go/internal/source/gcs/gcstest"
)

// TestNativeSchemaRendersTheBucketsMetadata lists a fake bucket with the
// Go lister (full object resources), serves the dataset it wrote, and
// requires every item to carry the bucket's own generation,
// metageneration, etag, storage class, content type and checksums, with
// size and updated exact, on the flat path and the rollup.
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
		Output: listing.OutputConfig{Dir: out},
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
		if _, ok := it["timeCreated"]; ok {
			t.Fatalf("%s: timeCreated is not stored, so it is not rendered", k)
		}
	}
	if !strings.HasSuffix(fmt.Sprint(got[0]["updated"]), ".456789Z") {
		t.Errorf("updated %v", got[0]["updated"])
	}
}
