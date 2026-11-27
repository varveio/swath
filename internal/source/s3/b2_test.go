// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/varveio/swath/internal/source/objhttp"
	"github.com/varveio/swath/internal/source/s3/client"
)

func TestB2Endpoint(t *testing.T) {
	for ep, want := range map[string]bool{
		"https://s3.us-east-005.backblazeb2.com":        true,
		"https://s3.eu-central-003.backblazeb2.com/":    true,
		"https://bucket.s3.us-west-004.backblazeb2.com": true,
		"s3.us-west-002.backblazeb2.com":                true,
		"HTTPS://S3.US-EAST-005.BACKBLAZEB2.COM.":       true,
		"":                                    false,
		"https://backblazeb2.com.example.com": false,
		"https://s3.amazonaws.com":            false,
		"https://ACCOUNT.r2.cloudflarestorage.com":      false,
		"http://127.0.0.1:9000/?host=x.backblazeb2.com": false,
		"https://notbackblazeb2.com":                    false,
	} {
		if got := b2Endpoint(ep); got != want {
			t.Errorf("%q: %v, want %v", ep, got, want)
		}
	}
}

func at(v client.Version, mtime int64) client.Version { v.Mtime = mtime; return v }

// b2Pages are two ListObjectVersions pages as Backblaze B2 sends them:
// each page's delete markers first, then its versions, and on the
// continuation the marker key's first entry marked IsLatest. In S3's
// order the first page ends at d's version 2, which is where B2 cuts it.
func b2Pages() []client.VersionPage {
	return []client.VersionPage{
		{Versions: []client.Version{
			at(ver("b", "b-dm", true, true), 5), at(ver("d", "d-dm", false, true), 3),
			at(ver("a", "a2", true, false), 9), at(ver("a", "a1", false, false), 8), at(ver("b", "b1", false, false), 4),
			at(ver("c", "c1", true, false), 7), at(ver("d", "d3", true, false), 6), at(ver("d", "d2", false, false), 2),
		}, IsTruncated: true, NextKeyMarker: "d", NextVersionIDMarker: "d2"},
		{Versions: []client.Version{at(ver("d", "d1", true, false), 1), at(ver("e", "e1", true, false), 10)}},
	}
}

// TestB2VersionsRepaired: on a B2 endpoint the pages are put in S3's order
// and the continuation's false IsLatest is cleared, and the report counts
// both.
func TestB2VersionsRepaired(t *testing.T) {
	f := &pagedVersions{t: t, pages: b2Pages()}
	p, err := NewVersions(t.Context(), Options{Bucket: "b", Workers: 1, Client: f, Endpoint: "https://s3.us-east-005.backblazeb2.com"})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := runVersions(t, p)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, r := range rows {
		s := r.Data.VersionID
		if r.Data.IsLatest {
			s += "*"
		}
		got = append(got, s)
	}
	want := []string{"a2*", "a1", "b-dm*", "b1", "c1*", "d3*", "d-dm", "d2", "d1", "e1*"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%q, want %q", got, want)
	}
	c := p.Stats().Counters
	if c.Value("b2.versions_reordered") != 1 || c.Value("b2.latest_cleared") != 1 {
		t.Fatalf("reordered %d cleared %d", c.Value("b2.versions_reordered"), c.Value("b2.latest_cleared"))
	}
}

// TestB2VersionsOnlyOnB2: the same pages from another endpoint are not
// repaired; the run stops (protocol) rather than write them.
func TestB2VersionsOnlyOnB2(t *testing.T) {
	f := &pagedVersions{t: t, pages: b2Pages()}
	p, err := NewVersions(t.Context(), Options{Bucket: "b", Workers: 1, Client: f, Endpoint: "https://minio.example.com"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runVersions(t, p); !errors.Is(err, objhttp.ErrInvalidPage) {
		t.Fatalf("err=%v", err)
	}
}

// TestB2VersionsTieRefused: a delete marker and a version of one key in
// the same millisecond cannot be ordered; the page is invalid.
func TestB2VersionsTieRefused(t *testing.T) {
	f := &pagedVersions{t: t, pages: []client.VersionPage{{Versions: []client.Version{
		at(ver("k", "dm", true, true), 5), at(ver("k", "v1", false, false), 5)}}}}
	p, err := NewVersions(t.Context(), Options{Bucket: "b", Workers: 1, Client: f, Endpoint: "https://s3.us-east-005.backblazeb2.com"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runVersions(t, p); !errors.Is(err, objhttp.ErrInvalidPage) || !strings.Contains(err.Error(), "share a LastModified") {
		t.Fatalf("err=%v", err)
	}
	// Two versions in one millisecond keep B2's order.
	f = &pagedVersions{t: t, pages: []client.VersionPage{{Versions: []client.Version{
		at(ver("k", "v2", true, false), 5), at(ver("k", "v1", false, false), 5)}}}}
	p, err = NewVersions(t.Context(), Options{Bucket: "b", Workers: 1, Client: f, Endpoint: "https://s3.us-east-005.backblazeb2.com"})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := runVersions(t, p)
	if err != nil || len(rows) != 2 || rows[0].Data.VersionID != "v2" {
		t.Fatalf("err=%v rows=%+v", err, rows)
	}
}
