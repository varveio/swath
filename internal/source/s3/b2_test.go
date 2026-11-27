// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/source/keyrange"
	"github.com/varveio/swath/internal/source/keyrange/keyrangetest"
	"github.com/varveio/swath/internal/source/objhttp"
	"github.com/varveio/swath/internal/source/s3/client"
)

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

// b2Listing is the sorted fake listing with Backblaze B2's bound limit: a
// start-after over 1,023 bytes is refused, and so is B2's own continuation
// token after a page whose last key is (B2's token is that key).
type b2Listing struct {
	*keyrangetest.Sorted
	refused int
}

func (b *b2Listing) ListObjectsV2(ctx context.Context, in *client.ListInput) (*client.Page, error) {
	long := len(in.StartAfter) > b2MaxBound
	if tok := in.ContinuationToken; tok != "" {
		i, err := strconv.Atoi(tok)
		if err != nil {
			return nil, &client.Error{Status: 400, Code: "InvalidArgument", Message: "not a token of this store"}
		}
		long = len(b.Keys[i-1]) > b2MaxBound
	}
	if long {
		b.refused++
		return nil, &client.Error{Status: 400, Code: "InvalidRequest", Message: "File name in UTF8 must be no more than 1024 bytes"}
	}
	out, err := b.List(ctx, &keyrangetest.ListInput{Prefix: in.Prefix, Delimiter: in.Delimiter, StartAfter: in.StartAfter,
		ContinuationToken: in.ContinuationToken, MaxKeys: in.MaxKeys})
	if err != nil {
		return nil, err
	}
	pg := &client.Page{CommonPrefixes: out.CommonPrefixes, IsTruncated: out.IsTruncated, NextContinuationToken: out.NextContinuationToken}
	for _, o := range out.Contents {
		pg.Contents = append(pg.Contents, client.Object{Key: o.Key, Size: o.Size})
	}
	return pg, nil
}

// longKeys are short keys around runs of 1,024-byte keys (the longest B2
// stores), some ending in a multibyte character.
func longKeys() []string {
	var keys []string
	for i := range 300 {
		keys = append(keys, fmt.Sprintf("a/%04d", i))
	}
	for i := range 700 {
		pad := strings.Repeat("x", 1024-len("m/")-5)
		keys = append(keys, fmt.Sprintf("m/%s%05d", pad, i))
	}
	for i := range 300 {
		pad := strings.Repeat("y", 1024-len("n/")-3-2)
		keys = append(keys, fmt.Sprintf("n/%s%03dé", pad, i)) // é is 2 bytes: 1,024 in all
	}
	for i := range 300 {
		keys = append(keys, fmt.Sprintf("z/%04d", i))
	}
	slices.Sort(keys)
	return keys
}

// TestB2LongKeys: on a B2 endpoint swath lists past pages that end on a
// 1,024-byte key, sequentially, in parallel and resumed after such a key,
// sending no start-after over 1,023 bytes and no token B2 refuses.
func TestB2LongKeys(t *testing.T) {
	keys := longKeys()
	for _, c := range []struct {
		name    string
		workers int
		policy  keyrange.Policy
		after   string
	}{
		{name: "sequential", workers: 1, policy: keyrange.Policy{Sequential: true, PageSize: 100}},
		{name: "parallel", workers: 8, policy: keyrange.Policy{PageSize: 100}},
		{name: "resume after a long key", workers: 4, policy: keyrange.Policy{PageSize: 100}, after: keys[650]},
	} {
		t.Run(c.name, func(t *testing.T) {
			b := &b2Listing{Sorted: keyrangetest.NewSorted(keys, false, 1)}
			p, err := New(t.Context(), Options{Bucket: "b", Client: b, Workers: c.workers, Policy: c.policy,
				Endpoint: "https://s3.us-east-005.backblazeb2.com"})
			if err != nil {
				t.Fatal(err)
			}
			want := keys
			if c.after != "" {
				if err := p.Restart(c.after); err != nil {
					t.Fatal(err)
				}
				want = keys[651:]
			}
			col := &collect{}
			if _, err := order.Run[Range, Object](t.Context(), p, col, &Schema, order.Options{Workers: c.workers, Budget: 100 * 256}); err != nil {
				t.Fatal(err)
			}
			if got := keysOf(col.rows); !slices.Equal(got, want) {
				t.Fatalf("%d keys, want %d", len(got), len(want))
			}
			if b.refused != 0 || p.Stats().Counters.Value("b2.long_key_restarts") == 0 {
				t.Fatalf("refused %d, restarts %d", b.refused, p.Stats().Counters.Value("b2.long_key_restarts"))
			}
		})
	}
}

// TestB2ContinueDrops: a restart page drops the keys and common prefixes
// at or below its point, and hands on B2's token, or a restart when the
// page ends on a long key.
func TestB2ContinueDrops(t *testing.T) {
	long := "k/" + strings.Repeat("x", 1022)
	out := &client.Page{Contents: []client.Object{{Key: long[:1023]}, {Key: long}, {Key: "l"}},
		CommonPrefixes: []string{"k/"}, IsTruncated: true, NextContinuationToken: "T"}
	tok, restarted, err := b2Continue(long, out)
	if err != nil || restarted || tok != "T" || len(out.Contents) != 1 || out.Contents[0].Key != "l" || len(out.CommonPrefixes) != 0 {
		t.Fatalf("tok=%q restarted=%v err=%v page=%+v", tok, restarted, err, out)
	}
	out = &client.Page{Contents: []client.Object{{Key: "a"}, {Key: long}}, IsTruncated: true, NextContinuationToken: "T"}
	if tok, restarted, err := b2Continue("", out); err != nil || !restarted || tok != b2Restart+long {
		t.Fatalf("tok=%q restarted=%v err=%v", tok, restarted, err)
	}
	if got := b2Start(strings.Repeat("x", 1022) + "é"); got != strings.Repeat("x", 1022) {
		t.Fatalf("b2Start cut a character: %q", got[1015:])
	}
	// A restart that cannot pass its point (a page of keys at or below it) stops.
	out = &client.Page{Contents: []client.Object{{Key: long}}, IsTruncated: true, NextContinuationToken: "T"}
	if _, _, err := b2Continue(long, out); !errors.Is(err, objhttp.ErrInvalidPage) {
		t.Fatal(err)
	}
}
