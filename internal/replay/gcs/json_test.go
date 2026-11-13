// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"bytes"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/varveio/swath/internal/replay/s3"
)

// evidenceIdentity is the fixture identity of the GCS evidence corpus the
// golden pages were answered from (bucket replay-bucket).
const evidenceIdentity = "6576cb29c300ff3962389bacd73903bcebf92ce4feb3158441f96d1a282da508"

// javaPages are pages the Java replay server answered on its GCS route
// for these queries over the GCS evidence corpus: names with control
// characters, DEL, U+2028, a BOM, a no-break space, backslashes, flat and
// delimiter pages, a full projection, and tokens resuming exclusive at an
// object and inclusive past a prefix.
var javaPages = []struct{ query, body string }{
	{"prefix=probe/r20261021t172056z-987e3b56/n/&startOffset=probe/r20261021t172056z-987e3b56/n/bom&maxResults=7",
		"{\"kind\":\"storage#objects\",\"items\":[{\"kind\":\"storage#object\",\"bucket\":\"replay-bucket\",\"name\":\"probe/r20261021t172056z-987e3b56/n/bom\xef\xbb\xbfx\",\"size\":\"4\",\"updated\":\"2026-10-21T17:21:09.119000Z\",\"etag\":\"swath-gcs-synthetic-v1\",\"generation\":\"1\",\"metageneration\":\"1\",\"storageClass\":\"STANDARD\",\"contentType\":\"application/octet-stream\"},{\"kind\":\"storage#object\",\"bucket\":\"replay-bucket\",\"name\":\"probe/r20261021t172056z-987e3b56/n/ctl\\u0001x\",\"size\":\"1\",\"updated\":\"2026-10-21T17:21:08.584000Z\",\"etag\":\"swath-gcs-synthetic-v1\",\"generation\":\"1\",\"metageneration\":\"1\",\"storageClass\":\"STANDARD\",\"contentType\":\"application/octet-stream\"},{\"kind\":\"storage#object\",\"bucket\":\"replay-bucket\",\"name\":\"probe/r20261021t172056z-987e3b56/n/del\x7fx\",\"size\":\"2\",\"updated\":\"2026-10-21T17:21:08.737000Z\",\"etag\":\"swath-gcs-synthetic-v1\",\"generation\":\"1\",\"metageneration\":\"1\",\"storageClass\":\"STANDARD\",\"contentType\":\"application/octet-stream\"},{\"kind\":\"storage#object\",\"bucket\":\"replay-bucket\",\"name\":\"probe/r20261021t172056z-987e3b56/n/dots..\",\"size\":\"6\",\"updated\":\"2026-10-21T17:21:07.432000Z\",\"etag\":\"swath-gcs-synthetic-v1\",\"generation\":\"1\",\"metageneration\":\"1\",\"storageClass\":\"STANDARD\",\"contentType\":\"application/octet-stream\"},{\"kind\":\"storage#object\",\"bucket\":\"replay-bucket\",\"name\":\"probe/r20261021t172056z-987e3b56/n/end.\",\"size\":\"5\",\"updated\":\"2026-10-21T17:21:07.298000Z\",\"etag\":\"swath-gcs-synthetic-v1\",\"generation\":\"1\",\"metageneration\":\"1\",\"storageClass\":\"STANDARD\",\"contentType\":\"application/octet-stream\"},{\"kind\":\"storage#object\",\"bucket\":\"replay-bucket\",\"name\":\"probe/r20261021t172056z-987e3b56/n/ls\xe2\x80\xa8x\",\"size\":\"3\",\"updated\":\"2026-10-21T17:21:08.990000Z\",\"etag\":\"swath-gcs-synthetic-v1\",\"generation\":\"1\",\"metageneration\":\"1\",\"storageClass\":\"STANDARD\",\"contentType\":\"application/octet-stream\"},{\"kind\":\"storage#object\",\"bucket\":\"replay-bucket\",\"name\":\"probe/r20261021t172056z-987e3b56/n/nbsp\xc2\xa0x\",\"size\":\"3\",\"updated\":\"2026-10-21T17:21:07.914000Z\",\"etag\":\"swath-gcs-synthetic-v1\",\"generation\":\"1\",\"metageneration\":\"1\",\"storageClass\":\"STANDARD\",\"contentType\":\"application/octet-stream\"}],\"nextPageToken\":\"gcs1.AABUz-pH-OdJ0j3Sg0OKbJrzcHJvYmUvcjIwMjYxMDIxdDE3MjA1NnotOTg3ZTNiNTYvbi9uYnNwwqB4\"}"},
	{"prefix=probe/r20261021t172056z-987e3b56/p/&maxResults=3&projection=full",
		"{\"kind\":\"storage#objects\",\"items\":[{\"kind\":\"storage#object\",\"bucket\":\"replay-bucket\",\"name\":\"probe/r20261021t172056z-987e3b56/p/ lead\",\"size\":\"2\",\"updated\":\"2026-10-21T17:21:04.991000Z\",\"etag\":\"swath-gcs-synthetic-v1\",\"generation\":\"1\",\"metageneration\":\"1\",\"storageClass\":\"STANDARD\",\"contentType\":\"application/octet-stream\",\"acl\":[]},{\"kind\":\"storage#object\",\"bucket\":\"replay-bucket\",\"name\":\"probe/r20261021t172056z-987e3b56/p/a b\",\"size\":\"4\",\"updated\":\"2026-10-21T17:21:01.643000Z\",\"etag\":\"swath-gcs-synthetic-v1\",\"generation\":\"1\",\"metageneration\":\"1\",\"storageClass\":\"STANDARD\",\"contentType\":\"application/octet-stream\",\"acl\":[]},{\"kind\":\"storage#object\",\"bucket\":\"replay-bucket\",\"name\":\"probe/r20261021t172056z-987e3b56/p/a%20b\",\"size\":\"7\",\"updated\":\"2026-10-21T17:21:01.991000Z\",\"etag\":\"swath-gcs-synthetic-v1\",\"generation\":\"1\",\"metageneration\":\"1\",\"storageClass\":\"STANDARD\",\"contentType\":\"application/octet-stream\",\"acl\":[]}],\"nextPageToken\":\"gcs1.AAALshdEj3Iu_vZmYFiCg7EbcHJvYmUvcjIwMjYxMDIxdDE3MjA1NnotOTg3ZTNiNTYvcC9hJTIwYg\"}"},
	{"prefix=probe/r20261021t172056z-987e3b56/&delimiter=/&maxResults=3",
		"{\"kind\":\"storage#objects\",\"prefixes\":[\"probe/r20261021t172056z-987e3b56/b/\",\"probe/r20261021t172056z-987e3b56/d/\",\"probe/r20261021t172056z-987e3b56/n/\"],\"nextPageToken\":\"gcs1.AQA43E-vJFs_q-zsLhN4EaIScHJvYmUvcjIwMjYxMDIxdDE3MjA1NnotOTg3ZTNiNTYvbjA\"}"},
	{"prefix=probe/r20261021t172056z-987e3b56/d/&delimiter=/&startOffset=probe/r20261021t172056z-987e3b56/d/a/m&maxResults=2",
		"{\"kind\":\"storage#objects\",\"items\":[{\"kind\":\"storage#object\",\"bucket\":\"replay-bucket\",\"name\":\"probe/r20261021t172056z-987e3b56/d/a0\",\"size\":\"1\",\"updated\":\"2026-10-21T17:21:05.836000Z\",\"etag\":\"swath-gcs-synthetic-v1\",\"generation\":\"1\",\"metageneration\":\"1\",\"storageClass\":\"STANDARD\",\"contentType\":\"application/octet-stream\"}],\"prefixes\":[\"probe/r20261021t172056z-987e3b56/d/a/\"],\"nextPageToken\":\"gcs1.AACDhddmJaKhYX32WAgHUwkLcHJvYmUvcjIwMjYxMDIxdDE3MjA1NnotOTg3ZTNiNTYvZC9hMA\"}"},
	{"prefix=probe/r20261021t172056z-987e3b56/p/b&maxResults=2",
		"{\"kind\":\"storage#objects\",\"items\":[{\"kind\":\"storage#object\",\"bucket\":\"replay-bucket\",\"name\":\"probe/r20261021t172056z-987e3b56/p/brk[x]\",\"size\":\"6\",\"updated\":\"2026-10-21T17:21:03.723000Z\",\"etag\":\"swath-gcs-synthetic-v1\",\"generation\":\"1\",\"metageneration\":\"1\",\"storageClass\":\"STANDARD\",\"contentType\":\"application/octet-stream\"},{\"kind\":\"storage#object\",\"bucket\":\"replay-bucket\",\"name\":\"probe/r20261021t172056z-987e3b56/p/bs\\\\x\",\"size\":\"3\",\"updated\":\"2026-10-21T17:21:05.097000Z\",\"etag\":\"swath-gcs-synthetic-v1\",\"generation\":\"1\",\"metageneration\":\"1\",\"storageClass\":\"STANDARD\",\"contentType\":\"application/octet-stream\"}]}"},
}

// TestRenderGolden rebuilds each Java page from its own items, prefixes
// and resume point and requires the same bytes: field order, escapes,
// six-digit times, the synthetic profile, the acl of a full projection,
// and the token (flag, binding to scope and fixture, boundary key).
func TestRenderGolden(t *testing.T) {
	for _, g := range javaPages {
		r, err := Parse("replay-bucket", g.query)
		if err != nil {
			t.Fatal(err)
		}
		var doc struct {
			Items []struct {
				Name, Size, Updated string
			}
			Prefixes      []string
			NextPageToken string
		}
		if err := json.Unmarshal([]byte(g.body), &doc); err != nil {
			t.Fatal(err)
		}
		page := &Page{}
		var last boundary
		for _, it := range doc.Items {
			size, _ := strconv.ParseInt(it.Size, 10, 64)
			tm, err := time.Parse(time.RFC3339Nano, it.Updated)
			if err != nil {
				t.Fatal(err)
			}
			page.Items = append(page.Items, Item{Key: []byte(it.Name), Size: size, Updated: tm.UnixMicro()})
			if bytes.Compare([]byte(it.Name), last.key) > 0 {
				last = boundary{[]byte(it.Name), false}
			}
		}
		for _, p := range doc.Prefixes {
			page.Prefixes = append(page.Prefixes, []byte(p))
			if next, _ := s3.Successor([]byte(p)); bytes.Compare(next, last.key) > 0 {
				last = boundary{next, true}
			}
		}
		binding := tokenBinding(r, evidenceIdentity)
		if doc.NextPageToken != "" {
			page.Token = encodeToken(last.inclusive, last.key, binding, 0)
			if _, err := decodeToken(doc.NextPageToken, binding); err != nil {
				t.Errorf("%s: the Java token does not decode: %v", g.query, err)
			}
		}
		got, err := AppendPage(nil, r, page)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != g.body {
			i := 0
			for i < min(len(got), len(g.body)) && got[i] == g.body[i] {
				i++
			}
			t.Errorf("%s: differs at byte %d:\n got %q\nwant %q", g.query, i, got[max(0, i-40):min(len(got), i+60)], g.body[max(0, i-40):min(len(g.body), i+60)])
		}
	}
}
