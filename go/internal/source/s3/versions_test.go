// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/s3/client"
)

// TestVersionsSchemaColumns: s3-versions/v1 is s3-objects/v1's columns in
// the same order (names, types, encodings), then version_id, is_latest and
// is_delete_marker; it is grouped and s3-objects/v1 is not.
func TestVersionsSchemaColumns(t *testing.T) {
	s := &VersionsSchema
	if s.ID != "swath/s3-versions/v1" || !s.Groups || Schema.Groups || s.Name != Schema.Name || s.Key != Schema.Key ||
		s.MaxKeyBytes != Schema.MaxKeyBytes || s.QuotedETag != Schema.QuotedETag {
		t.Fatalf("%+v", s.Info)
	}
	if len(s.Columns) != len(Schema.Columns)+3 {
		t.Fatalf("%d columns", len(s.Columns))
	}
	for i, c := range Schema.Columns {
		if v := s.Columns[i]; v.Spec != c.Spec || v.Delta != c.Delta {
			t.Errorf("column %d: %+v, want %+v", i, v.Spec, c.Spec)
		}
	}
	want := []schema.Spec{{Name: "version_id", Type: schema.String}, {Name: "is_latest", Type: schema.Bool, Required: true},
		{Name: "is_delete_marker", Type: schema.Bool, Required: true}}
	for i, w := range want {
		if got := s.Columns[len(Schema.Columns)+i].Spec; got != w {
			t.Errorf("column %d: %+v, want %+v", len(Schema.Columns)+i, got, w)
		}
	}
}

// cells reads a payload through the schema's accessors: a value, or nil
// for null.
func cells(s *schema.Schema[Version], v *Version) map[string]any {
	out := map[string]any{}
	for _, c := range s.Columns {
		var val any
		var ok bool
		switch {
		case c.Int != nil:
			val, ok = c.Int(v)
		case c.Str != nil:
			val, ok = c.Str(v)
		case c.Bool != nil:
			val, ok = c.Bool(v)
		}
		if !ok {
			val = nil
		}
		out[c.Name] = val
	}
	return out
}

// TestVersionsColumnsNullability: a delete marker has a null size, ETag,
// storage class and checksums (not a zero-byte object); a zero-byte
// version has size 0; "null" is a version ID like any other.
func TestVersionsColumnsNullability(t *testing.T) {
	dm := Version{Object: Object{Mtime: 7, OwnerID: "o"}, VersionID: "null", IsLatest: true, DeleteMarker: true}
	got := cells(&VersionsSchema, &dm)
	want := map[string]any{"size": nil, "last_modified": int64(7), "etag": nil, "storage_class": nil, "owner_id": "o",
		"owner_display_name": nil, "checksum_algorithm": nil, "checksum_type": nil, "version_id": "null", "is_latest": true, "is_delete_marker": true}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("delete marker %v, want %v", got, want)
	}
	empty := Version{Object: Object{ETag: "d41d8cd98f00b204e9800998ecf8427e", StorageClass: "STANDARD"}, VersionID: "v"}
	got = cells(&VersionsSchema, &empty)
	if got["size"] != int64(0) || got["is_delete_marker"] != false || got["is_latest"] != false || got["etag"] == nil {
		t.Fatalf("zero-byte version %v", got)
	}
}

// versionFixture is a key-sorted run of rows exercising every field: keys
// repeated across versions and delete markers in S3's interleaving,
// version→delete marker→version transitions, literal "null" IDs, both
// booleans, absent fields and repeated payloads.
func versionFixture() []VersionRow {
	const md5 = "3fcf97288388d24c5f5b1152108e1a10"
	obj := func(size, mtime int64, etag string) Object {
		return Object{Size: size, Mtime: mtime, ETag: etag, StorageClass: "STANDARD", ChecksumAlgorithm: "CRC64NVME", ChecksumType: "FULL_OBJECT", OwnerID: "own"}
	}
	dm := func(mtime int64, id string, latest bool) Version {
		return Version{Object: Object{Mtime: mtime, OwnerID: "own"}, VersionID: id, IsLatest: latest, DeleteMarker: true}
	}
	return []VersionRow{
		{Key: "a", Data: Version{Object: obj(19, 1_741_813_833_000_000, md5), VersionID: "NnTR3l2x4B8WsgyIl7zsK9df2TnS2sGw", IsLatest: true}},
		{Key: "a", Data: dm(1_741_813_833_000_000, "7gOvyuxqXdtVQj6S2EXn8gP5J34SNgz2", false)},
		{Key: "a", Data: Version{Object: obj(19, 1_741_384_624_000_000, md5), VersionID: "ouocMcVTLAKEOG5oI9Pp9w0xqKqicM6D"}},
		{Key: "a", Data: Version{Object: obj(0, 1_741_384_624_000_000, ""), VersionID: "null"}},
		{Key: "b", Data: dm(5, "null", true)},
		{Key: "b", Data: dm(5, "null", false)},
		{Key: "b", Data: Version{Object: obj(2111616388, 4, md5+"-123"), VersionID: "null"}},
		{Key: "c", Data: Version{Object: Object{Size: 0, Mtime: -1}, VersionID: ""}},
		{Key: "c", Data: dm(0, "", false)},
		{Key: "c:é", Data: Version{Object: obj(1<<62, 1<<62, strings.ToUpper(md5)), VersionID: "x", IsLatest: true}},
		{Key: "c:é", Data: Version{Object: obj(1<<62, 1<<62, strings.ToUpper(md5)), VersionID: "x", IsLatest: true}},
	}
}

// TestVersionCodecRoundTrip: the plain codec (through Detach), the delta
// codec (each row against the previous, the first against a zero
// Version, through an owning decoder) round-trip every row exactly.
func TestVersionCodecRoundTrip(t *testing.T) {
	rows := versionFixture()
	got := VersionsSchema.Detach(rows)
	if !reflect.DeepEqual(got, rows) {
		t.Fatalf("Detach\n got %+v\nwant %+v", got, rows)
	}

	var buf []byte
	var zero Version
	prev := &zero
	for i := range rows {
		buf = versionCodec{}.AppendDelta(buf, prev, &rows[i].Data)
		prev = &rows[i].Data
	}
	src := append([]byte(nil), buf...)
	d := schema.NewOwningDecoder(src, VersionsSchema.RowsBytes(rows))
	out := make([]Version, len(rows))
	prev = &zero
	for i := range out {
		versionCodec{}.DecodeDelta(d, prev, &out[i])
		prev = &out[i]
	}
	for i := range src {
		src[i] = 0 // decoded rows must not view the frame
	}
	if d.Err() != nil || d.Len() != 0 {
		t.Fatalf("delta decode: %v, %d bytes left", d.Err(), d.Len())
	}
	for i := range rows {
		if out[i] != rows[i].Data {
			t.Errorf("row %d: %+v, want %+v", i, out[i], rows[i].Data)
		}
	}
	// A repeated version ID costs a flag: the row is its flags byte and
	// the Object's delta.
	for _, i := range []int{5, 6, len(rows) - 1} {
		p, v := &rows[i-1].Data, &rows[i].Data
		if n, obj := len(versionCodec{}.AppendDelta(nil, p, v)), len(objectCodec{}.AppendDelta(nil, &p.Object, &v.Object)); n != 1+obj {
			t.Errorf("row %d after a repeated version ID takes %d bytes, want %d", i, n, 1+obj)
		}
	}
}

// TestVersionCodecMalformed: an unknown flag bit is a malformed frame, in
// either codec.
func TestVersionCodecMalformed(t *testing.T) {
	v := versionFixture()[0].Data
	plain := versionCodec{}.Append(nil, &v)
	plain[0] |= 1 << 7
	d := schema.NewDecoder(plain)
	versionCodec{}.Decode(d, &Version{})
	if !errors.Is(d.Err(), schema.ErrCodec) {
		t.Errorf("plain: %v", d.Err())
	}
	delta := versionCodec{}.AppendDelta(nil, &Version{}, &v)
	delta[0] |= 1 << 6
	d = schema.NewOwningDecoder(delta, 1<<10)
	versionCodec{}.DecodeDelta(d, &Version{}, &Version{})
	if !errors.Is(d.Err(), schema.ErrCodec) {
		t.Errorf("delta: %v", d.Err())
	}
	// The plain codec has no repeat flag.
	plain = versionCodec{}.Append(nil, &v)
	plain[0] |= versionSameID
	d = schema.NewDecoder(plain)
	versionCodec{}.Decode(d, &Version{})
	if !errors.Is(d.Err(), schema.ErrCodec) {
		t.Errorf("plain with a repeat flag: %v", d.Err())
	}
}

// TestVersionSizes: Heap covers every string (the version ID included),
// Bytes adds the fixed columns and depends on the row alone.
func TestVersionSizes(t *testing.T) {
	for _, r := range versionFixture() {
		v := r.Data
		strs := len(v.ETag) + len(v.StorageClass) + len(v.OwnerID) + len(v.OwnerDisplayName) + len(v.ChecksumAlgorithm) + len(v.ChecksumType) + len(v.VersionID)
		if h := VersionsSchema.Heap(&v); h != int64(strs) {
			t.Errorf("%+v: heap %d, want %d", v, h, strs)
		}
		if b := VersionsSchema.Bytes(&v); b != 18+int64(strs) {
			t.Errorf("%+v: bytes %d", v, b)
		}
	}
}

// TestVersionsToken: a token carries the markers exactly, for any key
// bytes (colons, digits, NUL, invalid UTF-8), with or without a version
// ID; anything versionsToken did not write is refused.
func TestVersionsToken(t *testing.T) {
	for _, key := range []string{"a", "12:34", ":", "k\x00v", "é/😀 +%2B", "\xff\xfe", strings.Repeat("x", 1024), "v3:abc"} {
		for _, id := range []string{"", "null", "7gOvyuxqXdtVQj6S2EXn8gP5J34SNgz2", "12:x"} {
			tok := versionsToken(key, id)
			k, v, err := parseVersionsToken(tok)
			if err != nil || k != key || v != id {
				t.Errorf("(%q, %q) → %q → (%q, %q, %v)", key, id, tok, k, v, err)
			}
		}
	}
	for _, tok := range []string{"", "k", "x abc", "v", "v3:ab", "v3:abc", "v-1:x", "v01:ax", "v+1:ax", "vx:a", "v1a", "v0:v"} {
		if k, v, err := parseVersionsToken(tok); !errors.Is(err, errToken) {
			t.Errorf("%q accepted as (%q, %q)", tok, k, v)
		}
	}
}

// TestVersionsInput: the chain's first request starts after After with
// the key marker alone; later requests send the markers the token carries,
// version-id-marker only when the page returned one.
func TestVersionsInput(t *testing.T) {
	q := &keyrange.Query{Prefix: "p/", Delimiter: "/", After: "p/a", Max: 1000}
	in, err := versionsInput("b", q)
	if err != nil || *in != (client.VersionsInput{Bucket: "b", Prefix: "p/", Delimiter: "/", KeyMarker: "p/a", MaxKeys: 1000}) {
		t.Fatalf("%+v %v", in, err)
	}
	q.Token = versionsToken("p/k", "null")
	if in, err = versionsInput("b", q); err != nil || in.KeyMarker != "p/k" || in.VersionIDMarker != "null" || in.Prefix != "p/" {
		t.Fatalf("%+v %v", in, err)
	}
	q.Token = versionsToken("p/z/", "")
	if in, err = versionsInput("b", q); err != nil || in.KeyMarker != "p/z/" || in.VersionIDMarker != "" {
		t.Fatalf("%+v %v", in, err)
	}
	q.Token = "garbage"
	if _, err = versionsInput("b", q); !errors.Is(err, errToken) {
		t.Fatal(err)
	}
}

// chainServer answers each request with the capture its markers name
// (client/testdata/versions) and fails on any other request.
func chainServer(t *testing.T, pages map[string]string) *client.Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		k := q.Get("prefix") + "|" + q.Get("delimiter") + "|" + q.Get("key-marker") + "|" + q.Get("version-id-marker")
		if _, ok := q["version-id-marker"]; ok && q.Get("version-id-marker") == "" {
			t.Errorf("an empty version-id-marker: %s", r.URL.RawQuery)
		}
		name, ok := pages[k]
		if !ok {
			t.Errorf("unexpected request %s (%q)", r.URL.RawQuery, k)
			w.WriteHeader(400)
			return
		}
		b, err := os.ReadFile(filepath.Join("client", "testdata", "versions", name+".xml"))
		if err != nil {
			t.Error(err)
		}
		w.Write(b)
	}))
	t.Cleanup(srv.Close)
	c, err := client.New(context.Background(), client.Config{Endpoint: srv.URL, PathStyle: true, Anonymous: true, Region: "us-east-1"})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// fetchVersions is the seam the provider's fetch composes: query →
// request → native page.
func fetchVersions(t *testing.T, c *client.Client, q *keyrange.Query, owner bool) *keyrange.Page[Version] {
	t.Helper()
	in, err := versionsInput("bkt", q)
	if err != nil {
		t.Fatal(err)
	}
	out, err := c.ListObjectVersions(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	pg, err := versionsPage(out, owner)
	if err != nil {
		t.Fatal(err)
	}
	return pg
}

// TestVersionsChain replays live chains: each page's token yields exactly
// the request that produced the next capture. A page that ends inside a
// key's versions continues with both markers (mixed-max2 → mixed-next);
// a page of common prefixes, which has no NextVersionIdMarker, with the
// key marker alone (prefix-only → prefix-next).
func TestVersionsChain(t *testing.T) {
	c := chainServer(t, map[string]string{
		"|||":                                 "mixed-max2",
		"|||7gOvyuxqXdtVQj6S2EXn8gP5J34SNgz2": "never", // a version marker without a key marker
		"||.bidsignore|7gOvyuxqXdtVQj6S2EXn8gP5J34SNgz2": "mixed-next",
		"|/|Enigma-JHU.csv|":                             "prefix-only",
		"|/|ai/|":                                        "prefix-next",
	})
	q := &keyrange.Query{Max: 2}
	pg := fetchVersions(t, c, q, true)
	if len(pg.Rows) != 2 || !pg.Truncated || pg.Rows[0].Key != ".bidsignore" || pg.Rows[1].Key != ".bidsignore" ||
		pg.Rows[0].Data.DeleteMarker || !pg.Rows[1].Data.DeleteMarker || pg.Rows[0].Data.ETag != "3fcf97288388d24c5f5b1152108e1a10" ||
		pg.Rows[1].Data.OwnerID == "" || pg.Token != versionsToken(".bidsignore", "7gOvyuxqXdtVQj6S2EXn8gP5J34SNgz2") {
		t.Fatalf("%+v", pg)
	}
	q.Token = pg.Token
	pg = fetchVersions(t, c, q, false)
	if len(pg.Rows) != 2 || pg.Rows[0].Key != ".bidsignore" || pg.Rows[1].Key != ".datalad/.gitattributes" || pg.Rows[0].Data.OwnerID != "" ||
		pg.Token != versionsToken(".datalad/.gitattributes", "snuFyhp6EmoHdYogfAE7NYwV1e63yn0I") {
		t.Fatalf("%+v", pg)
	}

	q = &keyrange.Query{Delimiter: "/", After: "Enigma-JHU.csv", Max: 1, Probe: true}
	pg = fetchVersions(t, c, q, false)
	if len(pg.Rows) != 0 || !reflect.DeepEqual(pg.Prefixes, []string{"ai/"}) || pg.Token != versionsToken("ai/", "") {
		t.Fatalf("%+v", pg)
	}
	q.Token = pg.Token
	pg = fetchVersions(t, c, q, false)
	if !reflect.DeepEqual(pg.Prefixes, []string{"alleninstitute/"}) || pg.Token != versionsToken("alleninstitute/", "") {
		t.Fatalf("%+v", pg)
	}
}

// TestVersionsPageRows: every entry is a row in wire order, delete
// markers included (probe pages too: the mapping has no probe mode);
// "null" IDs stay; delete markers carry no object fields; a page that
// is not truncated has no token.
func TestVersionsPageRows(t *testing.T) {
	c := chainServer(t, map[string]string{
		"||CAZy.dmnd|null": "null-pair",
		"ai/|/||":          "deleted-folder",
	})
	pg := fetchVersions(t, c, &keyrange.Query{Token: versionsToken("CAZy.dmnd", "null"), Max: 3, Probe: true}, false)
	var got []string
	for _, r := range pg.Rows {
		kind := "V"
		if r.Data.DeleteMarker {
			kind = "D"
		}
		got = append(got, kind+" "+r.Key+" "+r.Data.VersionID)
	}
	want := []string{"D CAZyDB.07242025.fa 97idrPrUxbCwA86JJUGKtjKULWbHN5R2", "V CAZyDB.07242025.fa null", "D PUL.dmnd tWxCjy1SKzEMFbIgm03zVapXGzhNC2wQ"}
	if !reflect.DeepEqual(got, want) || pg.Token != versionsToken("PUL.dmnd", "tWxCjy1SKzEMFbIgm03zVapXGzhNC2wQ") {
		t.Fatalf("%q %q", got, pg.Token)
	}
	dm, v := pg.Rows[0].Data, pg.Rows[1].Data
	if dm != (Version{Object: Object{Mtime: dm.Mtime}, VersionID: "97idrPrUxbCwA86JJUGKtjKULWbHN5R2", IsLatest: true, DeleteMarker: true}) ||
		v.Size != 2111616388 || v.ETag != "8484e791892a7e004d66b50b3dc10716-123" || v.StorageClass != "INTELLIGENT_TIERING" || v.IsLatest {
		t.Fatalf("%+v\n%+v", dm, v)
	}

	pg = fetchVersions(t, c, &keyrange.Query{Prefix: "ai/", Delimiter: "/", Max: 3}, false)
	if len(pg.Rows) != 2 || !pg.Rows[0].Data.DeleteMarker || pg.Rows[1].Data.DeleteMarker || pg.Truncated || pg.Token != "" {
		t.Fatalf("%+v", pg)
	}
}

// TestVersionsPageInvalid: a truncated page without a NextKeyMarker is
// invalid; no marker is made up from the last row.
func TestVersionsPageInvalid(t *testing.T) {
	out := &client.VersionPage{IsTruncated: true, NextVersionIDMarker: "v",
		Versions: []client.Version{{Object: client.Object{Key: "k"}, VersionID: "v"}}}
	if _, err := versionsPage(out, false); !errors.Is(err, client.ErrInvalidPage) {
		t.Fatal(err)
	}
}
