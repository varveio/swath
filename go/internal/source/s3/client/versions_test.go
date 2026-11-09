// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"encoding/xml"
	"errors"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/varveio/swath/go/internal/source/objhttp"
)

// refEntry is a <Version> or <DeleteMarker> as encoding/xml decodes it.
type refEntry struct {
	Key               string
	VersionId         string
	IsLatest          bool
	ETag              string
	Size              int64
	LastModified      refTime
	StorageClass      string
	ChecksumAlgorithm []string
	ChecksumType      string
	Owner             struct{ ID, DisplayName string }
}

// refVersions is the oracle for parseVersions: encoding/xml, walking the
// root's children in document order so that the interleaving of
// <Version> and <DeleteMarker> is the document's. The echoed Prefix and
// KeyMarker are not on the page but are decoded, as the scanner does, so
// a malformed escape in them is refused by both.
type refVersions struct {
	pg                          VersionPage
	prefix, keyMarker, encoding string
}

func (r *refVersions) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	if start.Name.Local != "ListVersionsResult" {
		return errors.New("not a ListVersionsResult")
	}
	pg := &r.pg
	for {
		tok, err := d.Token()
		if err != nil {
			return err
		}
		var el xml.StartElement
		switch t := tok.(type) {
		case xml.EndElement:
			return nil
		case xml.StartElement:
			el = t
		default:
			continue
		}
		var s *string
		switch el.Name.Local {
		case "Version", "DeleteMarker":
			var e refEntry
			if err := d.DecodeElement(&e, &el); err != nil {
				return err
			}
			v := Version{Object: Object{Key: e.Key, ETag: e.ETag, Size: e.Size, Mtime: e.LastModified.us, StorageClass: e.StorageClass,
				ChecksumType: e.ChecksumType, OwnerID: e.Owner.ID, OwnerDisplayName: e.Owner.DisplayName},
				VersionID: e.VersionId, IsLatest: e.IsLatest, DeleteMarker: el.Name.Local == "DeleteMarker"}
			if len(e.ChecksumAlgorithm) > 0 {
				v.ChecksumAlgorithm = e.ChecksumAlgorithm[0]
			}
			pg.Versions = append(pg.Versions, v)
			continue
		case "CommonPrefixes":
			var c struct{ Prefix string }
			if err := d.DecodeElement(&c, &el); err != nil {
				return err
			}
			pg.CommonPrefixes = append(pg.CommonPrefixes, c.Prefix)
			continue
		case "IsTruncated":
			if err := d.DecodeElement(&pg.IsTruncated, &el); err != nil {
				return err
			}
			continue
		case "NextKeyMarker":
			s = &pg.NextKeyMarker
		case "NextVersionIdMarker":
			s = &pg.NextVersionIDMarker
		case "Prefix":
			s = &r.prefix
		case "KeyMarker":
			s = &r.keyMarker
		case "EncodingType":
			s = &r.encoding
		default:
			if err := d.Skip(); err != nil {
				return err
			}
			continue
		}
		if err := d.DecodeElement(s, &el); err != nil {
			return err
		}
	}
}

// referenceVersions parses with encoding/xml and decodes as S3's
// encoding-type=url asks (url.QueryUnescape: form semantics, once).
func referenceVersions(body []byte) (*VersionPage, error) {
	var r refVersions
	if err := xml.Unmarshal(body, &r); err != nil {
		return nil, err
	}
	pg := &r.pg
	if pg.Versions == nil {
		pg.Versions = []Version{}
	}
	if r.encoding == "url" {
		var err error
		dec := func(s *string) {
			if err == nil {
				*s, err = url.QueryUnescape(*s)
			}
		}
		for i := range pg.Versions {
			dec(&pg.Versions[i].Key)
		}
		for i := range pg.CommonPrefixes {
			dec(&pg.CommonPrefixes[i])
		}
		dec(&pg.NextKeyMarker)
		dec(&r.prefix)
		dec(&r.keyMarker)
		if err != nil {
			return nil, err
		}
	}
	if pg.IsTruncated && pg.NextKeyMarker == "" {
		return nil, objhttp.ErrInvalidPage
	}
	return pg, nil
}

// agreeVersions: whenever encoding/xml accepts a versions document, the
// scanner returns the same page.
func agreeVersions(t *testing.T, body []byte) {
	t.Helper()
	want, werr := referenceVersions(body)
	got, gerr := parseVersions(body)
	if werr != nil {
		return
	}
	if gerr != nil {
		t.Fatalf("scanner rejects what encoding/xml accepts: %v\n%q", gerr, body)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("pages differ\n got %+v\nwant %+v\n%q", got, want, body)
	}
}

// capture reads a live ListObjectVersions response (testdata/versions,
// whose PROVENANCE names each request).
func capture(t testing.TB, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "versions", name+".xml"))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// kinds spells a page's entries in wire order: V a version, D a delete
// marker.
func kinds(pg *VersionPage) string {
	var b strings.Builder
	for _, v := range pg.Versions {
		if v.DeleteMarker {
			b.WriteByte('D')
		} else {
			b.WriteByte('V')
		}
	}
	return b.String()
}

// TestParseVersionsCaptures: the live captures parse as encoding/xml reads
// them, with Version and DeleteMarker interleaved in wire order, keys and
// markers decoded once, version IDs verbatim ("null" included) and delete
// markers without size, ETag, storage class or checksums.
func TestParseVersionsCaptures(t *testing.T) {
	const owner = "0000000000000000000000000000000000000000000000000000000000000000"
	for _, c := range []struct {
		name, kinds, nextKey, nextVID string
		truncated                     bool
		prefixes                      []string
		check                         func(t *testing.T, pg *VersionPage)
	}{
		{name: "mixed-max2", kinds: "VD", truncated: true, nextKey: ".bidsignore", nextVID: "7gOvyuxqXdtVQj6S2EXn8gP5J34SNgz2",
			check: func(t *testing.T, pg *VersionPage) {
				v, dm := pg.Versions[0], pg.Versions[1]
				if v.Key != ".bidsignore" || v.VersionID != "NnTR3l2x4B8WsgyIl7zsK9df2TnS2sGw" || !v.IsLatest || v.Size != 19 ||
					v.ETag != `"3fcf97288388d24c5f5b1152108e1a10"` || v.StorageClass != "STANDARD" || v.ChecksumAlgorithm != "CRC64NVME" ||
					v.ChecksumType != "FULL_OBJECT" || v.OwnerID != owner {
					t.Errorf("version %+v", v)
				}
				want := Version{Object: Object{Key: ".bidsignore", Mtime: v.Mtime, OwnerID: owner}, VersionID: "7gOvyuxqXdtVQj6S2EXn8gP5J34SNgz2", DeleteMarker: true}
				if dm != want {
					t.Errorf("delete marker %+v, want %+v", dm, want)
				}
			}},
		{name: "mixed-next", kinds: "VV", truncated: true, nextKey: ".datalad/.gitattributes", nextVID: "snuFyhp6EmoHdYogfAE7NYwV1e63yn0I"},
		{name: "dm-pair", kinds: "VVV", truncated: true, nextKey: ".datalad/config", nextVID: "ZsoLm4RUX5A6q0dD6yPCR3NYLZyVvqkP"},
		{name: "null-pair", kinds: "DVD", truncated: true, nextKey: "PUL.dmnd", nextVID: "tWxCjy1SKzEMFbIgm03zVapXGzhNC2wQ",
			check: func(t *testing.T, pg *VersionPage) {
				v := pg.Versions[1]
				if v.Key != "CAZyDB.07242025.fa" || v.VersionID != "null" || v.IsLatest ||
					v.Size != 2111616388 || v.ETag != `"8484e791892a7e004d66b50b3dc10716-123"` || v.StorageClass != "INTELLIGENT_TIERING" {
					t.Errorf("%+v", pg)
				}
				if dm := pg.Versions[0]; !dm.IsLatest || dm.Size != 0 || dm.ETag != "" || dm.Key != v.Key {
					t.Errorf("delete marker %+v", dm)
				}
			}},
		{name: "dm-null-pair", kinds: "DDD", truncated: true,
			nextKey: "/FTP/caldb/data/ixpe/gpd/bcf/asiccorr/ixpe_d2_20170101_asiccorr_02.fits", nextVID: "null",
			check: func(t *testing.T, pg *VersionPage) {
				for _, v := range pg.Versions {
					if v.VersionID != "null" {
						t.Errorf("%+v", v)
					}
				}
				if !sameData(pg.Versions[0].VersionID, pg.Versions[2].VersionID) {
					t.Error("repeated version IDs are not shared")
				}
			}},
		{name: "key-only", kinds: "VVV", truncated: true, nextKey: "README.txt", nextVID: "AZDsczM4minY49.zYpYsNi3ojEOh6FAW"},
		{name: "encoded-marker", kinds: "D", truncated: true,
			nextKey: "des/public/hats/des_dr2/des_dr2/dataset/Norder=4/Dir=0/Npix=1086.parquet", nextVID: "5HPknu9BraWqdVrirGAQ6gUIF1WUanMa",
			check: func(t *testing.T, pg *VersionPage) {
				if pg.Versions[0].Key != pg.NextKeyMarker {
					t.Errorf("%+v", pg)
				}
			}},
		{name: "special-echo"},
		{name: "prefix-only", truncated: true, nextKey: "ai/", prefixes: []string{"ai/"}},
		{name: "prefix-next", truncated: true, nextKey: "alleninstitute/", prefixes: []string{"alleninstitute/"}},
		{name: "deleted-folder-parent", truncated: true, nextKey: "archived/", prefixes: []string{"ai/", "alleninstitute/", "archived/"}},
		{name: "deleted-folder", kinds: "DV", check: func(t *testing.T, pg *VersionPage) {
			if dm, v := pg.Versions[0], pg.Versions[1]; dm.Key != "ai/" || dm.OwnerID != "" || v.Key != "ai/" || v.ETag != `"d41d8cd98f00b204e9800998ecf8427e"` || v.Size != 0 {
				t.Errorf("%+v", pg)
			}
		}},
		{name: "ties", kinds: "VDVDVDVDVDVDVDVDV", check: func(t *testing.T, pg *VersionPage) {
			// Equal-second ties keep the wire order: entry 4 and 5 share a
			// LastModified, as do 11 and 12.
			ids := []string{"W7XhFybg6tj2sEWlqcifTfYfJeQYp0D9", "lSCkBQYXlDASpeGIqQM4c2spFI6qvPFP", "Ugj5GljpJXM0paEI9BLYbZ4HyqsT0Hkn",
				"R9SMpBZ0ce.PDMyA2JdlYw6I8Ek3JjQA", "uEsCRmoxWxF4onDcA.uIA_gsklYhSlq2", "z.PrIpsRvrQaU00Nedk4tZYLgbqEgdOI"}
			for i, id := range ids {
				if pg.Versions[i].VersionID != id || pg.Versions[i].Key != ".gitattributes" {
					t.Errorf("entry %d: %+v, want version %s", i, pg.Versions[i], id)
				}
			}
			if pg.Versions[4].Mtime != pg.Versions[5].Mtime {
				t.Error("fixture: entries 4 and 5 should tie")
			}
		}},
		{name: "mixed-null-search", kinds: "DVVVVVVVVVVVVVDVV"},
		{name: "zero-max"},
		{name: "past-end"},
	} {
		t.Run(c.name, func(t *testing.T) {
			body := capture(t, c.name)
			pg, err := parseVersions(body)
			if err != nil {
				t.Fatal(err)
			}
			if got := kinds(pg); got != c.kinds || pg.IsTruncated != c.truncated || pg.NextKeyMarker != c.nextKey ||
				pg.NextVersionIDMarker != c.nextVID || !reflect.DeepEqual(pg.CommonPrefixes, c.prefixes) {
				t.Fatalf("kinds %q truncated %v next (%q, %q) prefixes %q", got, pg.IsTruncated, pg.NextKeyMarker, pg.NextVersionIDMarker, pg.CommonPrefixes)
			}
			for _, v := range pg.Versions {
				if v.DeleteMarker && (v.Size != 0 || v.ETag != "" || v.StorageClass != "" || v.ChecksumAlgorithm != "" || v.ChecksumType != "") {
					t.Errorf("delete marker with object fields: %+v", v)
				}
				if !v.DeleteMarker && v.ETag == "" {
					t.Errorf("version without an ETag: %+v", v)
				}
			}
			if c.check != nil {
				c.check(t, pg)
			}
			agreeVersions(t, body)
		})
	}
}

// TestParseVersionsEncoding: keys, common prefixes and NextKeyMarker are
// decoded once with form semantics (plus is a space); version IDs and the
// NextVersionIdMarker are not; XML escapes are undone before URL decoding.
func TestParseVersionsEncoding(t *testing.T) {
	doc := `<ListVersionsResult><EncodingType>url</EncodingType><IsTruncated>true</IsTruncated>` +
		`<Prefix>p+q%2B</Prefix><KeyMarker>a%2525</KeyMarker>` +
		`<NextKeyMarker>%C3%A9+x%2B%25%3C%26%3E</NextKeyMarker><NextVersionIdMarker>v+1%2B</NextVersionIdMarker>` +
		`<Version><Key>sp+ace</Key><VersionId>v+1%2B</VersionId><Size>1</Size></Version>` +
		`<DeleteMarker><Key>pl%2Bus</Key><VersionId>null</VersionId></DeleteMarker>` +
		`<Version><Key>pct%25+%2525</Key><VersionId>a</VersionId><Size>2</Size></Version>` +
		`<Version><Key>%E2%82%AC%F0%9F%98%80</Key><VersionId>b</VersionId><Size>3</Size></Version>` +
		`<DeleteMarker><Key>%3C%26%3E%22%27%0A</Key><VersionId>c</VersionId></DeleteMarker>` +
		`<Version><Key>&lt;raw&amp;&gt;</Key><VersionId>d</VersionId><Size>4</Size></Version>` +
		`<DeleteMarker><Key>%C3%A9+x%2B%25%3C%26%3E</Key><VersionId>v+1%2B</VersionId></DeleteMarker>` +
		`<CommonPrefixes><Prefix>dir+one%2F</Prefix></CommonPrefixes></ListVersionsResult>`
	pg, err := parseVersions([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, v := range pg.Versions {
		keys = append(keys, v.Key)
	}
	wantKeys := []string{"sp ace", "pl+us", "pct% %25", "€😀", "<&>\"'\n", "<raw&>", "é x+%<&>"}
	if !reflect.DeepEqual(keys, wantKeys) {
		t.Errorf("keys %q, want %q", keys, wantKeys)
	}
	if pg.NextKeyMarker != "é x+%<&>" || pg.NextVersionIDMarker != "v+1%2B" || pg.Versions[0].VersionID != "v+1%2B" ||
		!reflect.DeepEqual(pg.CommonPrefixes, []string{"dir one/"}) || kinds(pg) != "VDVVDVD" {
		t.Errorf("%+v", pg)
	}
	agreeVersions(t, []byte(doc))
	// Without encoding-type=url nothing is decoded.
	plain := strings.Replace(doc, "<EncodingType>url</EncodingType>", "", 1)
	if pg, err := parseVersions([]byte(plain)); err != nil || pg.Versions[0].Key != "sp+ace" || pg.NextKeyMarker != "%C3%A9+x%2B%25%3C%26%3E" {
		t.Fatalf("%v %+v", err, pg)
	}
	agreeVersions(t, []byte(plain))
}

// TestParseVersionsInvalidPage: a truncated page must say where it
// continues (NextKeyMarker); a page that ends at a common prefix may omit
// the NextVersionIdMarker.
func TestParseVersionsInvalidPage(t *testing.T) {
	for _, doc := range []string{
		`<ListVersionsResult><IsTruncated>true</IsTruncated><Version><Key>k</Key><VersionId>v</VersionId></Version></ListVersionsResult>`,
		`<ListVersionsResult><IsTruncated>true</IsTruncated><NextKeyMarker></NextKeyMarker><NextVersionIdMarker>v</NextVersionIdMarker></ListVersionsResult>`,
		`<ListVersionsResult><IsTruncated>true</IsTruncated><CommonPrefixes><Prefix>a/</Prefix></CommonPrefixes></ListVersionsResult>`,
	} {
		_, err := parseVersions([]byte(doc))
		if !errors.Is(err, objhttp.ErrInvalidPage) || errors.Is(err, objhttp.ErrSyntax) || objhttp.Classify(err) != objhttp.ClassOther {
			t.Errorf("%q: %v, want an invalid page", doc, err)
		}
	}
	for _, doc := range []string{
		`<ListVersionsResult><IsTruncated>true</IsTruncated><NextKeyMarker>a/</NextKeyMarker><CommonPrefixes><Prefix>a/</Prefix></CommonPrefixes></ListVersionsResult>`,
		`<ListVersionsResult><IsTruncated>false</IsTruncated><Version><Key>k</Key></Version></ListVersionsResult>`,
	} {
		if _, err := parseVersions([]byte(doc)); err != nil {
			t.Errorf("%q: %v", doc, err)
		}
	}
}

func TestParseVersionsRejects(t *testing.T) {
	for _, s := range []string{
		`<ListVersionsResult><EncodingType>url</EncodingType><NextKeyMarker>%4</NextKeyMarker></ListVersionsResult>`,
		`<ListVersionsResult><EncodingType>url</EncodingType><KeyMarker>%zz</KeyMarker></ListVersionsResult>`,
		`<ListVersionsResult><Version><IsLatest>maybe</IsLatest></Version></ListVersionsResult>`,
		`<ListVersionsResult><DeleteMarker><Key>k</Key></Version></ListVersionsResult>`,
		`<ListVersionsResult><Version>`,
	} {
		if _, err := parseVersions([]byte(s)); !errors.Is(err, objhttp.ErrSyntax) {
			t.Errorf("%q: %v, want a syntax error", s, err)
		}
	}
}

// TestParseListIgnoresVersionFields: ListObjectsV2 parsing skips the
// versions-only elements as encoding/xml does, whatever they hold.
func TestParseListIgnoresVersionFields(t *testing.T) {
	doc := `<ListBucketResult><Contents><Key>k</Key><IsLatest>maybe</IsLatest><VersionId>v</VersionId></Contents></ListBucketResult>`
	if pg, err := parse([]byte(doc)); err != nil || pg.Contents[0].Key != "k" {
		t.Fatalf("%v %+v", err, pg)
	}
	agree(t, []byte(doc))
}

// TestEveryVersionsTruncationFails: no proper prefix of a versions page
// parses.
func TestEveryVersionsTruncationFails(t *testing.T) {
	doc := string(capture(t, "null-pair"))
	for i := 0; i < len(doc); i++ {
		if pg, err := parseVersions([]byte(doc[:i])); err == nil {
			t.Fatalf("prefix %d of %d parsed: %+v", i, len(doc), pg)
		}
	}
}

// FuzzParseVersions holds the versions scanner to encoding/xml.
func FuzzParseVersions(f *testing.F) {
	for _, name := range []string{"mixed-max2", "null-pair", "prefix-only", "special-echo", "deleted-folder"} {
		f.Add(capture(f, name))
	}
	f.Add([]byte(`<ListVersionsResult><EncodingType>url</EncodingType><Version><Key>a+%2B</Key><Size></Size><IsLatest> 1 </IsLatest></Version></ListVersionsResult>`))
	f.Fuzz(func(t *testing.T, body []byte) {
		agreeVersions(t, body)
	})
}

// TestListObjectVersionsRequest: the request carries versions,
// encoding-type=url and its markers, read back exactly by the server;
// max-keys is left to the server default unless set; version-id-marker is
// refused without a key marker.
func TestListObjectVersionsRequest(t *testing.T) {
	var raw atomic.Value
	page := capture(t, "null-pair")
	c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		raw.Store(r.URL.RawQuery)
		w.Write(page)
	}, Config{Anonymous: true})
	const key = "a b+c%/é&=?<>"
	for _, c2 := range []struct {
		in   VersionsInput
		want url.Values
	}{
		{VersionsInput{Bucket: "b"}, url.Values{"versions": {""}, "encoding-type": {"url"}}},
		{VersionsInput{Bucket: "b", Prefix: "p/" + key, Delimiter: "/", KeyMarker: key, VersionIDMarker: "null", MaxKeys: 7},
			url.Values{"versions": {""}, "encoding-type": {"url"}, "prefix": {"p/" + key}, "delimiter": {"/"},
				"key-marker": {key}, "version-id-marker": {"null"}, "max-keys": {"7"}}},
	} {
		if _, err := c.ListObjectVersions(context.Background(), &c2.in); err != nil {
			t.Fatal(err)
		}
		q := raw.Load().(string)
		if got, err := url.ParseQuery(q); err != nil || !reflect.DeepEqual(got, c2.want) {
			t.Errorf("%+v: query %s, want %v", c2.in, q, c2.want)
		}
	}
	if _, err := c.ListObjectVersions(context.Background(), &VersionsInput{Bucket: "b", VersionIDMarker: "v"}); err == nil {
		t.Fatal("a version-id marker without a key marker was sent")
	}
}

// TestListObjectVersionsFailures: a 200 that is not a ListVersionsResult
// is retried like any parse failure; a truncated page without a
// NextKeyMarker is an invalid page, not retried; S3's answers are
// classified as for ListObjectsV2.
func TestListObjectVersionsFailures(t *testing.T) {
	var n atomic.Int32
	c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) == 1 {
			w.Write(capture(t, "deleted-folder-v2"))
			return
		}
		w.Write(capture(t, "mixed-max2"))
	}, Config{Anonymous: true})
	if _, err := c.ListObjectVersions(context.Background(), &VersionsInput{Bucket: "b"}); err != nil {
		t.Fatal(err)
	}
	if got := c.Counters(); got.Attempts != 2 || got.Retries != 1 || got.ParseFailures != 1 {
		t.Fatalf("%+v", got)
	}

	n.Store(0)
	c, _ = testClient(t, func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		w.Write([]byte(`<ListVersionsResult><IsTruncated>true</IsTruncated><Version><Key>k</Key></Version></ListVersionsResult>`))
	}, Config{Anonymous: true})
	_, err := c.ListObjectVersions(context.Background(), &VersionsInput{Bucket: "b"})
	if !errors.Is(err, objhttp.ErrInvalidPage) || n.Load() != 1 || c.Counters().ParseFailures != 0 {
		t.Fatalf("%v after %d attempts (%+v)", err, n.Load(), c.Counters())
	}

	c, _ = testClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(403)
		w.Write([]byte(`<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>`))
	}, Config{Anonymous: true})
	_, err = c.ListObjectVersions(context.Background(), &VersionsInput{Bucket: "b"})
	var se *Error
	if !errors.As(err, &se) || se.Code != "AccessDenied" || objhttp.Classify(err) != objhttp.ClassAccessDenied {
		t.Fatal(err)
	}
}

func TestParseVersionsMissingVersionMarker(t *testing.T) {
	for _, kind := range []string{"Version", "DeleteMarker"} {
		body := "<ListVersionsResult><IsTruncated>true</IsTruncated><NextKeyMarker>k</NextKeyMarker><" + kind + "><Key>k</Key><VersionId>null</VersionId></" + kind + "></ListVersionsResult>"
		if _, err := parseVersions([]byte(body)); !errors.Is(err, objhttp.ErrInvalidPage) {
			t.Fatalf("%s: %v", kind, err)
		}
	}
}
