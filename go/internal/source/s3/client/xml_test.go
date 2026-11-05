// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"encoding/xml"
	"errors"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"
)

// refTime records LastModified as encoding/xml would hand it to a time.Time.
type refTime struct{ us int64 }

func (r *refTime) UnmarshalText(b []byte) error {
	var t time.Time
	if err := t.UnmarshalText(b); err != nil {
		return err
	}
	r.us = t.UnixMicro()
	return nil
}

type refList struct {
	XMLName               xml.Name `xml:"ListBucketResult"`
	IsTruncated           bool
	NextContinuationToken *string
	KeyCount              int
	EncodingType          string
	Contents              []struct {
		Key               string
		ETag              string
		Size              int64
		LastModified      refTime
		StorageClass      string
		ChecksumAlgorithm []string
		ChecksumType      string
		Owner             struct{ ID, DisplayName string }
	}
	CommonPrefixes []struct{ Prefix string }
}

// reference parses with encoding/xml: the oracle for the scanner.
func reference(body []byte) (*Page, error) {
	var r refList
	if err := xml.Unmarshal(body, &r); err != nil {
		return nil, err
	}
	dec := func(s string) (string, error) {
		if r.EncodingType != "url" {
			return s, nil
		}
		return url.QueryUnescape(s)
	}
	pg := &Page{IsTruncated: r.IsTruncated, KeyCount: r.KeyCount, EncodingType: r.EncodingType}
	if r.NextContinuationToken != nil {
		pg.NextContinuationToken, pg.HasContinuation = *r.NextContinuationToken, true
	}
	pg.Contents = make([]Object, len(r.Contents))
	for i, c := range r.Contents {
		k, err := dec(c.Key)
		if err != nil {
			return nil, err
		}
		o := Object{Key: k, ETag: c.ETag, Size: c.Size, Mtime: c.LastModified.us, StorageClass: c.StorageClass,
			ChecksumType: c.ChecksumType, OwnerID: c.Owner.ID, OwnerDisplayName: c.Owner.DisplayName}
		if len(c.ChecksumAlgorithm) > 0 {
			o.ChecksumAlgorithm = c.ChecksumAlgorithm[0]
		}
		pg.Contents[i] = o
	}
	for _, p := range r.CommonPrefixes {
		s, err := dec(p.Prefix)
		if err != nil {
			return nil, err
		}
		pg.CommonPrefixes = append(pg.CommonPrefixes, s)
	}
	return pg, nil
}

func parse(body []byte) (*Page, error) { return new(parser).parseList(body) }

// agree checks the property the scanner promises: whenever encoding/xml
// accepts the document, the scanner returns the same page.
func agree(t *testing.T, body []byte) {
	t.Helper()
	want, werr := reference(body)
	got, gerr := parse(body)
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

const sample = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>b</Name><Prefix></Prefix>` +
	`<NextContinuationToken>1ueGcxLPRx1Tr/XYExHnhbYLgveDs2J/wm36Hy4vbOwM=</NextContinuationToken>` +
	`<KeyCount>3</KeyCount><MaxKeys>1000</MaxKeys><EncodingType>url</EncodingType><IsTruncated>true</IsTruncated>` +
	`<Contents><Key>a%2Fb+c%25d</Key><LastModified>2009-10-12T17:50:30.000Z</LastModified>` +
	`<ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag><ChecksumAlgorithm>CRC32</ChecksumAlgorithm>` +
	`<ChecksumAlgorithm>SHA256</ChecksumAlgorithm><ChecksumType>FULL_OBJECT</ChecksumType><Size>434234</Size>` +
	`<Owner><ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID><DisplayName>me</DisplayName></Owner>` +
	`<StorageClass>STANDARD</StorageClass><RestoreStatus><IsRestoreInProgress>false</IsRestoreInProgress></RestoreStatus></Contents>` +
	`<Contents><Key>%E2%82%AC</Key><LastModified>2024-02-29T23:59:59.123456Z</LastModified><ETag>"x-2"</ETag>` +
	`<Size>0</Size><Owner><ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID></Owner>` +
	`<StorageClass>STANDARD</StorageClass></Contents>` +
	`<CommonPrefixes><Prefix>photos%2F</Prefix></CommonPrefixes></ListBucketResult>`

func TestParseSample(t *testing.T) {
	pg, err := parse([]byte(sample))
	if err != nil {
		t.Fatal(err)
	}
	if len(pg.Contents) != 2 || pg.Contents[0].Key != "a/b c%d" || pg.Contents[1].Key != "€" ||
		pg.Contents[0].ETag != `"fba9dede5f27731c9771645a39863328"` || pg.Contents[0].ChecksumAlgorithm != "CRC32" ||
		pg.Contents[0].Size != 434234 || pg.Contents[0].OwnerDisplayName != "me" ||
		pg.Contents[0].Mtime != time.Date(2009, 10, 12, 17, 50, 30, 0, time.UTC).UnixMicro() ||
		pg.Contents[1].Mtime != time.Date(2024, 2, 29, 23, 59, 59, 123456000, time.UTC).UnixMicro() ||
		!pg.IsTruncated || !pg.HasContinuation || pg.KeyCount != 3 || len(pg.CommonPrefixes) != 1 || pg.CommonPrefixes[0] != "photos/" {
		t.Fatalf("%+v", pg)
	}
	agree(t, []byte(sample))
}

// TestParseShapes covers the XML features encoding/xml accepts.
func TestParseShapes(t *testing.T) {
	wrap := func(s string) string { return "<ListBucketResult>" + s + "</ListBucketResult>" }
	for _, s := range []string{
		wrap(`<Contents><Key>a&amp;b&lt;&gt;&apos;&#65;&#x42;</Key></Contents>`),
		wrap(`<Contents><Key><![CDATA[<x>&amp;]]>tail</Key></Contents>`),
		wrap(`<Contents><Key>a<!-- c -->b<?pi x?>c</Key></Contents>`),
		wrap(`<Contents><Key>a<child>skipped</child>b</Key></Contents>`),
		wrap("<Contents><Key>a\r\nb\rc</Key></Contents>"),
		wrap(`<Contents><Key>first</Key><Key>last</Key><Size> 12 </Size></Contents>`),
		wrap(`<Contents/><Contents></Contents><CommonPrefixes/>`),
		wrap(`<s3:Contents><s3:Key>ns</s3:Key></s3:Contents>`),
		wrap(`<Contents a="1" b='>'><Key >k</Key ></Contents>`),
		wrap(`<Contents><LastModified>2006-01-02T15:04:05+07:00</LastModified><LastModified>2006-01-02T15:04:05.1Z</LastModified></Contents>`),
		wrap(`<IsTruncated>1</IsTruncated><IsTruncated> TRUE </IsTruncated><KeyCount></KeyCount>`),
		wrap(`<EncodingType>url</EncodingType><Contents><Key>%41+</Key></Contents>`),
		wrap(`<Contents><Key>%41+</Key></Contents><EncodingType>url</EncodingType>`),
		"junk<!DOCTYPE x [<!ENTITY y 'z'>]>" + wrap(`<NextContinuationToken/>`),
		wrap(`<Contents><Owner><ID>a</ID></Owner><Owner><DisplayName>b</DisplayName></Owner></Contents>`),
	} {
		agree(t, []byte(s))
		if _, err := parse([]byte(s)); err != nil {
			t.Errorf("%q: %v", s, err)
		}
	}
}

func TestParseRejects(t *testing.T) {
	for _, s := range []string{
		``, `<Error><Code>x</Code></Error>`, `<ListBucketResult><Key>x</ListBucketResult>`,
		`<ListBucketResult><EncodingType>url</EncodingType><Contents><Key>%4</Key></Contents></ListBucketResult>`,
		`<ListBucketResult><Contents><Key>&bogus;</Key></Contents></ListBucketResult>`,
		`<ListBucketResult><Contents><Size>x</Size></Contents></ListBucketResult>`,
		`<ListBucketResult><Contents>`,
	} {
		if _, err := parse([]byte(s)); !errors.Is(err, ErrSyntax) {
			t.Errorf("%q: %v, want a syntax error", s, err)
		}
	}
}

func TestParseError(t *testing.T) {
	code, msg, id, ok := parseError([]byte(`<?xml version="1.0"?><Error><Code>SlowDown</Code><Message>Please &amp; reduce</Message><RequestId>R1</RequestId><HostId>h</HostId></Error>`))
	if !ok || code != "SlowDown" || msg != "Please & reduce" || id != "R1" {
		t.Fatal(code, msg, id, ok)
	}
	if _, _, _, ok := parseError([]byte("<html>busy</html>")); ok {
		t.Fatal("html accepted as an S3 error")
	}
}

// TestPageStringsShareArena pins the allocation model: one arena per page,
// repeated values shared.
func TestPageStringsShareArena(t *testing.T) {
	pg, err := parse([]byte(sample))
	if err != nil {
		t.Fatal(err)
	}
	a, b := pg.Contents[0], pg.Contents[1]
	if !sameData(a.StorageClass, b.StorageClass) || !sameData(a.OwnerID, b.OwnerID) {
		t.Fatal("repeated values are not shared")
	}
}

func sameData(a, b string) bool {
	return len(a) == len(b) && len(a) > 0 && unsafeData(a) == unsafeData(b)
}

// FuzzParseList holds the scanner to encoding/xml on arbitrary documents.
func FuzzParseList(f *testing.F) {
	f.Add([]byte(sample))
	f.Add([]byte(`<ListBucketResult><Contents><Key>a&#x20;<![CDATA[b]]></Key></Contents></ListBucketResult>`))
	f.Add([]byte("<ListBucketResult><EncodingType>url</EncodingType><CommonPrefixes><Prefix>a%2F+</Prefix></CommonPrefixes></ListBucketResult>"))
	f.Fuzz(func(t *testing.T, body []byte) {
		agree(t, body)
	})
}

// FuzzParseFields marshals fuzzed field values with encoding/xml and
// URL-encodes keys as S3 does, so every input is a valid document.
func FuzzParseFields(f *testing.F) {
	f.Add("a/b c", "\"etag\"", "STANDARD", "id", int64(5), int64(1700000000), true, "tok", "p/")
	f.Add("ü%+&<>\r\n", "", "", "", int64(-1), int64(0), false, "", "")
	f.Fuzz(func(t *testing.T, key, etag, class, owner string, size, sec int64, truncated bool, token, prefix string) {
		type obj struct {
			Key          string
			LastModified string
			ETag         string
			Size         int64
			StorageClass string
			Owner        struct{ ID string }
		}
		type doc struct {
			XMLName               xml.Name `xml:"ListBucketResult"`
			EncodingType          string
			IsTruncated           bool
			NextContinuationToken string
			Contents              []obj
			CommonPrefixes        []struct{ Prefix string }
		}
		ts := time.Unix(sec%(400*365*86400), 0).UTC().Format("2006-01-02T15:04:05.000Z")
		d := doc{EncodingType: "url", IsTruncated: truncated, NextContinuationToken: token}
		for _, k := range []string{key, key + "x"} {
			o := obj{Key: url.QueryEscape(k), LastModified: ts, ETag: etag, Size: size, StorageClass: class}
			o.Owner.ID = owner
			d.Contents = append(d.Contents, o)
		}
		d.CommonPrefixes = append(d.CommonPrefixes, struct{ Prefix string }{url.QueryEscape(prefix)})
		body, err := xml.Marshal(d)
		if err != nil {
			t.Skip()
		}
		agree(t, body)
		if _, err := reference(body); err == nil {
			pg, _ := parse(body)
			if !strings.EqualFold(pg.EncodingType, "url") {
				t.Fatal("encoding type lost")
			}
		}
	})
}

func unsafeData(s string) *byte { return unsafe.StringData(s) }
