// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"errors"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/source/objhttp"
)

// sameAsOld checks body against the scanner as it was (xml_old_test.go),
// as a ListBucketResult and as a ListVersionsResult: the same page, or the
// same error.
func sameAsOld(t *testing.T, body []byte) {
	t.Helper()
	got, gerr := parseList(body)
	want, werr := oldParseList(body)
	same(t, "list", body, got, want, gerr, werr)
	gotV, gerr := parseVersions(body)
	wantV, werr := oldParseVersions(body)
	same(t, "versions", body, gotV, wantV, gerr, werr)
}

func same[T any](t *testing.T, kind string, body []byte, got, want *T, gerr, werr error) {
	t.Helper()
	switch {
	case (gerr == nil) != (werr == nil):
		t.Fatalf("%s: error %v, was %v\n%q", kind, gerr, werr, body)
	case gerr != nil:
		if gerr.Error() != werr.Error() || errors.Is(gerr, objhttp.ErrSyntax) != errors.Is(werr, objhttp.ErrSyntax) {
			t.Fatalf("%s: error %v, was %v\n%q", kind, gerr, werr, body)
		}
	case !reflect.DeepEqual(got, want):
		t.Fatalf("%s: pages differ\n got %+v\nwant %+v\n%q", kind, got, want, body)
	}
}

// diffSeeds is every fixture the package has: the live versions captures,
// the checked-in fuzz corpus, the samples of the other tests and the
// benchmark pages.
func diffSeeds(t testing.TB) [][]byte {
	seeds := [][]byte{[]byte(sample), []byte(okPageEntities), benchBody(false), benchBody(true), shapedBody(false), shapedBody(true)}
	for _, dir := range []string{"testdata/versions", "testdata/fuzz/FuzzParseList"} {
		names, err := filepath.Glob(filepath.Join(dir, "*"))
		if err != nil {
			t.Fatal(err)
		}
		for _, n := range names {
			if strings.HasSuffix(n, "PROVENANCE") {
				continue
			}
			b, err := os.ReadFile(n)
			if err != nil {
				t.Fatal(err)
			}
			seeds = append(seeds, b)
		}
	}
	wrap := func(s string) []byte { return []byte("<ListBucketResult>" + s + "</ListBucketResult>") }
	for _, s := range []string{
		`<Contents><Key>a&amp;b&lt;&gt;&apos;&#65;&#x42;</Key></Contents>`,
		`<Contents><Key><![CDATA[<x>&amp;]]>tail</Key></Contents>`,
		`<Contents><Key>a<!-- c -->b<?pi x?>c</Key></Contents>`,
		`<Contents><Key>a<child>skipped</child>b</Key></Contents>`,
		"<Contents><Key>a\r\nb\rc</Key></Contents>",
		`<Contents><Key>first</Key><Key>last</Key><Size> 12 </Size></Contents>`,
		`<Contents/><Contents></Contents><CommonPrefixes/>`,
		`<s3:Contents><s3:Key>ns</s3:Key></s3:Contents>`,
		`<Contents a="1" b='>'><Key >k</Key ></Contents>`,
		`<IsTruncated>1</IsTruncated><IsTruncated> TRUE </IsTruncated><KeyCount></KeyCount>`,
		`<EncodingType>url</EncodingType><Contents><Key>%41+</Key></Contents>`,
		`<Contents><Key>%41+</Key></Contents><EncodingType>url</EncodingType>`,
		`<Contents><Owner><ID>a</ID></Owner><Owner><DisplayName>b</DisplayName></Owner></Contents>`,
		`<Contents><Key>]]></Key></Contents>`,
		`<Contents><Key>a]]b]]</Key><ETag>&#x22;e&#34;</ETag></Contents>`,
	} {
		seeds = append(seeds, wrap(s))
	}
	return seeds
}

// TestScannerMatchesOld runs every fixture, every prefix of the short
// ones and a set of generated documents through both scanners.
func TestScannerMatchesOld(t *testing.T) {
	for _, s := range diffSeeds(t) {
		sameAsOld(t, s)
		if len(s) < 4096 {
			for i := range s {
				sameAsOld(t, s[:i])
			}
		}
	}
	r := rand.New(rand.NewPCG(1, 2))
	n := 50000
	if testing.Short() {
		n = 5000
	}
	for range n {
		doc := genDoc(r)
		sameAsOld(t, doc)
		sameAsOld(t, mutate(r, doc))
	}
}

// genDoc builds a listing document from the elements S3 sends and the
// XML forms the scanner must handle: entities, character references,
// CDATA, comments, processing instructions, \r\n, namespace prefixes,
// attributes, empty elements, child elements of scalars, unknown
// elements, URL escapes good and bad, odd whitespace and mismatched or
// missing end tags.
func genDoc(r *rand.Rand) []byte {
	pick := func(s ...string) string { return s[r.IntN(len(s))] }
	var b strings.Builder
	if r.IntN(3) > 0 {
		b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + pick("\n", "", "\r\n", " "))
	}
	if r.IntN(10) == 0 {
		b.WriteString(pick(`<!DOCTYPE x [<!ENTITY y 'z'>]>`, `<!-- lead -->`, `<!x <!-- > --> >`))
	}
	ns := pick("", "", "", "s3:")
	root := pick("ListBucketResult", "ListVersionsResult", "ListBucketResult", "ListVersionsResult", "Error")
	text := func() string {
		var t strings.Builder
		for range r.IntN(4) {
			t.WriteString(pick("abc", "%41", "a+b", "%2F", "%C3%A9", "%4", "%zz", "&amp;", "&quot;", "&lt;", "&#65;", "&#x42;",
				"&#xD800;", "&bogus;", "&", "&#;", "<![CDATA[c<d>&amp;]]>", "<!-- c -->", "<?pi x?>", "\r\n", "\r", " ",
				"\t", "]]>", "]]", "12", "-3", "true", "false", " 1 ", "TRUE", "url", "2023-01-02T03:04:05.000Z",
				"2006-01-02T15:04:05+07:00", "2024-02-30T00:00:00Z", "<x>child</x>", "<y/>", "é", "null", "99999999999999999999"))
		}
		return t.String()
	}
	var el func(name string, depth int)
	el = func(name string, depth int) {
		name = ns + name
		open := "<" + name + pick("", "", "", " ", ` a="1"`, ` b='>'`, "\n") + ">"
		switch r.IntN(12) {
		case 0:
			b.WriteString("<" + name + "/>")
			return
		case 1:
			b.WriteString("<" + name + " />")
			return
		}
		b.WriteString(open)
		if depth > 0 {
			for range r.IntN(9) {
				el(pick("Key", "ETag", "Size", "LastModified", "StorageClass", "ChecksumAlgorithm", "ChecksumType",
					"VersionId", "IsLatest", "Owner", "ID", "DisplayName", "RestoreStatus", "Prefix", "Other"), depth-1)
				if r.IntN(8) == 0 {
					b.WriteString(pick(" ", "\n", "<!-- c -->", "<?p?>", "text"))
				}
			}
		} else {
			b.WriteString(text())
		}
		end := name
		if r.IntN(60) == 0 {
			end = pick("Wrong", name+"x", "")
		}
		b.WriteString("</" + end + pick(">", ">", ">", " >"))
	}
	b.WriteString("<" + ns + root + ` xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
	for range r.IntN(12) {
		name := pick("Contents", "Contents", "Version", "DeleteMarker", "CommonPrefixes", "IsTruncated", "EncodingType",
			"NextContinuationToken", "KeyCount", "NextKeyMarker", "NextVersionIdMarker", "Prefix", "KeyMarker", "Name", "MaxKeys")
		depth := 0
		switch name {
		case "Contents", "Version", "DeleteMarker", "CommonPrefixes":
			depth = 2
		}
		el(name, depth)
	}
	if r.IntN(20) > 0 {
		b.WriteString("</" + ns + root + ">")
	}
	return []byte(b.String())
}

// mutate truncates, deletes or inserts a byte, or swaps two halves.
func mutate(r *rand.Rand, doc []byte) []byte {
	if len(doc) == 0 {
		return doc
	}
	d := append([]byte(nil), doc...)
	i := r.IntN(len(d))
	switch r.IntN(4) {
	case 0:
		return d[:i]
	case 1:
		return append(d[:i], d[i+1:]...)
	case 2:
		c := "<>/&;!?[]\"'= \r\n-%x"[r.IntN(18)]
		return append(d[:i], append([]byte{c}, d[i:]...)...)
	}
	return append(d[i:], d[:i]...)
}

// FuzzScannerMatchesOld holds the scanner to the old one on arbitrary
// documents.
func FuzzScannerMatchesOld(f *testing.F) {
	for _, s := range diffSeeds(f) {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		sameAsOld(t, body)
	})
}
