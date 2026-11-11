// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"errors"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
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
	versions, err := filepath.Glob(filepath.Join("testdata", "versions", "*.xml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range versions {
		b, err := os.ReadFile(n)
		if err != nil {
			t.Fatal(err)
		}
		seeds = append(seeds, b)
	}
	corpus, err := filepath.Glob(filepath.Join("testdata", "fuzz", "FuzzParseList", "*"))
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range corpus {
		b, err := os.ReadFile(n)
		if err != nil {
			t.Fatal(err)
		}
		seeds = append(seeds, corpusBytes(t, n, b))
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

// corpusBytes decodes a saved input of a fuzz target that takes one
// []byte: the file is "go test fuzz v1" and one []byte("...") line.
func corpusBytes(t testing.TB, name string, file []byte) []byte {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(string(file)), "\n")
	if len(lines) != 2 || lines[0] != "go test fuzz v1" || !strings.HasPrefix(lines[1], "[]byte(") || !strings.HasSuffix(lines[1], ")") {
		t.Fatalf("%s: not a fuzz input of one []byte", name)
	}
	s, err := strconv.Unquote(strings.TrimSuffix(strings.TrimPrefix(lines[1], "[]byte("), ")"))
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return []byte(s)
}

// TestCorpusSeedDecoded: the differential test reads a saved fuzz input as
// the document it holds, not its file.
func TestCorpusSeedDecoded(t *testing.T) {
	name := filepath.Join("testdata", "fuzz", "FuzzParseList", "9dd9741b722c207c")
	file, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	got := corpusBytes(t, name, file)
	if want := `<ListBucketResult><Contents><Key>a&#x20;<!"000000000></Key></Contents></ListBucketResult>`; string(got) != want {
		t.Fatalf("decoded %q, want %q", got, want)
	}
	pg, err := parseList(got)
	if err != nil || len(pg.Contents) != 1 || pg.Contents[0].Key != "a " {
		t.Fatalf("%+v, %v; want one key %q", pg, err, "a ")
	}
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
		doc, _ := genDoc(r)
		sameAsOld(t, doc)
		sameAsOld(t, mutate(r, doc))
	}
}

// The classes of text genDoc writes into the fields the scanner reads.
// The URL classes count only in fields that are URL-decoded (keys,
// prefixes, key markers) of a document that says EncodingType url.
const (
	clEntity    = "entity"        // &amp; &quot; &lt; &gt; &apos;
	clCharRef   = "charref"       // &#65; &#x42; &#xE9;
	clPercent   = "percent"       // valid %XX escapes, under url
	clPlus      = "plus"          // '+' under url
	clMalformed = "malformed-url" // a bad escape after a valid one, under url
	clCDATA     = "cdata"
	clUTF8      = "utf8"
	clMarkup    = "comment-pi" // comments and processing instructions inside text
	clCRLF      = "crlf"
	clChild     = "child" // a child element inside a scalar
	clBadText   = "bad-text"
)

var genClasses = []string{clEntity, clCharRef, clPercent, clPlus, clMalformed, clCDATA, clUTF8, clMarkup, clCRLF, clChild, clBadText}

// docInfo is what genDoc wrote into fields the scanner reads (elements it
// skips do not count): the operation the root names and the text classes.
type docInfo struct {
	versions bool // the root is ListVersionsResult; else ListBucketResult (or neither)
	listing  bool // the root is one of the two
	url      bool // an <EncodingType>url</EncodingType> among the root's children
	classes  map[string]bool
}

// docGen builds one document.
type docGen struct {
	r    *rand.Rand
	b    strings.Builder
	ns   string
	info docInfo
	// keyed is the URL-decoded fields' text classes, merged into
	// info.classes once the whole document says whether it is url.
	keyed map[string]bool
}

func (g *docGen) pick(s ...string) string { return s[g.r.IntN(len(s))] }

func (g *docGen) one(n int) bool { return g.r.IntN(n) == 0 }

// open writes a start tag, sometimes with attributes or space; selfClosed
// reports an empty element <x/> already written whole.
func (g *docGen) open(name string, emptyOdds int) (selfClosed bool) {
	if g.one(emptyOdds) {
		g.b.WriteString("<" + g.ns + name + g.pick("/>", " />"))
		return true
	}
	g.b.WriteString("<" + g.ns + name + g.pick("", "", "", "", " ", ` a="1"`, ` b='>'`, "\n") + ">")
	return false
}

// close writes the end tag, now and then mismatched.
func (g *docGen) close(name string) {
	if g.one(300) {
		name = g.pick("Wrong", name+"x", "")
	} else {
		name = g.ns + name
	}
	g.b.WriteString("</" + name + g.pick(">", ">", ">", " >") + "")
}

// text writes the character data of a scalar of the given kind and
// returns the classes it used.
func (g *docGen) text(kind string) map[string]bool {
	used := map[string]bool{}
	piece := func() {
		if g.one(40) { // the rare broken or odd forms
			c := g.pick(clChild, clBadText, clMarkup, clCRLF)
			used[c] = true
			switch c {
			case clChild:
				g.b.WriteString(g.pick("<x>child</x>", "<y/>", "<Key>inner</Key>"))
			case clBadText:
				g.b.WriteString(g.pick("&bogus;", "&", "&#;", "&#xD800;", "]]>", "&#99999999;"))
			case clMarkup:
				g.b.WriteString(g.pick("<!-- c -->", "<?pi x?>"))
			case clCRLF:
				g.b.WriteString(g.pick("\r\n", "\r", "\n"))
			}
			return
		}
		switch kind {
		case "key":
			switch c := g.pick(clEntity, clCharRef, clPercent, clPercent, clPlus, clCDATA, clUTF8, "plain", "plain", "plain", clMalformed); c {
			case clEntity:
				g.b.WriteString(g.pick("&amp;", "&quot;", "&lt;", "&gt;", "&apos;"))
				used[c] = true
			case clCharRef:
				g.b.WriteString(g.pick("&#65;", "&#x42;", "&#xE9;"))
				used[c] = true
			case clPercent:
				g.b.WriteString(g.pick("%41", "%2F", "%3D", "%C3%A9", "%7E", "%7e"))
				used[c] = true
			case clPlus:
				g.b.WriteString("+")
				used[c] = true
			case clCDATA:
				g.b.WriteString(g.pick("<![CDATA[<c>&amp;]]>", "<![CDATA[%41]]>", "<![CDATA[]]>"))
				used[c] = true
			case clUTF8:
				g.b.WriteString(g.pick("é", "€", "\xff"))
				used[c] = true
			case clMalformed:
				if g.one(2) {
					g.b.WriteString("%41") // a valid escape always comes first
					used[clPercent] = true
					g.b.WriteString(g.pick("%4", "%zz", "%", "%G1"))
					used[c] = true
				} else {
					g.b.WriteString("v1.0/x")
				}
			default:
				g.b.WriteString(g.pick("v1.0/btc", "date=2023-01-01", "a", " ", "-_.*"))
			}
		case "etag":
			switch c := g.pick(clEntity, clEntity, clCharRef, "plain"); c {
			case clEntity, clCharRef:
				g.b.WriteString(g.pick("&quot;", "&#34;", "&#x22;"))
				used[c] = true
			default:
				g.b.WriteString(g.pick("0123456789abcdef", "-12", "\""))
			}
		case "int": // one piece in fifty does not parse
			if g.one(50) {
				g.b.WriteString(g.pick("99999999999999999999", "x", "1 2"))
			} else {
				g.b.WriteString(g.pick("12", " 12 ", "0", "-3", "1&#48;", "<![CDATA[7]]>"))
			}
		case "time":
			if g.one(50) {
				g.b.WriteString(g.pick("2024-02-30T00:00:00Z", "x", "2023-01-02T03:04:05.000"))
			} else {
				g.b.WriteString(g.pick("2023-01-02T03:04:05.000Z", "2006-01-02T15:04:05+07:00", "2024-02-29T23:59:59.123456Z"))
			}
		case "bool":
			if g.one(50) {
				g.b.WriteString(g.pick("x", "yes"))
			} else {
				g.b.WriteString(g.pick("true", "false", " 1 ", "TRUE", "0"))
			}
		default: // other strings: classes, owners, tokens, version IDs
			switch c := g.pick(clEntity, clUTF8, clCDATA, "plain", "plain", "plain"); c {
			case clEntity:
				g.b.WriteString(g.pick("&amp;", "&lt;"))
				used[c] = true
			case clUTF8:
				g.b.WriteString("é")
				used[c] = true
			case clCDATA:
				g.b.WriteString("<![CDATA[x<y]]>")
				used[c] = true
			default:
				g.b.WriteString(g.pick("STANDARD", "null", "75aa57f0", "1ueGcx/=", "CRC64NVME"))
			}
		}
	}
	n := 1 // a number, time or boolean is one value
	switch kind {
	case "key", "etag", "string":
		n += g.r.IntN(4)
	}
	for range n {
		piece()
	}
	return used
}

// scalar writes one scalar element; recognized says the scanner reads
// it (its text classes count), keyed that it is URL-decoded.
func (g *docGen) scalar(name, kind string, recognized, keyed bool) {
	if g.open(name, 25) {
		return
	}
	used := g.text(kind)
	if recognized {
		for c := range used {
			switch {
			case c == clPercent || c == clPlus || c == clMalformed:
				if keyed {
					g.keyed[c] = true
				}
			default:
				g.info.classes[c] = true
			}
		}
	}
	g.close(name)
}

// entry writes a Contents, Version or DeleteMarker element with its
// fields as direct children: scalars, an Owner and a RestoreStatus with
// their own children, an unknown element.
func (g *docGen) entry(name string, recognized bool) {
	if g.open(name, 30) {
		return
	}
	versionField := recognized && g.info.versions
	for range g.r.IntN(10) {
		switch f := g.pick("Key", "Key", "ETag", "Size", "LastModified", "StorageClass", "ChecksumAlgorithm", "ChecksumType",
			"VersionId", "IsLatest", "Owner", "RestoreStatus", "Other"); f {
		case "Key":
			g.scalar(f, "key", recognized, true)
		case "ETag":
			g.scalar(f, "etag", recognized, false)
		case "Size":
			g.scalar(f, "int", recognized, false)
		case "LastModified":
			g.scalar(f, "time", recognized, false)
		case "IsLatest":
			g.scalar(f, "bool", versionField, false)
		case "VersionId":
			g.scalar(f, "string", versionField, false)
		case "Owner":
			if !g.open(f, 20) {
				for range g.r.IntN(3) {
					o := g.pick("ID", "DisplayName", "Other")
					g.scalar(o, "string", recognized && o != "Other", false)
				}
				g.close(f)
			}
		case "RestoreStatus":
			if !g.open(f, 10) {
				g.scalar("IsRestoreInProgress", "bool", false, false)
				g.close(f)
			}
		case "Other":
			g.scalar(f, "key", false, false)
		default:
			g.scalar(f, "string", recognized, false)
		}
		if g.one(10) {
			g.b.WriteString(g.pick(" ", "\n", "<!-- c -->", "<?p?>", "text"))
		}
	}
	g.close(name)
}

// genDoc builds a listing document by element role: an entry's fields are
// its direct children, a scalar's text is where the adversarial forms go
// (entities, character references, CDATA, comments, \r\n, UTF-8, URL
// escapes good and bad), with now and then a child element inside a
// scalar, empty elements, attributes, namespace prefixes, unknown
// elements and mismatched or missing end tags. It returns what it put in
// the fields the scanner reads.
func genDoc(r *rand.Rand) ([]byte, docInfo) {
	g := &docGen{r: r, info: docInfo{classes: map[string]bool{}}, keyed: map[string]bool{}}
	if g.one(3) {
		g.b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + g.pick("\n", "", "\r\n", " "))
	}
	if g.one(20) {
		g.b.WriteString(g.pick(`<!DOCTYPE x [<!ENTITY y 'z'>]>`, `<!-- lead -->`, `<!x <!-- > --> >`))
	}
	g.ns = g.pick("", "", "", "", "", "s3:")
	root := g.pick("ListBucketResult", "ListVersionsResult", "ListBucketResult", "ListVersionsResult",
		"ListBucketResult", "ListVersionsResult", "ListBucketResult", "ListVersionsResult", "Error")
	g.info.versions = root == "ListVersionsResult"
	g.info.listing = root != "Error"
	entryNames := []string{"Contents"}
	if g.info.versions {
		entryNames = []string{"Version", "DeleteMarker"}
	}
	g.b.WriteString("<" + g.ns + root + ` xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
	for range g.r.IntN(14) {
		in := g.info.listing
		switch c := g.pick("entry", "entry", "entry", "entry", "foreign-entry", "CommonPrefixes", "EncodingType",
			"IsTruncated", "NextContinuationToken", "KeyCount", "NextKeyMarker", "NextVersionIdMarker", "Prefix",
			"KeyMarker", "Name", "MaxKeys"); c {
		case "entry":
			g.entry(entryNames[g.r.IntN(len(entryNames))], in)
		case "foreign-entry": // the other operation's entry: skipped
			if g.info.versions {
				g.entry("Contents", false)
			} else {
				g.entry(g.pick("Version", "DeleteMarker"), false)
			}
		case "CommonPrefixes":
			if !g.open(c, 20) {
				for range g.r.IntN(3) {
					g.scalar("Prefix", "key", in, true)
				}
				g.close(c)
			}
		case "EncodingType":
			if g.one(10) {
				g.scalar(c, "string", false, false) // anything but url: keys are verbatim
			} else {
				g.b.WriteString("<" + g.ns + c + ">url</" + g.ns + c + ">")
				g.info.url = g.info.url || in
			}
		case "IsTruncated":
			g.scalar(c, "bool", in, false)
		case "KeyCount":
			g.scalar(c, "int", in && !g.info.versions, false)
		case "NextContinuationToken":
			g.scalar(c, "string", in && !g.info.versions, false)
		case "NextKeyMarker", "Prefix", "KeyMarker":
			g.scalar(c, "key", in && g.info.versions, true)
		case "NextVersionIdMarker":
			g.scalar(c, "string", in && g.info.versions, false)
		default:
			g.scalar(c, "string", false, false)
		}
	}
	if !g.one(30) {
		g.b.WriteString("</" + g.ns + root + ">")
	}
	if g.info.url {
		for c := range g.keyed {
			g.info.classes[c] = true
		}
	}
	return []byte(g.b.String()), g.info
}

// TestGeneratedCoverage holds genDoc to its purpose: over the stream
// TestScannerMatchesOld draws, in each operation, every class of text
// reaches fields the scanner reads, in documents the old scanner accepts
// (a malformed escape: refused as one), and decoded values reach pages.
func TestGeneratedCoverage(t *testing.T) {
	type count struct{ docs, accepted, refusedEscape int }
	counts := map[string]map[string]*count{"list": {}, "versions": {}}
	values := map[string]map[string]int{"list": {}, "versions": {}}
	r := rand.New(rand.NewPCG(1, 2))
	for range 50000 {
		doc, info := genDoc(r)
		mutate(r, doc) // keep the stream TestScannerMatchesOld draws
		if !info.listing {
			continue
		}
		mode := "list"
		var keys []string
		var err error
		if info.versions {
			mode = "versions"
			var pg *VersionPage
			if pg, err = oldParseVersions(doc); err == nil {
				for _, v := range pg.Versions {
					keys = append(keys, v.Key, v.ETag)
				}
				keys = append(keys, pg.NextKeyMarker)
				keys = append(keys, pg.CommonPrefixes...)
			}
		} else {
			var pg *Page
			if pg, err = oldParseList(doc); err == nil {
				for _, o := range pg.Contents {
					keys = append(keys, o.Key, o.ETag)
				}
				keys = append(keys, pg.CommonPrefixes...)
			}
		}
		for c := range info.classes {
			n := counts[mode][c]
			if n == nil {
				n = new(count)
				counts[mode][c] = n
			}
			n.docs++
			if err == nil {
				n.accepted++
			} else if strings.Contains(err.Error(), "invalid URL escape") {
				n.refusedEscape++
			}
		}
		for _, k := range keys {
			for name, sub := range map[string]string{"&": "&", "<": "<", `"`: `"`, "é": "é", "~ (from %7E)": "~", "A": "A", "space": " "} {
				if strings.Contains(k, sub) {
					values[mode][name]++
				}
			}
		}
	}
	for _, mode := range []string{"list", "versions"} {
		for _, c := range genClasses {
			n := counts[mode][c]
			if n == nil {
				n = new(count)
			}
			t.Logf("%-8s %-13s docs %6d  accepted %6d  refused as a bad escape %5d", mode, c, n.docs, n.accepted, n.refusedEscape)
			switch c {
			case clMalformed:
				if n.refusedEscape < 100 {
					t.Errorf("%s: %s refused as a bad escape in %d documents, want at least 100", mode, c, n.refusedEscape)
				}
			case clChild, clBadText, clMarkup, clCRLF:
				if n.docs < 100 {
					t.Errorf("%s: %s in %d documents, want at least 100", mode, c, n.docs)
				}
			default:
				if n.accepted < 100 {
					t.Errorf("%s: %s in %d accepted documents, want at least 100", mode, c, n.accepted)
				}
			}
		}
		t.Logf("%-8s decoded values holding: %v", mode, values[mode])
		for name, n := range values[mode] {
			if n < 100 {
				t.Errorf("%s: %d page values hold %s, want at least 100", mode, n, name)
			}
		}
		if len(values[mode]) < 7 {
			t.Errorf("%s: page values hold only %v", mode, values[mode])
		}
	}
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
// documents, seeded with the fixtures and 200 generated documents.
func FuzzScannerMatchesOld(f *testing.F) {
	for _, s := range diffSeeds(f) {
		f.Add(s)
	}
	r := rand.New(rand.NewPCG(5, 6))
	for range 200 {
		doc, _ := genDoc(r)
		f.Add(doc)
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		sameAsOld(t, body)
	})
}
