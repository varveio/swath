// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azure

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf16"

	"github.com/varveio/swath/go/internal/replay/store"
	"github.com/varveio/swath/go/internal/replay/store/memstore"
)

func objects(names ...string) []store.Object {
	out := make([]store.Object, len(names))
	for i, n := range names {
		size := int64(i)
		t := time.Unix(1_700_000_000+int64(i), 0).UTC()
		out[i] = store.Object{Key: []byte(n), Size: &size, LastModified: &t}
	}
	slices.SortFunc(out, func(a, b store.Object) int { return bytes.Compare(a.Key, b.Key) })
	return out
}

func newStore(t *testing.T, rollup bool, objs []store.Object) *memstore.Store {
	t.Helper()
	st, err := memstore.New(objs, memstore.Options{Rollup: rollup})
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func version(v string) http.Header { return http.Header{"X-Ms-Version": {v}} }

func parse(t *testing.T, query string) *Request {
	t.Helper()
	q := "restype=container&comp=list"
	if query != "" {
		q += "&" + query
	}
	r, err := Parse("GET", "/replay/c1", q, version(Version20260606), "replay", "c1")
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return r
}

// utf16Less is the reference order: UTF-16 code units.
func utf16Less(a, b string) int {
	return slices.Compare(utf16.Encode([]rune(a)), utf16.Encode([]rune(b)))
}

// expected lists names in provider order the brute-force way: every name
// in the prefix window at or after the lower bound, a delimiter rolling a
// run of names into one BlobPrefix.
func expected(names []string, prefix, delimiter, startFrom string) []string {
	sorted := slices.Clone(names)
	slices.SortFunc(sorted, utf16Less)
	var out []string
	for _, n := range sorted {
		if !strings.HasPrefix(n, prefix) || startFrom != "" && utf16Less(n, startFrom) < 0 {
			continue
		}
		if delimiter != "" {
			if i := strings.Index(n[len(prefix):], delimiter); i >= 0 {
				p := n[:len(prefix)+i+len(delimiter)] + "<prefix>"
				if len(out) > 0 && out[len(out)-1] == p {
					continue
				}
				n = p
			}
		}
		out = append(out, n)
	}
	return out
}

// walk pages the listing to its end with the pager's own markers.
func walk(t *testing.T, p *Pager, r *Request) []string {
	t.Helper()
	var out []string
	for pages := 0; ; pages++ {
		if pages > 10_000 {
			t.Fatal("walk does not end")
		}
		if err := p.ValidateMarker(r); err != nil {
			t.Fatal(err)
		}
		res, err := p.List(context.Background(), r)
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Entries) > r.PageSize {
			t.Fatalf("page of %d over %d", len(res.Entries), r.PageSize)
		}
		for _, e := range res.Entries {
			if e.Object != nil {
				out = append(out, string(e.Object.Key))
			} else {
				out = append(out, string(e.Prefix)+"<prefix>")
			}
		}
		if res.NextMarker == "" {
			return out
		}
		next := *r
		next.Marker, next.MarkerSupplied = res.NextMarker, true
		r = &next
	}
}

// TestProviderOrderMatchesUTF16 walks random names built from the
// characters whose UTF-8 and UTF-16 orders disagree (U+E000..U+FFFF
// against U+10000 and above, at several depths), flat and delimited, from
// random startFrom values and prefixes, at several page sizes, over a store
// that rolls up and one that does not, and requires the brute-force UTF-16
// order every time.
func TestProviderOrderMatchesUTF16(t *testing.T) {
	alphabet := []string{"a", "z", "/", "-", "\u00e9", "\u0800", "\ud7ff", "\ue000", "\uf8ff", "\uffef", "\uffff", "\U00010000", "\U0001F600", "\U0010FFFF"}
	rng := rand.New(rand.NewPCG(7, 11))
	word := func(n int) string {
		var b strings.Builder
		for range 1 + rng.IntN(n) {
			b.WriteString(alphabet[rng.IntN(len(alphabet))])
		}
		return b.String()
	}
	seen := map[string]bool{}
	var names []string
	for len(names) < 400 {
		if w := word(6); !seen[w] {
			seen[w] = true
			names = append(names, w)
		}
	}
	for _, rollup := range []bool{false, true} {
		p := &Pager{Store: newStore(t, rollup, objects(names...)), FixtureIdentity: "f"}
		for trial := range 150 {
			prefix, startFrom, delimiter := "", "", ""
			if trial%3 == 1 {
				prefix = word(2)
			}
			if trial%2 == 0 {
				startFrom = prefix + word(4)
			}
			switch trial % 4 {
			case 1:
				delimiter = "/"
			case 2:
				delimiter = alphabet[rng.IntN(len(alphabet))]
			case 3:
				delimiter = "\ue000\U00010000"
			}
			size := []int{1, 2, 3, 7, 5000}[trial%5]
			r := &Request{Account: "replay", Container: "c1", Version: Version20260606, Prefix: prefix, Delimiter: delimiter,
				StartFrom: startFrom, PageSize: size}
			got := walk(t, p, r)
			want := expected(names, prefix, delimiter, startFrom)
			if !slices.Equal(got, want) {
				t.Fatalf("rollup=%v prefix=%q delimiter=%q startFrom=%q size=%d:\n got %q\nwant %q", rollup, prefix, delimiter, startFrom, size, got, want)
			}
		}
	}
}

// TestStartFromInsideRolledPrefix pins the delimiter rule: a startFrom
// inside a rolled prefix lists that prefix only when one of its names is
// at or after startFrom, then continues after the prefix.
func TestStartFromInsideRolledPrefix(t *testing.T) {
	p := &Pager{Store: newStore(t, true, objects("a/1", "a/2", "b/1", "b/2", "c")), FixtureIdentity: "f"}
	for _, c := range []struct {
		startFrom string
		want      []string
	}{
		{"a/2", []string{"a/<prefix>", "b/<prefix>", "c"}},
		{"a/3", []string{"b/<prefix>", "c"}},
		{"a/", []string{"a/<prefix>", "b/<prefix>", "c"}},
		{"b/1\x00", []string{"b/<prefix>", "c"}},
		{"b/3", []string{"c"}},
	} {
		r := &Request{Account: "replay", Container: "c1", Version: Version20260606, Delimiter: "/", StartFrom: c.startFrom, PageSize: 5000}
		if got := walk(t, p, r); !slices.Equal(got, c.want) {
			t.Errorf("startFrom %q: %q, want %q", c.startFrom, got, c.want)
		}
	}
}

// TestLongStartFromCostBound pins the empty-read settlement: a 4,096-byte
// startFrom of U+10000 covers 2,049 byte ranges, almost all empty, and is
// answered in under ten store reads; the answer is the EE branch, which
// follows U+10000 in UTF-16 order although it precedes it in bytes.
func TestLongStartFromCostBound(t *testing.T) {
	var names []string
	for i := range 2000 {
		names = append(names, fmt.Sprintf("logs/%04d", i))
	}
	names = append(names, "\ue000a", "\ue000b", "\uffffc")
	st := newStore(t, false, objects(names...))
	p := &Pager{Store: st, FixtureIdentity: "f"}
	startFrom := strings.Repeat("\U00010000", 1024)
	if len(startFrom) != 4096 || len(rangesFrom([]byte(startFrom), true, nil)) != 2049 {
		t.Fatal("bound shape")
	}
	r := &Request{Account: "replay", Container: "c1", Version: Version20260606, StartFrom: startFrom, PageSize: 10}
	before := st.Calls()
	res, err := p.List(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, e := range res.Entries {
		got = append(got, string(e.Object.Key))
	}
	if !slices.Equal(got, []string{"\ue000a", "\ue000b", "\uffffc"}) || res.NextMarker != "" {
		t.Fatalf("got %q, marker %q", got, res.NextMarker)
	}
	if calls := st.Calls() - before; calls >= 10 {
		t.Fatalf("%d store reads", calls)
	}
}

// TestMarkerGolden pins the marker bytes: the Java server issues this
// marker for the same request over the synthetic 10k fixture (whose
// identity this is), as the differential driver confirms byte for byte.
func TestMarkerGolden(t *testing.T) {
	r := parse(t, "prefix=logs/&maxresults=2")
	r.Container = "synth"
	bind := binding(r, "54ee4ea602dae0344b56f59b49d0dda6e4a8330627252cdcab6d177c3ce8cd49")
	got := encodeMarker(false, []byte("logs/alpha/dt=2026-01-01/part-00004.json.gz"), bind)
	const want = "az1.AADToCfJ7QNN16QFc8GhMeg_bG9ncy9hbHBoYS9kdD0yMDI2LTAxLTAxL3BhcnQtMDAwMDQuanNvbi5neg"
	if got != want {
		t.Fatalf("%s\nwant %s", got, want)
	}
	m, err := decodeMarker(want, bind)
	if err != nil || m.inclusive || string(m.boundary) != "logs/alpha/dt=2026-01-01/part-00004.json.gz" {
		t.Fatalf("%+v %v", m, err)
	}
}

// TestMarkerDecodeLeniency pins what a marker may look like: padded or
// not, unused trailing bits ignored, CR or LF, a foreign binding, a flag
// other than 0 or 1, an over-long boundary or marker refused.
func TestMarkerDecodeLeniency(t *testing.T) {
	r := parse(t, "")
	bind := binding(r, "f")
	m := encodeMarker(true, []byte("k"), bind) // 19 bytes: one byte of the last unit carries spare bits
	last := m[len(m)-1]
	spare := strings.IndexByte("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_", last) | 0x0f
	flipped := m[:len(m)-1] + string("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"[spare])
	other := encodeMarker(true, []byte("k"), binding(parse(t, "prefix=x"), "f"))
	raw := make([]byte, 2+bindingBytes)
	copy(raw[2:], bind)
	for _, c := range []struct {
		name   string
		marker string
		ok     bool
	}{
		{"plain", m, true}, {"padded", m + "==", true}, {"spare bits", flipped, true},
		{"newline", m[:6] + "\n" + m[6:], false}, {"bad padding", m + "=", false}, {"other scope", other, false},
		{"no prefix", m[4:], false}, {"flag 2", markerPrefix + b64(append([]byte{2, 0}, bind...)), false},
		{"progress 1", markerPrefix + b64(append([]byte{0, 1}, bind...)), true},
		{"empty boundary", markerPrefix + b64(raw), true},
		{"boundary 4097", markerPrefix + b64(append(slices.Clone(raw), bytes.Repeat([]byte("x"), 4097)...)), false},
		{"short", markerPrefix + b64(raw[:10]), false},
		{"too long", m + strings.Repeat("A", maxMarkerChars), false},
	} {
		if _, err := decodeMarker(c.marker, bind); (err == nil) != c.ok {
			t.Errorf("%s: err %v", c.name, err)
		}
	}
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

// TestParseRefusalOrder pins the request checks, their order (the first
// problem is reported) and their codes.
func TestParseRefusalOrder(t *testing.T) {
	h := func(kv ...string) http.Header {
		out := http.Header{}
		for i := 0; i < len(kv); i += 2 {
			out[http.CanonicalHeaderKey(kv[i])] = append(out[http.CanonicalHeaderKey(kv[i])], kv[i+1])
		}
		return out
	}
	v := h("x-ms-version", Version20261006)
	for _, c := range []struct {
		method, path, query string
		header              http.Header
		reason, code        string
		status              int
	}{
		{"GET", "/replay/other", "restype=container&comp=list", v, "container_not_found", "ContainerNotFound", 404},
		{"HEAD", "/replay/c1", "restype=container&comp=list", v, "container_head_unsupported", "UnsupportedQueryParameter", 400},
		{"POST", "/replay/c1", "restype=container&comp=list", nil, "wrong_method", "UnsupportedHttpVerb", 405},
		{"GET", "/replay/c1", "restype=container&comp=list", h("x-ms-version", "x", "x-ms-version", "y"), "duplicate_header", "InvalidHeaderValue", 400},
		{"GET", "/replay/c1", "restype=container&comp=list", nil, "invalid_header_value", "InvalidHeaderValue", 400},
		{"GET", "/replay/c1", "restype=container&comp=list", h("x-ms-version", "2025-01-05"), "invalid_header_value", "InvalidHeaderValue", 400},
		{"GET", "/replay/c1", "include=x", h("x-ms-version", Version20260606, "accept", "application/json"), "unsupported_accept", "InvalidHeaderValue", 400},
		{"GET", "/replay/c1", "include=x", h("x-ms-version", Version20260606, "x-ms-client-request-id", strings.Repeat("c", 1025)), "invalid_client_request_id", "InvalidHeaderValue", 400},
		{"GET", "/replay/c1", "prefix=%ZZ&bogus=1", v, "invalid_percent_escape", "InvalidQueryParameterValue", 400},
		{"GET", "/replay/c1", "prefix=%C3%28", v, "invalid_utf8", "InvalidQueryParameterValue", 400},
		{"GET", "/replay/c1", "bogus=1&prefix=a&prefix=b", v, "unsupported_query_parameter", "UnsupportedQueryParameter", 400},
		{"GET", "/replay/c1", "prefix=a&prefix=a&bogus=1", v, "duplicate_query_parameter", "InvalidQueryParameterValue", 400},
		{"GET", "/replay/c1", "restype=container&maxresults=x", v, "invalid_selector", "InvalidQueryParameterValue", 400},
		{"GET", "/replay/c1", "restype=container&comp=list&include=metadata&timeout=0", v, "unsupported_include", "UnsupportedQueryParameter", 400},
		{"GET", "/replay/c1", "restype=container&comp=list&timeout=3601", v, "invalid_timeout", "InvalidQueryParameterValue", 400},
		{"GET", "/replay/c1", "restype=container&comp=list&prefix=" + strings.Repeat("p", 4097), v, "query_field_too_long", "UnsupportedQueryParameter", 400},
		{"GET", "/replay/c1", "restype=container&comp=list&prefix=a%0D&maxresults=0", v, "xml_query_echo_unmeasured", "UnsupportedQueryParameter", 400},
		{"GET", "/replay/c1", "restype=container&comp=list&maxresults=-1", v, "maxresults_out_of_range", "OutOfRangeQueryParameterValue", 400},
		{"GET", "/replay/c1", "restype=container&comp=list&maxresults=0", v, "maxresults_out_of_range", "OutOfRangeQueryParameterValue", 400},
		{"GET", "/replay/c1", "restype=container&comp=list&maxresults=2147483648", v, "invalid_maxresults", "InvalidQueryParameterValue", 400},
		{"GET", "/replay/c1", "restype=container&comp=list&maxresults=+2", v, "invalid_maxresults", "InvalidQueryParameterValue", 400},
	} {
		_, err := Parse(c.method, c.path, c.query, c.header, "replay", "c1")
		var e *Error
		if !errors.As(err, &e) || e.Reason != c.reason || e.Code() != c.code || e.Status() != c.status {
			t.Errorf("%s %s?%s: %v, want %s %s %d", c.method, c.path, c.query, err, c.reason, c.code, c.status)
		}
	}
	r := parse(t, "maxresults=002&timeout=3600&include=&startFrom=s&marker=&delimiter=")
	if r.PageSize != 2 || r.MaxResults != "002" || !r.MarkerSupplied || r.Marker != "" || !r.DelimiterSupplied || r.PrefixSupplied {
		t.Errorf("%+v", r)
	}
	if r := parse(t, "maxresults=2147483647"); r.PageSize != MaxPageSize {
		t.Errorf("page size %d", r.PageSize)
	}
}

// TestAcceptRule pins the Accept header rule.
func TestAcceptRule(t *testing.T) {
	for _, c := range []struct {
		accept string
		ok     bool
	}{
		{"", true}, {"  ", true}, {"application/xml", true}, {"*/*", true}, {"APPLICATION/XML;Q=1E0", true},
		{"text/plain, application/xml;q=0.5", true}, {"application/xml,", true}, {"application/xml;q=0x1p-1", true},
		{",", false}, {"application/json", false}, {"application/xml;q=0", false}, {"application/xml;q=2", false},
		{"application/xml;q=abc", false}, {"application/xml, application/vnd.apache.arrow.stream;q=0", false},
		{"application/xml;q=NaN", false},
	} {
		if got := acceptsXMLOnly(c.accept); got != c.ok {
			t.Errorf("%q: %v", c.accept, got)
		}
	}
}

// TestRequestLineText pins the request-line decoding: valid UTF-8 passes
// through, each maximal ill-formed subsequence is one U+FFFD.
func TestRequestLineText(t *testing.T) {
	for in, want := range map[string]string{
		"prefix=\u00e9":     "prefix=\u00e9",
		"a\xffb":            "a\ufffdb",
		"\xf0\x9f\x98":      "\ufffd",
		"\xf0\x9f\x98a":     "\ufffda",
		"\xed\xa0\x80":      "\ufffd\ufffd\ufffd",
		"\xc3":              "\ufffd",
		"\xe2\x82\xac\xc0x": "\u20ac\ufffdx",
	} {
		if got := requestLineText(in); got != want {
			t.Errorf("%q: %q, want %q", in, got, want)
		}
	}
}

// TestLatencyKeyGolden pins the jitter key against the Java server's
// values for the same requests.
func TestLatencyKeyGolden(t *testing.T) {
	for _, c := range []struct {
		r    Request
		want int64
	}{
		{Request{PageSize: 5000}, 28634151},
		{Request{Prefix: "logs/", Delimiter: "/", StartFrom: "logs/\u00e9\U0001F600", Marker: "az1.AADToCfJ7QNN16QFc8GhMeg_bG9ncy9h", PageSize: 2}, -277522265},
		{Request{Prefix: "p", PageSize: 1}, 132063504},
	} {
		if got := c.r.LatencyKey(); got != c.want {
			t.Errorf("%+v: %d, want %d", c.r, got, c.want)
		}
	}
}

// goldenObjects are rows of the tricky-names fixture the Java server
// rendered for TestRenderListGolden: (name, size, epoch microseconds).
func goldenObject(name string, size int64, micros int64) store.Object {
	t := time.UnixMicro(micros).UTC()
	return store.Object{Key: []byte(name), Size: &size, LastModified: &t}
}

const goldenProps = "<Etag>0x000000000000001</Etag><Content-Length>%d</Content-Length><Content-Type>application/octet-stream</Content-Type>" +
	"<BlobType>BlockBlob</BlobType><AccessTier>Hot</AccessTier><AccessTierInferred>true</AccessTierInferred>" +
	"<LeaseStatus>unlocked</LeaseStatus><LeaseState>available</LeaseState></Properties></Blob>"

// TestRenderListGolden pins the body bytes against the Java server's
// answers over the same rows: the BOM, the escaping, the Encoded name
// grammar for U+FFFE, CR in a BlobPrefix as &#13;, the echoes and the
// synthetic properties.
func TestRenderListGolden(t *testing.T) {
	us := func(i int64) int64 { return 1_700_000_000_000_000 + i*1_000_123 }
	objs := []store.Object{
		goldenObject("a&b<c>\"'d", 0, us(0)), goldenObject("dir\r/x", 7, us(1)), goldenObject("u\ufffev", 21, us(3)),
		goldenObject("p\ufffe/q", 35, us(5)), goldenObject("plain/k1", 217, us(31)), goldenObject("plus+sign", 238, us(34)),
		goldenObject("pct%41", 245, us(35)),
	}
	slices.SortFunc(objs, func(a, b store.Object) int { return bytes.Compare(a.Key, b.Key) })
	p := &Pager{Store: newStore(t, true, objs), FixtureIdentity: "f"}
	head := "\xef\xbb\xbf<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<EnumerationResults ServiceEndpoint=\"http://127.0.0.1:29490/replay/\" ContainerName=\"tiny\">"
	blob := func(name, time string, size int) string {
		return "<Blob>" + name + "<Properties><Last-Modified>" + time + "</Last-Modified>" + fmt.Sprintf(goldenProps, size)
	}
	for _, c := range []struct{ query, want string }{
		{"prefix=p&delimiter=/", head + "<Prefix>p</Prefix><Delimiter>/</Delimiter><Blobs>" +
			blob("<Name>pct%41</Name>", "Tue, 14 Nov 2023 22:13:55 GMT", 245) + "<BlobPrefix><Name>plain/</Name></BlobPrefix>" +
			blob("<Name>plus+sign</Name>", "Tue, 14 Nov 2023 22:13:54 GMT", 238) +
			`<BlobPrefix><Name Encoded="true">p%EF%BF%BE%2F</Name></BlobPrefix></Blobs><NextMarker></NextMarker></EnumerationResults>`},
		{"prefix=dir&delimiter=/", head + "<Prefix>dir</Prefix><Delimiter>/</Delimiter><Blobs><BlobPrefix><Name>dir&#13;/</Name></BlobPrefix></Blobs><NextMarker></NextMarker></EnumerationResults>"},
		{"prefix=a%26", head + "<Prefix>a&amp;</Prefix><Blobs>" + blob("<Name>a&amp;b&lt;c&gt;&quot;&apos;d</Name>", "Tue, 14 Nov 2023 22:13:20 GMT", 0) +
			"</Blobs><NextMarker></NextMarker></EnumerationResults>"},
		{"prefix=u", head + "<Prefix>u</Prefix><Blobs>" + blob(`<Name Encoded="true">u%EF%BF%BEv</Name>`, "Tue, 14 Nov 2023 22:13:23 GMT", 21) +
			"</Blobs><NextMarker></NextMarker></EnumerationResults>"},
	} {
		r, err := Parse("GET", "/replay/tiny", "restype=container&comp=list&"+c.query, version(Version20260606), "replay", "tiny")
		if err != nil {
			t.Fatal(err)
		}
		res, err := p.List(context.Background(), r)
		if err != nil {
			t.Fatal(err)
		}
		got, err := AppendList(nil, res, "http://127.0.0.1:29490/replay/")
		if err != nil || string(got) != c.want {
			t.Errorf("%s: %v\n got %q\nwant %q", c.query, err, got, c.want)
		}
	}
	if got := string(AppendError(nil, "InternalError")); got != "\xef\xbb\xbf<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<Error><Code>InternalError</Code><Message>InternalError</Message></Error>" {
		t.Errorf("error body %q", got)
	}
}

// TestRenderRefusals pins the rows the profile cannot render, and that a
// name of more than 254 segments renders (native Azure lists it: evidence
// capture azure-12a).
func TestRenderRefusals(t *testing.T) {
	r := parse(t, "")
	long := strings.Repeat("s/", 254) + "x"
	neg, epoch := int64(-1), int64(0)
	future := time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, c := range []struct {
		obj    store.Object
		prefix bool
		reason string
	}{
		{store.Object{Key: []byte("t\tx")}, false, "control_character"},
		{store.Object{Key: []byte("t\t/")}, true, ""},
		{store.Object{Key: []byte("t\x01/")}, true, "control_character"},
		{store.Object{Key: []byte(strings.Repeat("q", 1024))}, false, ""},
		{store.Object{Key: []byte(strings.Repeat("q", 1025))}, false, "name_too_long"},
		{store.Object{Key: []byte(strings.Repeat("\u00e9", 1024))}, false, ""},
		{store.Object{Key: []byte(strings.Repeat("\U00010000", 513))}, false, "name_too_long"},
		{store.Object{Key: []byte("bad\xff")}, false, "invalid_utf8_name"},
		{store.Object{Key: []byte("\xed\xa0\x80")}, false, "invalid_utf8_name"},
		{store.Object{Key: []byte{}}, false, "empty_name"},
		{store.Object{Key: []byte("n"), Size: &neg}, false, "negative_size"},
		{store.Object{Key: []byte("n"), LastModified: &future}, false, "timestamp_year_out_of_range"},
		{store.Object{Key: []byte("n"), Size: &epoch}, false, ""},
		{store.Object{Key: []byte(long)}, false, ""},
	} {
		e := Entry{Object: &c.obj}
		if c.prefix {
			e = Entry{Prefix: c.obj.Key}
		}
		_, err := AppendList(nil, &Result{Request: r, Entries: []Entry{e}}, "http://h:1/replay/")
		var fe *Error
		if c.reason == "" && err != nil || c.reason != "" && (!errors.As(err, &fe) || fe.Reason != c.reason || fe.Kind != FixtureIncompatible) {
			t.Errorf("%.40q: %v, want %q", c.obj.Key, err, c.reason)
		}
	}
}

// TestClientRequestIDLength pins the length rule over raw header bytes:
// a byte at or above 0x80 is one Latin-1 character, two UTF-8 bytes.
func TestClientRequestIDLength(t *testing.T) {
	for _, c := range []struct {
		id string
		ok bool
	}{
		{strings.Repeat("c", 1024), true}, {strings.Repeat("c", 1025), false},
		{strings.Repeat("\xe9", 512), true}, {strings.Repeat("\xe9", 513), false},
		{strings.Repeat("c", 1023) + "\xe9", false}, {"a\rb", false}, {"", true},
	} {
		if got := ValidClientRequestID(c.id); got != c.ok {
			t.Errorf("%d bytes: %v", len(c.id), got)
		}
		_, err := Parse("GET", "/replay/c1", "restype=container&comp=list",
			http.Header{"X-Ms-Version": {Version20260606}, "X-Ms-Client-Request-Id": {c.id}}, "replay", "c1")
		if (err == nil) != c.ok {
			t.Errorf("%d bytes: parse %v", len(c.id), err)
		}
	}
}
