// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

// The Azure route's section of the driver (-protocol azure): a request matrix
// of the flat List Blobs subset, marker walks, and a structural comparator.
// Both servers serve s3,azure with the account "replay"; the S3 route stays
// on so the key listing and the requests the Azure route does not claim
// are compared too.

const (
	azureServeArgs = " --protocols s3,azure --azure-account replay"
	azureWalkSizes = "1,7,5000"
	azurePath      = "/replay/{bucket}"
	azureBOM       = "\xef\xbb\xbf"
)

var azureVersions = []string{"2026-06-06", "2026-10-06"}

// aq builds an Azure query: the selector, then the given parameters, each
// value percent-encoded with the RFC 3986 unreserved bytes literal.
func aq(params ...string) string {
	var b strings.Builder
	b.WriteString("restype=container&comp=list")
	for i := 0; i+1 < len(params); i += 2 {
		b.WriteByte('&')
		b.WriteString(params[i])
		b.WriteByte('=')
		b.WriteString(azureEncode(params[i+1]))
	}
	return b.String()
}

func azureEncode(v string) string {
	var b strings.Builder
	for i := 0; i < len(v); i++ {
		c := v[i]
		if c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '.' || c == '_' || c == '~' {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

func versionHeader(v string) [][2]string { return [][2]string{{"x-ms-version", v}} }

// azureMarkerCase is a request that resumes with each server's own marker
// from a source request: follow builds the query from that marker (after
// mutate, when set).
type azureMarkerCase struct {
	name   string
	source request
	follow func(marker string) request
	// refused marks a follow-up that changes the marker's scope: both
	// servers must refuse it as an invalid marker, not merely agree.
	refused bool
	// skip, when set, says why the fixture cannot make the case
	// meaningful; the case is reported skipped without a request.
	skip string
}

// azureMatrix builds the scripted requests for a fixture whose keys
// (byte-sorted) are given, and the marker cases.
func azureMatrix(keys [][]byte, rng *rand.Rand, maxPrefixes int) ([]request, []azureMarkerCase) {
	var out []request
	n := 0
	add := func(name string, header [][2]string, query string) {
		out = append(out, request{Name: name, Method: "GET", Path: azurePath, Query: query, Header: header})
	}
	// alt alternates the service version so both are covered without
	// doubling the matrix.
	alt := func() [][2]string {
		n++
		return versionHeader(azureVersions[n%2])
	}
	for _, v := range azureVersions {
		add("version "+v, versionHeader(v), aq("maxresults", "3"))
		add("version "+v+" delimiter", versionHeader(v), aq("maxresults", "3", "delimiter", "/"))
	}
	for _, mr := range []string{"1", "2", "3", "5000", "5001", "0", "-1", "abc", "", "002", "+2", "2147483647", "2147483648",
		"9223372036854775808", "1.5", " 7", "\u0661"} {
		add("maxresults="+mr, alt(), aq("maxresults", mr))
	}
	prefixes := directoryPrefixes(keys)
	if len(prefixes) > maxPrefixes {
		rng.Shuffle(len(prefixes), func(i, j int) { prefixes[i], prefixes[j] = prefixes[j], prefixes[i] })
		prefixes = prefixes[:maxPrefixes]
		slices.Sort(prefixes)
	}
	// String prefixes that stop inside a segment, and one that matches
	// nothing.
	for i := 0; i < len(keys) && i < 5; i++ {
		k := keys[rng.IntN(len(keys))]
		prefixes = append(prefixes, string(k[:1+rng.IntN(len(k))]))
	}
	prefixes = append(prefixes, "zz-no-such-prefix/", "")
	multi := multiByteDelimiter(keys, rng)
	for _, p := range prefixes {
		add("prefix="+p, alt(), aq("prefix", p, "maxresults", "100"))
		add("prefix="+p+" delimiter=/", alt(), aq("prefix", p, "delimiter", "/", "maxresults", "100"))
		add("prefix="+p+" delimiter=/ maxresults=3", alt(), aq("prefix", p, "delimiter", "/", "maxresults", "3"))
		add("prefix="+p+" delimiter="+multi, alt(), aq("prefix", p, "delimiter", multi, "maxresults", "100"))
		add("prefix="+p+" maxresults=1", alt(), aq("prefix", p, "maxresults", "1"))
	}
	for _, d := range []string{"é", "-", "|", "/1", "\t", "ab", "\U0001F600"} {
		add("delimiter="+d, alt(), aq("delimiter", d, "maxresults", "50"))
	}
	add("empty delimiter", alt(), aq("delimiter", "", "maxresults", "5"))
	add("empty prefix", alt(), aq("prefix", "", "maxresults", "5"))
	add("empty marker", alt(), aq("marker", "", "maxresults", "5"))
	add("empty startFrom", alt(), aq("startFrom", "", "maxresults", "5"))
	// startFrom at, between and around names, inside rolled prefixes, and
	// with multi-byte and astral characters so the UTF-16 order engages
	// (the ranges split at every EE..F4 lead byte of the bound).
	if len(keys) > 0 {
		first, last := string(keys[0]), string(keys[len(keys)-1])
		mid := string(keys[len(keys)/2])
		pick := string(keys[rng.IntN(len(keys))])
		astral := strings.Repeat("\U00010000", 1024) // 4,096 bytes
		for _, sf := range []struct{ name, v string }{
			{"before", "\x01"}, {"first", first}, {"at", mid}, {"between", mid + "\x00"}, {"random", pick},
			{"last", last}, {"after", last + "~"}, {"e000", "\ue000"}, {"ffff", "\uffff"}, {"10000", "\U00010000"},
			{"mid+e000", mid + "\ue000"}, {"mid+astral", mid + "\U0001F600"}, {"mid+é", mid + "é"},
			{"mid[:half]+efbfbd", mid[:len(mid)/2] + "\ufffd"}, {"astral 4096", astral},
		} {
			add("startFrom "+sf.name, alt(), aq("startFrom", sf.v, "maxresults", "50"))
			add("startFrom "+sf.name+" delimiter=/", alt(), aq("startFrom", sf.v, "delimiter", "/", "maxresults", "50"))
		}
		for _, p := range prefixes[:min(len(prefixes), 20)] {
			if p == "" || !strings.HasSuffix(p, "/") {
				continue
			}
			parent := parentPrefix(p)
			// Inside a rolled prefix: before its first name, at a name of
			// it, after its last name.
			add("prefix "+parent+" startFrom="+p+" delimiter", alt(), aq("prefix", parent, "startFrom", p, "delimiter", "/", "maxresults", "20"))
			add("prefix "+parent+" startFrom inside "+p, alt(), aq("prefix", parent, "startFrom", p+"\x00", "delimiter", "/", "maxresults", "20"))
			add("prefix "+parent+" startFrom after "+p, alt(), aq("prefix", parent, "startFrom", p+"\U0010FFFF", "delimiter", "/", "maxresults", "20"))
			add("prefix "+p+" startFrom before", alt(), aq("prefix", p, "startFrom", parent, "maxresults", "20"))
			add("prefix "+p+" startFrom past", alt(), aq("prefix", p, "startFrom", p+"\U0010FFFF", "maxresults", "20"))
		}
	}
	h := versionHeader(azureVersions[0])
	with := func(extra ...[2]string) [][2]string { return append(slices.Clone(h), extra...) }
	// Headers.
	add("no version", nil, aq("maxresults", "1"))
	add("unknown version", versionHeader("2025-01-05"), aq("maxresults", "1"))
	add("duplicate version", with([2]string{"x-ms-version", "2026-06-06"}), aq("maxresults", "1"))
	add("client request id", with([2]string{"x-ms-client-request-id", "client-1"}), aq("maxresults", "1"))
	add("empty client request id", with([2]string{"x-ms-client-request-id", ""}), aq("maxresults", "1"))
	add("long client request id", with([2]string{"x-ms-client-request-id", strings.Repeat("c", 1025)}), aq("maxresults", "1"))
	add("duplicate client request id", with([2]string{"x-ms-client-request-id", "a"}, [2]string{"x-ms-client-request-id", "b"}), aq("maxresults", "1"))
	// Raw obs-text bytes: the length limit counts each as two.
	for _, n := range []int{512, 513} {
		id := strings.Repeat("\xe9", n)
		add(fmt.Sprintf("client request id %d raw 0xE9", n), with([2]string{"x-ms-client-request-id", id}), aq("maxresults", "1"))
		add(fmt.Sprintf("client request id %d raw 0xE9 bad version", n), [][2]string{{"x-ms-version", "x"}, {"x-ms-client-request-id", id}}, aq("maxresults", "1"))
	}
	add("client request id mixed obs-text", with([2]string{"x-ms-client-request-id", "id-\xc3\xa9-\xff"}), aq("maxresults", "1"))
	add("client id with bad version", [][2]string{{"x-ms-version", "x"}, {"x-ms-client-request-id", "c"}}, aq("maxresults", "1"))
	for _, a := range []string{"application/xml", "*/*", "application/json", "application/vnd.apache.arrow.stream", "application/xml;q=0",
		"application/xml;q=0.5", "application/xml;q=2", "application/xml;q=abc", "text/plain, application/xml", "application/xml,", ",", " ", "APPLICATION/XML;Q=1E0"} {
		add("accept "+a, with([2]string{"Accept", a}), aq("maxresults", "1"))
	}
	// Query grammar.
	for _, c := range []struct{ name, query string }{
		{"include=metadata", aq("include", "metadata")}, {"include empty", aq("include", "", "maxresults", "1")},
		{"timeout=0", aq("timeout", "0")}, {"timeout=1", aq("timeout", "1", "maxresults", "1")}, {"timeout=3600", aq("timeout", "3600", "maxresults", "1")},
		{"timeout=3601", aq("timeout", "3601")}, {"timeout=abc", aq("timeout", "abc")}, {"timeout empty", aq("timeout", "")},
		{"unknown parameter", aq("showonly", "files")}, {"endBefore", aq("endBefore", "z")},
		{"duplicate prefix", aq("prefix", "a", "prefix", "a")}, {"duplicate restype", "restype=container&restype=container&comp=list"},
		{"restype only", "restype=container"}, {"comp only", "comp=list"}, {"restype=x", "restype=x&comp=list"},
		{"encoded selector", "rest%79pe=container&comp=list&maxresults=1"},
		{"bad escape", "restype=container&comp=list&prefix=%ZZ"}, {"trailing percent", "restype=container&comp=list&prefix=%"},
		{"invalid utf-8", "restype=container&comp=list&prefix=%C3%28"},
		{"raw invalid utf-8", "restype=container&comp=list&prefix=a\xff&maxresults=1"},
		{"raw truncated astral", "restype=container&comp=list&prefix=\xf0\x9f\x98&maxresults=1"},
		{"raw encoded surrogate", "restype=container&comp=list&prefix=\xed\xa0\x80&maxresults=1"},
		{"raw plus", "restype=container&comp=list&prefix=a+b&maxresults=1"},
		{"raw question mark", "restype=container&comp=list&prefix=a?b&maxresults=1"}, {"raw utf-8", "restype=container&comp=list&prefix=é&maxresults=1"},
		{"name without =", "restype=container&comp=list&prefix"}, {"empty field", "restype=container&&comp=list"},
		{"control in prefix", aq("prefix", "a\x01")}, {"cr in prefix", aq("prefix", "a\r")}, {"tab in prefix", aq("prefix", "a\t", "maxresults", "1")},
		{"ffff in delimiter", aq("delimiter", "\uffff")}, {"long prefix", aq("prefix", strings.Repeat("p", 4097))},
		{"4096-byte prefix", aq("prefix", strings.Repeat("p", 4096), "maxresults", "1")}, {"long startFrom", aq("startFrom", strings.Repeat("s", 4097))},
		{"bad marker", aq("marker", "zz")}, {"marker wrong prefix", aq("marker", "az1.AAAA")}, {"long marker", aq("marker", strings.Repeat("m", 8193))},
		{"xml echo", aq("prefix", `<&>"'`, "maxresults", "1")},
	} {
		add(c.name, h, c.query)
	}
	// Routing and methods.
	out = append(out,
		request{Name: "HEAD", Method: "HEAD", Path: azurePath, Query: aq("maxresults", "1"), Header: h},
		request{Name: "POST", Method: "POST", Path: azurePath, Query: aq("maxresults", "1"), Header: h},
		request{Name: "DELETE", Method: "DELETE", Path: azurePath, Query: aq(), Header: h},
		request{Name: "wrong container", Method: "GET", Path: "/replay/no-such-container", Query: aq(), Header: h},
		request{Name: "HEAD wrong container", Method: "HEAD", Path: "/replay/no-such-container", Query: aq(), Header: h},
		// The next four are answered by the S3 route. Here they test the
		// routing: the same status and classified headers, so the same
		// route answered. Their bodies are the S3 route's, which answers
		// as live S3 does where the Java server does not (the error
		// document, the continuation token); the S3 section holds those
		// bodies through its divergence rules.
		request{Name: "wrong account", Method: "GET", Path: "/other/{bucket}", Query: aq(), Header: h, Compare: "headers"},
		request{Name: "extra segment", Method: "GET", Path: "/replay/{bucket}/x", Query: aq(), Header: h, Compare: "headers"},
		request{Name: "ambiguous selectors", Method: "GET", Path: azurePath, Query: aq("list-type", "2"), Header: h},
		request{Name: "S3 beside Azure", Method: "GET", Path: "", Query: "list-type=2&max-keys=2", Compare: "headers"},
		request{Name: "account root", Method: "GET", Path: "/replay/", Query: aq(), Header: h, Compare: "headers"},
	)
	// Marker cases: each server resumes with its own marker.
	base := request{Method: "GET", Path: azurePath, Header: h}
	src := func(params ...string) request { r := base; r.Query = aq(params...); return r }
	follow := func(params ...string) func(string) request {
		return func(m string) request { r := base; r.Query = aq(append(params, "marker", m)...); return r }
	}
	// The marker cases resume from a source that pages: one result per
	// page in scopes that hold at least two names (the whole container, a
	// directory prefix with two or more names, a startFrom at the first
	// name). With fewer than two names there is no marker to test.
	pfx := pagingPrefix(keys)
	rolled := rolledPrefix(keys)
	from := ""
	if len(keys) > 0 {
		from = string(keys[0])
	}
	noPrefix, noName := "", ""
	if pfx == "" {
		noPrefix = "no directory prefix holds two names: an absent and an empty prefix bind the same scope"
	}
	if from == "" {
		noName = "the fixture has no name to start from"
	}
	marks := []azureMarkerCase{
		{name: "marker page 2", source: src("maxresults", "1"), follow: follow("maxresults", "1")},
		{name: "marker page 2 delimiter", source: src("maxresults", "1", "prefix", rolled, "delimiter", "/"), follow: follow("maxresults", "1", "prefix", rolled, "delimiter", "/")},
		{name: "marker page 2 prefix", source: src("maxresults", "1", "prefix", pfx), follow: follow("maxresults", "1", "prefix", pfx), skip: noPrefix},
		{name: "marker changed page size", source: src("maxresults", "1"), follow: follow("maxresults", "7")},
		{name: "marker added delimiter", source: src("maxresults", "1"), follow: follow("maxresults", "3", "delimiter", "/"), refused: true},
		{name: "marker removed delimiter", source: src("maxresults", "1", "prefix", rolled, "delimiter", "/"), follow: follow("maxresults", "3", "prefix", rolled), refused: true},
		{name: "marker other prefix", source: src("maxresults", "1", "prefix", pfx), follow: follow("maxresults", "3", "prefix", pfx+"zz"), refused: true, skip: noPrefix},
		{name: "marker dropped prefix", source: src("maxresults", "1", "prefix", pfx), follow: follow("maxresults", "3"), refused: true, skip: noPrefix},
		{name: "marker other version", source: src("maxresults", "1"), follow: func(m string) request {
			r := follow("maxresults", "3")(m)
			r.Header = versionHeader(azureVersions[1])
			return r
		}, refused: true},
		{name: "marker with same startFrom", source: src("maxresults", "1", "startFrom", from), follow: follow("maxresults", "3", "startFrom", from), skip: noName},
		{name: "marker without its startFrom", source: src("maxresults", "1", "startFrom", from), follow: follow("maxresults", "3"), refused: true, skip: noName},
		{name: "marker truncated", source: src("maxresults", "1"), follow: func(m string) request { return follow("maxresults", "3")(m[:max(1, len(m)-4)]) }},
		{name: "marker last char flipped", source: src("maxresults", "1"), follow: func(m string) request {
			last := byte('A')
			if m[len(m)-1] == 'A' {
				last = 'B'
			}
			return follow("maxresults", "3")(m[:len(m)-1] + string(last))
		}},
		{name: "marker padded", source: src("maxresults", "1"), follow: func(m string) request {
			return follow("maxresults", "3")(m + strings.Repeat("=", (4-(len(m)-4)%4)%4))
		}},
		// Progress sits outside the scope digest: a marker that carries 1
		// is accepted, and the next marker must keep it.
		{name: "marker progress 1", source: src("maxresults", "1"), follow: func(m string) request {
			return follow("maxresults", "1")(markerWithProgress(m, 1))
		}},
	}
	return out, marks
}

// invalidMarker reports the route's refusal of a marker of another scope.
func invalidMarker(r *response) bool {
	return r.Status == 400 && values(r.Header, "x-ms-error-code") == "InvalidQueryParameterValue" &&
		values(r.Header, "x-swath-replay-error") == "invalid_marker"
}

// pagingPrefix is the first directory prefix holding at least two names,
// or "" (the whole container) when none does.
func pagingPrefix(keys [][]byte) string {
	count := map[string]int{}
	for _, k := range keys {
		if i := bytes.IndexByte(k, '/'); i >= 0 {
			count[string(k[:i+1])]++
		}
	}
	for _, p := range directoryPrefixes(keys) {
		if count[p] >= 2 && echoable(p) {
			return p
		}
	}
	return ""
}

// echoable reports whether the route accepts p as a prefix: valid UTF-8
// without a control character other than TAB and LF, U+FFFE or U+FFFF.
func echoable(p string) bool {
	if !utf8.ValidString(p) {
		return false
	}
	for _, r := range p {
		if r < 0x20 && r != '\t' && r != '\n' || r == 0xfffe || r == 0xffff {
			return false
		}
	}
	return true
}

// rolledPrefix is the first prefix ("" or a directory) under which a "/"
// listing holds at least two entries, so a one-entry delimiter page has a
// marker; "" when none does.
func rolledPrefix(keys [][]byte) string {
	for _, p := range append([]string{""}, directoryPrefixes(keys)...) {
		if !echoable(p) {
			continue
		}
		seen := map[string]bool{}
		for _, k := range keys {
			rest, ok := bytes.CutPrefix(k, []byte(p))
			if !ok {
				continue
			}
			if i := bytes.IndexByte(rest, '/'); i >= 0 {
				rest = rest[:i+1]
			}
			seen[string(rest)] = true
			if len(seen) >= 2 {
				return p
			}
		}
	}
	return ""
}

// azureWalks lists the marker walks: flat and delimited, the whole
// container and each top-level prefix, from the start and from a startFrom,
// at each page size, alternating the service version.
func azureWalks(keys [][]byte, pageSizes []int, maxTop int) []azureWalk {
	var out []azureWalk
	tops := topLevel(keys)
	if len(tops) > maxTop {
		tops = tops[:maxTop]
	}
	mid := ""
	if len(keys) > 0 {
		mid = string(keys[len(keys)/2])
	}
	n := 0
	add := func(name string, params ...string) {
		n++
		v := azureVersions[n%2]
		out = append(out, azureWalk{name + " " + v, aq(params...), versionHeader(v)})
	}
	for _, size := range pageSizes {
		m := fmt.Sprint(size)
		add("container maxresults="+m, "maxresults", m)
		add("container delimiter=/ maxresults="+m, "delimiter", "/", "maxresults", m)
		add("container startFrom mid maxresults="+m, "startFrom", mid, "maxresults", m)
		add("container startFrom mid delimiter=/ maxresults="+m, "startFrom", mid, "delimiter", "/", "maxresults", m)
		add("container startFrom mid+astral maxresults="+m, "startFrom", mid+"\U0001F600", "maxresults", m)
		for _, t := range tops {
			add("prefix="+t+" maxresults="+m, "prefix", t, "maxresults", m)
			add("prefix="+t+" delimiter=/ maxresults="+m, "prefix", t, "delimiter", "/", "maxresults", m)
		}
	}
	return out
}

type azureWalk struct {
	Name, Query string
	Header      [][2]string
}

// runAzure runs the Azure matrix, the marker cases, the walks and a
// seeded random set.
func (d *driver) runAzure(keys [][]byte, rng *rand.Rand) {
	d.comp = azureComparator{acceptSegments: d.o.acceptSegments}
	cases, marks := azureMatrix(keys, rng, d.o.maxPrefixes)
	d.runCases("case", cases)
	for _, m := range marks {
		d.record(d.markerCase(m))
	}
	var sizes []int
	for _, s := range strings.Split(d.o.walkSizes, ",") {
		var n int
		if _, err := fmt.Sscan(strings.TrimSpace(s), &n); err == nil && n > 0 {
			sizes = append(sizes, n)
		}
	}
	walks := azureWalks(keys, sizes, d.o.maxTop)
	work := make(chan azureWalk)
	var wg sync.WaitGroup
	for range d.o.parallel {
		wg.Go(func() {
			for w := range work {
				d.record(d.azureWalkOne(w))
			}
		})
	}
	for _, w := range walks {
		work <- w
	}
	close(work)
	wg.Wait()
	prefixes := directoryPrefixes(keys)
	random := make([]request, d.o.random)
	for i := range random {
		random[i] = azureRandom(keys, rng, prefixes)
		random[i].Name = fmt.Sprintf("random %d", i)
	}
	d.runCases("random", random)
	fmt.Printf("azure: %d answers matched by marker effect only (marker bytes differ); %d accepted as the segment improvement\n",
		markerBytesDiffer.Load(), improvements.Load())
}

func azureRandom(keys [][]byte, rng *rand.Rand, prefixes []string) request {
	var params []string
	if len(prefixes) > 0 && rng.IntN(3) > 0 {
		params = append(params, "prefix", prefixes[rng.IntN(len(prefixes))])
	}
	switch rng.IntN(4) {
	case 0:
		params = append(params, "delimiter", "/")
	case 1:
		params = append(params, "delimiter", []string{"-", "_", "/a", "0", "é"}[rng.IntN(5)])
	}
	if len(keys) > 0 && rng.IntN(2) == 0 {
		k := keys[rng.IntN(len(keys))]
		switch rng.IntN(4) {
		case 0:
			k = k[:rng.IntN(len(k)+1)]
		case 1:
			k = append(bytes.Clone(k), []byte([]string{"\x00", "~", "é", "\ue000", "\U0001F600"}[rng.IntN(5)])...)
		}
		if utf8.Valid(k) {
			params = append(params, "startFrom", string(k))
		}
	}
	params = append(params, "maxresults", fmt.Sprint([]int{1, 1, 2, 3, 7, 50, 5000, 5001}[rng.IntN(8)]))
	return request{Method: "GET", Path: azurePath, Query: aq(params...), Header: versionHeader(azureVersions[rng.IntN(2)])}
}

func (d *driver) markerCase(m azureMarkerCase) result {
	res := result{Kind: "case", Name: m.name, Method: "GET"}
	if m.skip != "" {
		res.Skipped = m.skip
		return res
	}
	a, err := d.ref.do(d.o.bucket, m.source)
	if err != nil {
		res.Error = err.Error()
		return res
	}
	b, err := d.got.do(d.o.bucket, m.source)
	if err != nil {
		res.Error = err.Error()
		return res
	}
	d.count(2)
	ma, mb := azureNextMarker(a.Body), azureNextMarker(b.Body)
	if ma == "" || mb == "" {
		// The case cannot run: report a differing source as a failure and
		// an agreeing one as a skip, never as a pass.
		if df := d.comp.compare(a, b); !df.OK {
			res.diff = &df
			res.Error = "source answers differ"
			return res
		}
		res.Skipped = fmt.Sprintf("no marker to resume: the source answered %d without one", a.Status)
		return res
	}
	ra, rb := m.follow(ma), m.follow(mb)
	res.Target = d.got.url(d.o.bucket, rb)
	a, err = d.ref.do(d.o.bucket, ra)
	if err == nil {
		b, err = d.got.do(d.o.bucket, rb)
	}
	if err != nil {
		res.Error = err.Error()
		return res
	}
	d.count(2)
	df := d.comp.compare(a, b)
	res.Pass = df.OK
	if !df.OK {
		res.diff = &df
	}
	if m.refused && df.OK && !invalidMarker(a) {
		// A scope change both servers accept means the case resumed the
		// same scope after all: it tested nothing.
		res.Pass = false
		res.Error = fmt.Sprintf("a changed scope was accepted: want 400 InvalidQueryParameterValue invalid_marker, got %d %s",
			a.Status, values(a.Header, "x-swath-replay-error"))
	}
	return res
}

// azureWalkOne follows each server's own markers to the end, comparing
// every page; the first difference ends the walk.
func (d *driver) azureWalkOne(w azureWalk) result {
	res := result{Kind: "walk", Name: w.Name, Method: "GET", Pass: true}
	var ma, mb string
	for {
		ra := request{Name: w.Name, Method: "GET", Path: azurePath, Query: w.Query, Header: w.Header}
		rb := ra
		if res.Pages > 0 {
			ra.Query += "&marker=" + azureEncode(ma)
			rb.Query += "&marker=" + azureEncode(mb)
		}
		res.Target = d.got.url(d.o.bucket, rb)
		a, err := d.ref.do(d.o.bucket, ra)
		var b *response
		if err == nil {
			b, err = d.got.do(d.o.bucket, rb)
		}
		res.Pages++
		if err != nil {
			res.Pass, res.FailAt, res.Error = false, res.Pages, err.Error()
			return res
		}
		d.count(2)
		df := d.comp.compare(a, b)
		if !df.OK {
			res.Pass, res.FailAt, res.diff = false, res.Pages, &df
			return res
		}
		if df.Detail != "" {
			res.Error = df.Detail // the reference cannot continue
			return res
		}
		ma, mb = azureNextMarker(a.Body), azureNextMarker(b.Body)
		if ma == "" {
			return res
		}
		if d.o.walkLimit > 0 && res.Pages >= d.o.walkLimit {
			res.Partial = true
			return res
		}
	}
}

var (
	nextMarkerRe = regexp.MustCompile(`<NextMarker>([^<]*)</NextMarker>`)
	// The two marker elements are matched separately, each with its own
	// closing tag, so a corrupted pair such as <NextMarker>...</Marker> is
	// left as it is and compared byte for byte.
	markerRe    = regexp.MustCompile(`<Marker>([^<]*)</Marker>`)
	nextMarkRe  = regexp.MustCompile(`<NextMarker>([^<]*)</NextMarker>`)
	endpointRe2 = regexp.MustCompile(`ServiceEndpoint="http://[^"/]*/`)
	requestIDRe = regexp.MustCompile(`^replay-[0-9a-f]+$`)
)

func azureNextMarker(body []byte) string {
	m := nextMarkerRe.FindSubmatch(body)
	if m == nil {
		return ""
	}
	return xmlUnescape(string(m[1]))
}

// markerEffect is what a marker resumes: its flag, progress and boundary.
// Two markers with the same effect resume the same listing, whatever scope
// digest each carries (the fixture identities of the two servers may
// differ, for example a Java DuckDB capture against a Go sorted fixture of
// the same rows).
func markerEffect(m string) string {
	raw, ok := strings.CutPrefix(m, "az1.")
	if !ok {
		return m
	}
	b, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(raw, "="))
	if err != nil || len(b) < 18 {
		return m
	}
	return fmt.Sprintf("az1(flag=%d progress=%d boundary=%s)", b[0], b[1], hex.EncodeToString(b[18:]))
}

// markerWithProgress is m with its progress byte (the second payload
// byte) set to v and re-encoded unpadded; m unchanged when it is not a
// decodable marker.
func markerWithProgress(m string, v byte) string {
	raw, ok := strings.CutPrefix(m, "az1.")
	if !ok {
		return m
	}
	b, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(raw, "="))
	if err != nil || len(b) < 2 {
		return m
	}
	b[1] = v
	return "az1." + base64.RawURLEncoding.EncodeToString(b)
}

// azureComparator compares Azure answers structurally: the same status;
// the BOM before the declaration on both bodies (or on neither, for an
// answer of another route); the body bytes equal once the ServiceEndpoint
// authority (each server's own port) is masked and every marker is
// replaced by its effect; the same classified headers (Content-Type,
// x-ms-version, x-ms-error-code, x-ms-client-request-id, allow and
// x-swath-replay-*); and a request id and Date of the right shape on each.
// An answer without the BOM on either side (the S3 route, the replay
// envelope) is compared byte for byte as the S3 driver does.
type azureComparator struct {
	acceptSegments bool
}

// markerBytesDiffer counts answers that matched by marker effect but not
// byte for byte (servers whose fixture identities differ).
var markerBytesDiffer atomic.Int64

var azureCompared = []string{"content-type", "x-ms-version", "x-ms-error-code", "x-ms-client-request-id", "allow", "x-swath-replay-error",
	"x-amz-request-id", "x-amz-id-2"}

// segmentImprovement reports the one known, intended difference: the
// reference refuses a page holding a name of more than 254 segments
// (500, too_many_segments) where native Azure lists it and the Go route
// serves it (evidence capture azure-12a).
func segmentImprovement(ref, got *response) bool {
	return ref.Status == 500 && got.Status == 200 && values(ref.Header, "x-swath-replay-error") == "too_many_segments"
}

// improvements counts answers accepted as the segment improvement under
// -accept-segment-improvement.
var improvements atomic.Int64

func (c azureComparator) compare(ref, got *response) diff {
	if c.acceptSegments && segmentImprovement(ref, got) {
		improvements.Add(1)
		return diff{OK: true, Status: [2]int{ref.Status, got.Status}, Offset: -1, Detail: "improvement: too_many_segments"}
	}
	if !bytes.HasPrefix(ref.Body, []byte(azureBOM)) && !bytes.HasPrefix(got.Body, []byte(azureBOM)) &&
		values(ref.Header, "x-ms-request-id") == "" && values(got.Header, "x-ms-request-id") == "" {
		return bytesComparator{}.compare(ref, got)
	}
	d := diff{Status: [2]int{ref.Status, got.Status}, Offset: -1}
	for _, n := range azureCompared {
		if a, b := values(ref.Header, n), values(got.Header, n); a != b {
			d.HeaderDiffs = append(d.HeaderDiffs, fmt.Sprintf("%s: %q | %q", n, a, b))
		}
	}
	for side, r := range []*response{ref, got} {
		if id := values(r.Header, "x-ms-request-id"); !requestIDRe.MatchString(id) {
			d.HeaderDiffs = append(d.HeaderDiffs, fmt.Sprintf("x-ms-request-id[%d]: %q", side, id))
		}
		if _, err := time.Parse(http.TimeFormat, values(r.Header, "date")); err != nil {
			d.HeaderDiffs = append(d.HeaderDiffs, fmt.Sprintf("date[%d]: %q", side, values(r.Header, "date")))
		}
	}
	if (len(ref.Body) > 0) != bytes.HasPrefix(ref.Body, []byte(azureBOM)) || (len(got.Body) > 0) != bytes.HasPrefix(got.Body, []byte(azureBOM)) {
		d.Detail = "a body without the BOM"
	}
	a, b := normalizeAzure(ref.Body), normalizeAzure(got.Body)
	if bytes.Equal(a, b) {
		ea := endpointRe2.ReplaceAll(ref.Body, []byte(`ServiceEndpoint="http://HOST:PORT/`))
		eb := endpointRe2.ReplaceAll(got.Body, []byte(`ServiceEndpoint="http://HOST:PORT/`))
		if !bytes.Equal(ea, eb) {
			markerBytesDiffer.Add(1)
		}
	} else {
		d.Offset = firstDiff(a, b)
		d.RefExcerpt, d.GotExcerpt = excerpt(a, d.Offset), excerpt(b, d.Offset)
	}
	d.OK = ref.Status == got.Status && len(d.HeaderDiffs) == 0 && d.Offset < 0 && d.Detail == ""
	return d
}

func normalizeAzure(body []byte) []byte {
	body = endpointRe2.ReplaceAll(body, []byte(`ServiceEndpoint="http://HOST:PORT/`))
	for _, m := range []struct {
		re  *regexp.Regexp
		tag string
	}{{markerRe, "Marker"}, {nextMarkRe, "NextMarker"}} {
		body = m.re.ReplaceAllFunc(body, func(b []byte) []byte {
			v := m.re.FindSubmatch(b)[1]
			return []byte("<" + m.tag + ">" + markerEffect(xmlUnescape(string(v))) + "</" + m.tag + ">")
		})
	}
	return body
}
