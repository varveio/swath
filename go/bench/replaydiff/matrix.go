// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"

	"github.com/varveio/swath/go/internal/replay/s3"
)

// request is one request of the matrix. A route's matrix is data: a list
// of these built from the fixture's keys, so another listing route adds
// its own builder and comparator without touching the driver.
type request struct {
	Name   string `json:"name"`
	Method string `json:"method"`
	// Path is the request path; "" is the bucket's root ("/" + bucket).
	Path  string `json:"path,omitempty"`
	Query string `json:"query"`
	// Live marks a request that means the same against live S3
	// (virtual-host style, so no bucket in the path).
	Live bool `json:"-"`
	// TokenFrom, when set, names a request whose reference answer's
	// NextContinuationToken is appended to this one's query. When that
	// answer has no token the request is not sent and is reported as
	// skipped, never as passing.
	TokenFrom string `json:"-"`
	// Raw, when set, is the whole request target, sent byte for byte on
	// its own connection ({bucket} replaced), for targets a URL-building
	// client would normalize or refuse to send.
	Raw string `json:"raw,omitempty"`
	// Compare narrows the comparison for answers the contract's HTTP layer
	// writes itself: "headers" compares the status and the headers but
	// not the HTML error page (this server's page is its own); "status"
	// compares the status only, for request lines Go's HTTP parser refuses
	// before the server sees them (its page and Content-Type are the
	// parser's); "residual" sends a documented difference and reports what
	// each server answered without passing or failing it. Empty compares
	// everything.
	Compare string `json:"compare,omitempty"`
	// Wire, when set, builds the whole byte stream sent on a connection of
	// its own (several requests, odd line endings); the last answer is the
	// one compared.
	Wire func(bucket, host string) string `json:"-"`
	// BucketPad, when positive, is the length of an X-Pad header the raw
	// request carries after its other headers, less the bucket name's
	// length (so the head's size does not depend on the bucket).
	BucketPad int `json:"-"`
	// Header holds request headers, a name repeated when sent twice.
	Header [][2]string `json:"-"`
}

// walk is a token walk: the same request repeated with each answer's
// NextContinuationToken until the listing ends.
type walk struct {
	Name  string
	Query string
}

func q(params ...string) string {
	var b strings.Builder
	for i := 0; i+1 < len(params); i += 2 {
		if b.Len() > 0 {
			b.WriteByte('&')
		}
		b.WriteString(params[i])
		b.WriteByte('=')
		b.WriteString(s3.PercentEncode([]byte(params[i+1])))
	}
	return b.String()
}

// s3Matrix builds the scripted ListObjectsV2 requests for a fixture whose
// keys (sorted) are given.
func s3Matrix(keys [][]byte, rng *rand.Rand, maxPrefixes int) []request {
	var out []request
	add := func(name string, live bool, params ...string) {
		out = append(out, request{Name: name, Method: "GET", Query: q(append([]string{"list-type", "2"}, params...)...), Live: live})
	}
	for _, mk := range []string{"0", "1", "2", "999", "1000", "1001", "100000", "abc", "-1", "", " 7", "+3", "2147483648"} {
		add("max-keys="+mk, true, "max-keys", mk)
		add("max-keys="+mk+" url", true, "max-keys", mk, "encoding-type", "url")
	}
	// Prefixes at every depth of the keys, a sample when there are many,
	// and one that matches nothing.
	prefixes := directoryPrefixes(keys)
	if len(prefixes) > maxPrefixes {
		rng.Shuffle(len(prefixes), func(i, j int) { prefixes[i], prefixes[j] = prefixes[j], prefixes[i] })
		prefixes = prefixes[:maxPrefixes]
		slices.Sort(prefixes)
	}
	prefixes = append(prefixes, "zz-no-such-prefix/", "")
	multi := multiByteDelimiter(keys, rng)
	for _, p := range prefixes {
		add("prefix="+p, true, "prefix", p, "max-keys", "1000", "encoding-type", "url")
		add("prefix="+p+" delimiter=/", true, "prefix", p, "delimiter", "/", "max-keys", "1000", "encoding-type", "url")
		add("prefix="+p+" delimiter=/ max-keys=3", true, "prefix", p, "delimiter", "/", "max-keys", "3")
		add("prefix="+p+" delimiter="+multi, true, "prefix", p, "delimiter", multi, "max-keys", "1000", "encoding-type", "url")
		add("prefix="+p+" max-keys=1", true, "prefix", p, "max-keys", "1")
	}
	add("delimiter=é", true, "delimiter", "é", "max-keys", "100")
	add("delimiter=-", true, "delimiter", "-", "max-keys", "100", "encoding-type", "url")
	// start-after before, at, between and after keys, and at prefix
	// boundaries (a directory and its successor).
	if len(keys) > 0 {
		first, last := string(keys[0]), string(keys[len(keys)-1])
		mid := string(keys[len(keys)/2])
		pick := string(keys[rng.IntN(len(keys))])
		for _, sa := range []struct{ name, v string }{
			{"before", "\x01"}, {"first", first}, {"at", mid}, {"between", mid + "\x00"}, {"random", pick},
			{"last", last}, {"after", last + "\xff"}, {"high", "\xff\xff"},
		} {
			add("start-after "+sa.name, true, "start-after", sa.v, "max-keys", "50", "encoding-type", "url")
			add("start-after "+sa.name+" delimiter=/", true, "start-after", sa.v, "delimiter", "/", "max-keys", "50")
		}
		for _, p := range prefixes[:min(len(prefixes), 20)] {
			if p == "" {
				continue
			}
			next, _ := s3.Successor([]byte(p))
			add("start-after=prefix "+p, true, "start-after", p, "delimiter", "/", "max-keys", "20")
			add("start-after=successor "+p, true, "start-after", string(next), "delimiter", "/", "max-keys", "20")
			add("prefix+start-after inside "+p, true, "prefix", p, "start-after", p+"\x00", "max-keys", "20")
			parent := parentPrefix(p)
			add("prefix "+parent+" start-after "+p, true, "prefix", parent, "delimiter", "/", "start-after", p, "max-keys", "20")
		}
	}
	// Keys that need encoding, as themselves and as boundaries.
	for i, k := range keysNeedingEncoding(keys, 20) {
		add(fmt.Sprintf("encoded key %d", i), true, "prefix", string(k), "max-keys", "5", "encoding-type", "url")
		add(fmt.Sprintf("encoded key %d unencoded", i), true, "prefix", string(k), "max-keys", "5")
		add(fmt.Sprintf("encoded key %d start-after", i), true, "start-after", string(k), "max-keys", "5", "encoding-type", "url")
	}
	add("fetch-owner=true", true, "fetch-owner", "true", "max-keys", "50")
	add("fetch-owner=TRUE delimiter", true, "fetch-owner", "TRUE", "delimiter", "/", "max-keys", "50")
	add("fetch-owner=false", true, "fetch-owner", "false", "max-keys", "50")
	// Tokens: malformed, blank, foreign (a live S3 token), v1, and a real
	// token combined with a start-after on either side of it.
	for _, tok := range []struct{ name, v string }{
		{"bad", "v2:!!!"}, {"unknown prefix", "zz:AA"}, {"empty v2", "v2:"}, {"bad flag", "v2:Amtl"},
		{"blank", " "}, {"empty", ""}, {"foreign", "1ueGcxLPRx1Tr/XYExHnhbYLgveDs2J/wm36Hy4vbOwM="},
		{"v1", "v1:" + "a2V5"}, {"inclusive", s3.EncodeToken([]byte("a"), true)},
	} {
		add("token "+tok.name, false, "continuation-token", tok.v, "max-keys", "5")
		add("token "+tok.name+" with start-after", false, "continuation-token", tok.v, "start-after", "a", "max-keys", "5")
	}
	// Seed pages of one entry, so any fixture of two or more keys
	// truncates and issues the token the dependent requests need; the
	// delimiter seed lists the first prefix (the root when it qualifies)
	// that rolls up to two or more entries.
	seed := delimiterSeed(keys)
	out = append(out,
		request{Name: "token page 1", Method: "GET", Query: q("list-type", "2", "max-keys", "1")},
		request{Name: "token page 2 + earlier start-after", Method: "GET", Query: q("list-type", "2", "max-keys", "3", "start-after", "\x01"), TokenFrom: "token page 1"},
		request{Name: "token page 2 + later start-after", Method: "GET", Query: q("list-type", "2", "max-keys", "3", "start-after", "\xff"), TokenFrom: "token page 1"},
		request{Name: "token page 2 + blank start-after", Method: "GET", Query: q("list-type", "2", "max-keys", "3", "start-after", ""), TokenFrom: "token page 1"},
		request{Name: "delimiter token page 1", Method: "GET", Query: q("list-type", "2", "prefix", seed, "max-keys", "1", "delimiter", "/")},
		request{Name: "delimiter token page 2", Method: "GET", Query: q("list-type", "2", "prefix", seed, "max-keys", "1", "delimiter", "/"), TokenFrom: "delimiter token page 1"},
		request{Name: "delimiter token page 2 + start-after", Method: "GET", Query: q("list-type", "2", "prefix", seed, "max-keys", "1", "delimiter", "/", "start-after", "\xff"), TokenFrom: "delimiter token page 1"},
		request{Name: "delimiter token page 2 url", Method: "GET", Query: q("list-type", "2", "prefix", seed, "max-keys", "2", "delimiter", "/", "encoding-type", "url"), TokenFrom: "delimiter token page 1"},
	)
	// Malformed and unusual requests.
	out = append(out,
		request{Name: "no list-type", Method: "GET", Query: "max-keys=1", Live: false},
		request{Name: "list-type=1", Method: "GET", Query: "list-type=1"},
		request{Name: "encoded names", Method: "GET", Query: "list%2Dtype=2&max%2Dkeys=2&pre%66ix=" + s3.PercentEncode([]byte(firstSegment(keys)))},
		request{Name: "duplicate max-keys", Method: "GET", Query: "list-type=2&max-keys=1&max-keys=2"},
		// Digits from other scripts: a BMP fullwidth one and Arabic-Indic
		// zero (a digit), and a mathematical one outside the BMP (not a
		// digit: its UTF-16 form is a surrogate pair).
		request{Name: "max-keys BMP digits", Method: "GET", Query: "list-type=2&max-keys=%EF%BC%91%D9%A0"},
		request{Name: "max-keys supplementary digit", Method: "GET", Query: "list-type=2&max-keys=%F0%9D%9F%99"},
		request{Name: "raw plus and broken escape", Method: "GET", Query: "list-type=2&prefix=a+b%2&max-keys=1"},
		request{Name: "unknown bucket", Method: "GET", Path: "/no-such-bucket-here", Query: "list-type=2"},
		request{Name: "escaped bucket", Method: "GET", Path: "/%62ucket", Query: "list-type=2"},
		request{Name: "root path", Method: "GET", Path: "/", Query: "list-type=2"},
		request{Name: "bucket with trailing segments", Method: "GET", Path: "/{bucket}/x/y", Query: "list-type=2&max-keys=2"},
		request{Name: "HEAD", Method: "HEAD", Query: "list-type=2&max-keys=2"},
		request{Name: "POST", Method: "POST", Query: "list-type=2"},
		request{Name: "DELETE unknown bucket", Method: "DELETE", Path: "/no-such-bucket-here", Query: "list-type=2"},
	)
	return append(out, rawTargets()...)
}

// rawTargets are request targets sent byte for byte: the ones the
// contract's HTTP layer refuses before routing (compared on status and
// headers), the unusual ones it lets through to the S3 route (compared in
// full), and request lines Go's HTTP parser refuses itself (status only).
// Each name carries its comparison: [headers], [full] or [status]. The
// documented residuals (docs/replay.md, Limits) are the headers and
// status cases; an absolute target whose host differs from the Host
// header is not sent, since this server cannot see the difference.
func rawTargets() []request {
	var out []request
	add := func(method, compare string, targets ...string) {
		for _, t := range targets {
			mode := compare
			if mode == "" {
				mode = "full"
			}
			out = append(out, request{Name: fmt.Sprintf("raw %s [%s] %q", method, mode, t), Method: method, Raw: t, Compare: compare})
		}
	}
	add("GET", "headers",
		"//{bucket}?list-type=2", "/{bucket}%2Fchild?list-type=2", "/{bucket}%2fchild?list-type=2", "/%2F{bucket}?list-type=2",
		"/{bucket}/x%2Fy?list-type=2", "/{bucket}//x?list-type=2", "/{bucket}/x//?list-type=2", "/;/{bucket}?list-type=2",
		"/{bucket}/;p/x?list-type=2", "/{bucket}/%2e%2e/x?list-type=2", "/{bucket}/.%2e?list-type=2", "/%2e/{bucket}?list-type=2",
		"/{bucket}/%2E?list-type=2", "/{bucket}/..;/x?list-type=2", "/../{bucket}?list-type=2", "/{bucket}/../../x?list-type=2",
		"/%2e%2e?list-type=2", "/{bucket}%00?list-type=2", "/{bucket}%25?list-type=2", "/{bucket}%5C?list-type=2",
		"/{bucket}%01?list-type=2", "/{bucket}%7F?list-type=2", "/{bucket}%0A?list-type=2", "/{bucket}\\x?list-type=2",
		"/{bucket}\xc3\xa9?list-type=2", "/{bucket}\xff?list-type=2", "/{bucket}\"?list-type=2", "/{bucket}|?list-type=2",
		"/{bucket}[?list-type=2", "/{bucket}^?list-type=2", "/{bucket}#frag?list-type=2", "/{bucket}?list-type=2#frag",
		"/{bucket}%C0%AF?list-type=2", "/{bucket}%FF?list-type=2", "/{bucket}%ED%A0%80?list-type=2", "*",
		"http://{host}//{bucket}?list-type=2", "http://{host}/{bucket}%2F?list-type=2",
		"http://user@{host}/{bucket}?list-type=2")
	add("HEAD", "headers", "//{bucket}?list-type=2", "/{bucket}%2Fx?list-type=2")
	add("POST", "headers", "//{bucket}?list-type=2")
	add("GET", "",
		"/{bucket}/../{bucket}?list-type=2&max-keys=2", "/{bucket}/.?list-type=2&max-keys=2", "/{bucket}/..?list-type=2&max-keys=2",
		"/./{bucket}?list-type=2", "/{bucket};p?list-type=2", "/;{bucket}?list-type=2", "/{bucket}/x;/?list-type=2&max-keys=2",
		"/{bucket}/%3B?list-type=2&max-keys=2", "/{bucket}/a%2Eb?list-type=2&max-keys=2", "/{bucket}%20x?list-type=2",
		"/{bucket}%C3%A9?list-type=2", "/{bucket}/%3Fx?list-type=2&max-keys=2", "/{bucket}%3F?list-type=2",
		"/{bucket}!$&'()*+,=:@~?list-type=2", "http://{host}/{bucket}?list-type=2&max-keys=2",
		"HTTP://{host}/{bucket}?list-type=2&max-keys=2", "http://{host}?list-type=2", "ftp://{host}/{bucket}?list-type=2&max-keys=2",
		"http:/{bucket}?list-type=2&max-keys=2", "/{bucket}?", "/{bucket}??list-type=2",
		"/{bucket}?list-type=2&prefix=\xc3\xa9&max-keys=2", "/{bucket}?list-type=2&prefix=\"<>`{|}^[]\\&max-keys=2",
		"/{bucket}?list-type=2&prefix=%&max-keys=2", "/{bucket}/x%20/?list-type=2&max-keys=2", "/", "/?list-type=2")
	// A declared body that never arrives: answered as without one, the
	// connection closed rather than drained.
	for _, framing := range [][2]string{{"Content-Length", "1"}, {"Transfer-Encoding", "chunked"}} {
		out = append(out, request{Name: fmt.Sprintf("raw GET [full] unsent body %s", framing[0]), Method: "GET",
			Raw: "/{bucket}?list-type=2&max-keys=2", Header: [][2]string{framing}})
	}
	// Request heads around the limits (8 KiB with S3 alone, 64 KiB with a
	// native route): a long header, a long request line, and the 70 KiB
	// header a native configuration also refuses.
	for _, n := range []int{8000, 9000, 60000, 70000} {
		pad := strings.Repeat("a", n)
		out = append(out,
			request{Name: fmt.Sprintf("raw GET [headers] %d-byte header", n), Method: "GET", Raw: "/{bucket}?list-type=2&max-keys=1",
				Header: [][2]string{{"X-Pad", pad}}, Compare: "headers"},
			request{Name: fmt.Sprintf("raw GET [headers] %d-byte request line", n), Method: "GET",
				Raw: "/{bucket}?list-type=2&max-keys=1&prefix=" + pad, Compare: "headers"})
	}
	// A head just past the S3-only limit as sent, but under it once
	// Transfer-Encoding is dropped: the limit counts the bytes on the wire.
	// (The driver sends Host: 127.0.0.1 and a five-digit port, X-Pad, then
	// Connection: close.)
	{
		fixed := len("GET /") + len("?list-type=2&max-keys=1 HTTP/1.1\r\nHost: 127.0.0.1:12345\r\nTransfer-Encoding: chunked\r\nX-Pad: \r\nConnection: close\r\n\r\n")
		out = append(out, request{Name: "raw GET [headers] chunked head past the limit as sent", Method: "GET",
			Raw: "/{bucket}?list-type=2&max-keys=1", BucketPad: 8192 + 10 - fixed,
			Header: [][2]string{{"Transfer-Encoding", "chunked"}}, Compare: "headers"})
	}
	// Heads framed the ways net/http also accepts, sized one byte past the
	// S3-only limit: a CRLF blank line after an LF-ended last header, and a
	// GET pipelined after a bodyless POST and a stray CRLFCRLF (which is
	// not counted as a head).
	getHead := func(bucket, host, ending, blank string, total int) string {
		h := func(pad string) string {
			return "GET /" + bucket + "?list-type=2&max-keys=1 HTTP/1.1\r\nHost: " + host + "\r\nConnection: close\r\nX-Pad: " + pad + ending + blank
		}
		return h(strings.Repeat("a", total-len(h(""))))
	}
	out = append(out,
		request{Name: "raw GET [headers] LF-ended last header, CRLF blank line, one byte past the limit", Method: "GET", Compare: "headers",
			Wire: func(b, h string) string { return getHead(b, h, "\n", "\r\n", 8193) }},
		request{Name: "raw GET [headers] after a bodyless POST and CRLFCRLF, one byte past the limit", Method: "GET", Compare: "headers",
			Wire: func(b, h string) string {
				return "POST /" + b + "?list-type=2 HTTP/1.1\r\nHost: " + h + "\r\nContent-Length: 0\r\n\r\n\r\n\r\n" + getHead(b, h, "\r\n", "\r\n", 8193)
			}},
		request{Name: "raw GET [full] after a bodyless POST and CRLFCRLF, at the limit", Method: "GET",
			Wire: func(b, h string) string {
				return "POST /" + b + "?list-type=2 HTTP/1.1\r\nHost: " + h + "\r\nContent-Length: 0\r\n\r\n\r\n\r\n" + getHead(b, h, "\r\n", "\r\n", 8192)
			}})
	// The two parser-level differences the user page documents, sent so
	// they show in every report: more than four CR or LF after a bodyless
	// POST (the reference serves the next request, Go's parser answers
	// 400 and closes), and a CRLF before a GET with nothing in front (the
	// reference counts it and answers 431 at the limit, Go answers 400).
	out = append(out,
		request{Name: "raw GET [residual] after a bodyless POST and five LF", Method: "GET", Compare: "residual",
			Wire: func(b, h string) string {
				return "POST /" + b + "?list-type=2 HTTP/1.1\r\nHost: " + h + "\r\nContent-Length: 0\r\n\r\n\n\n\n\n\n" +
					getHead(b, h, "\r\n", "\r\n", 200)
			}},
		request{Name: "raw GET [residual] CRLF before the request line, head at the limit", Method: "GET", Compare: "residual",
			Wire: func(b, h string) string { return "\r\n" + getHead(b, h, "\r\n", "\r\n", 8192) }})
	add("GET", "status",
		"/{bucket}%zz?list-type=2", "/{bucket}%?list-type=2", "/{bucket}%u0041?list-type=2", "{bucket}?list-type=2",
		"/{bucket}?list-type=2&prefix=a b&max-keys=1", "/{bucket}?list-type=2&prefix=\x01&max-keys=1",
		"/{bucket}?list-type=2&prefix=a\x7f&max-keys=1", "/{bucket}\t?list-type=2")
	return out
}

// s3Walks lists the token walks: the whole bucket and every top-level
// prefix, with and without the delimiter, at each page size; and, for a
// few common prefixes, walks that begin with a start-after at the prefix
// itself (the boundary of a rolled-up prefix), inside it (at its first
// key) and at its successor, at the root and inside the parent prefix.
// Every page after the first carries both the token and the start-after.
func s3Walks(keys [][]byte, pageSizes []int, maxTop int) []walk {
	var out []walk
	tops := topLevel(keys)
	if len(tops) > maxTop {
		tops = tops[:maxTop]
	}
	boundaries := boundaryPrefixes(keys, 4)
	for _, mk := range pageSizes {
		m := fmt.Sprint(mk)
		out = append(out,
			walk{"bucket max-keys=" + m, q("list-type", "2", "max-keys", m, "encoding-type", "url")},
			walk{"bucket delimiter=/ max-keys=" + m, q("list-type", "2", "delimiter", "/", "max-keys", m, "encoding-type", "url")})
		for _, t := range tops {
			out = append(out,
				walk{"prefix=" + t + " max-keys=" + m, q("list-type", "2", "prefix", t, "max-keys", m, "encoding-type", "url", "fetch-owner", "true")},
				walk{"prefix=" + t + " delimiter=/ max-keys=" + m, q("list-type", "2", "prefix", t, "delimiter", "/", "max-keys", m)})
		}
		for _, p := range boundaries {
			parent := parentPrefix(p)
			next, _ := s3.Successor([]byte(p))
			for _, sa := range []struct{ name, v string }{{"at", p}, {"inside", firstKeyUnder(keys, p)}, {"successor", string(next)}} {
				out = append(out,
					walk{"start-after " + sa.name + " " + p + " delimiter=/ max-keys=" + m,
						q("list-type", "2", "delimiter", "/", "start-after", sa.v, "max-keys", m, "encoding-type", "url")},
					walk{"prefix=" + parent + " start-after " + sa.name + " " + p + " delimiter=/ max-keys=" + m,
						q("list-type", "2", "prefix", parent, "delimiter", "/", "start-after", sa.v, "max-keys", m)})
			}
		}
	}
	return out
}

// delimiterSeed is the first of the root and the directory prefixes whose
// "/" listing has at least two entries.
func delimiterSeed(keys [][]byte) string {
	for _, p := range append([]string{""}, directoryPrefixes(keys)...) {
		seen := map[string]bool{}
		for _, k := range keys {
			if !bytes.HasPrefix(k, []byte(p)) {
				continue
			}
			rest := k[len(p):]
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

// boundaryPrefixes picks up to n directory prefixes spread over the
// keyspace, preferring ones below the top level so the parent-prefix walks
// have a parent.
func boundaryPrefixes(keys [][]byte, n int) []string {
	all := directoryPrefixes(keys)
	var deep []string
	for _, p := range all {
		if strings.Count(p, "/") > 1 {
			deep = append(deep, p)
		}
	}
	if len(deep) == 0 {
		deep = all
	}
	if len(deep) <= n {
		return deep
	}
	var out []string
	for i := range n {
		out = append(out, deep[i*len(deep)/n])
	}
	return out
}

func firstKeyUnder(keys [][]byte, p string) string {
	i, _ := slices.BinarySearchFunc(keys, []byte(p), bytes.Compare)
	if i < len(keys) && bytes.HasPrefix(keys[i], []byte(p)) {
		return string(keys[i])
	}
	return p
}

// randomRequest draws one request from the key space.
func randomRequest(keys [][]byte, rng *rand.Rand, prefixes []string) request {
	params := []string{"list-type", "2"}
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
			k = append(bytes.Clone(k), byte(rng.IntN(256)))
		}
		params = append(params, "start-after", string(k))
	}
	params = append(params, "max-keys", fmt.Sprint([]int{0, 1, 1, 2, 3, 7, 50, 1000, 1001}[rng.IntN(9)]))
	if rng.IntN(2) == 0 {
		params = append(params, "encoding-type", "url")
	}
	if rng.IntN(4) == 0 {
		params = append(params, "fetch-owner", "true")
	}
	return request{Name: "random", Method: "GET", Query: q(params...)}
}

// directoryPrefixes is every prefix of a key that ends at a '/'.
func directoryPrefixes(keys [][]byte) []string {
	set := map[string]bool{}
	for _, k := range keys {
		for i, c := range k {
			if c == '/' {
				set[string(k[:i+1])] = true
			}
		}
	}
	return slices.Sorted(func(yield func(string) bool) {
		for p := range set {
			if !yield(p) {
				return
			}
		}
	})
}

func topLevel(keys [][]byte) []string {
	var out []string
	for _, p := range directoryPrefixes(keys) {
		if strings.Count(p, "/") == 1 {
			out = append(out, p)
		}
	}
	return out
}

func parentPrefix(p string) string {
	i := strings.LastIndex(strings.TrimSuffix(p, "/"), "/")
	return p[:i+1]
}

func firstSegment(keys [][]byte) string {
	if len(keys) == 0 {
		return ""
	}
	k := keys[0]
	if i := bytes.IndexByte(k, '/'); i >= 0 {
		return string(k[:i+1])
	}
	return string(k)
}

// multiByteDelimiter is a two-byte sequence that occurs inside the keys,
// so a multi-byte delimiter actually rolls something up.
func multiByteDelimiter(keys [][]byte, rng *rand.Rand) string {
	for range 100 {
		if len(keys) == 0 {
			break
		}
		k := keys[rng.IntN(len(keys))]
		if len(k) < 4 {
			continue
		}
		i := 1 + rng.IntN(len(k)-3)
		return string(k[i : i+2])
	}
	return "/x"
}

func keysNeedingEncoding(keys [][]byte, n int) [][]byte {
	var out [][]byte
	for _, k := range keys {
		if s3.PercentEncode(k) != string(k) {
			out = append(out, k)
			if len(out) == n {
				break
			}
		}
	}
	return out
}
