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
	// NextContinuationToken is appended to this one's query.
	TokenFrom string `json:"-"`
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
	out = append(out,
		request{Name: "token page 1", Method: "GET", Query: q("list-type", "2", "max-keys", "3")},
		request{Name: "token page 2 + earlier start-after", Method: "GET", Query: q("list-type", "2", "max-keys", "3", "start-after", "\x01"), TokenFrom: "token page 1"},
		request{Name: "token page 2 + later start-after", Method: "GET", Query: q("list-type", "2", "max-keys", "3", "start-after", "\xff"), TokenFrom: "token page 1"},
		request{Name: "delimiter token page 1", Method: "GET", Query: q("list-type", "2", "max-keys", "1", "delimiter", "/")},
		request{Name: "delimiter token page 2", Method: "GET", Query: q("list-type", "2", "max-keys", "1", "delimiter", "/"), TokenFrom: "delimiter token page 1"},
	)
	// Malformed and unusual requests.
	out = append(out,
		request{Name: "no list-type", Method: "GET", Query: "max-keys=1", Live: false},
		request{Name: "list-type=1", Method: "GET", Query: "list-type=1"},
		request{Name: "encoded names", Method: "GET", Query: "list%2Dtype=2&max%2Dkeys=2&pre%66ix=" + s3.PercentEncode([]byte(firstSegment(keys)))},
		request{Name: "duplicate max-keys", Method: "GET", Query: "list-type=2&max-keys=1&max-keys=2"},
		request{Name: "raw plus and broken escape", Method: "GET", Query: "list-type=2&prefix=a+b%2&max-keys=1"},
		request{Name: "unknown bucket", Method: "GET", Path: "/no-such-bucket-here", Query: "list-type=2"},
		request{Name: "escaped bucket", Method: "GET", Path: "/%62ucket", Query: "list-type=2"},
		request{Name: "root path", Method: "GET", Path: "/", Query: "list-type=2"},
		request{Name: "bucket with trailing segments", Method: "GET", Path: "/{bucket}/x/y", Query: "list-type=2&max-keys=2"},
		request{Name: "HEAD", Method: "HEAD", Query: "list-type=2&max-keys=2"},
		request{Name: "POST", Method: "POST", Query: "list-type=2"},
		request{Name: "DELETE unknown bucket", Method: "DELETE", Path: "/no-such-bucket-here", Query: "list-type=2"},
	)
	return out
}

// s3Walks lists the token walks: the whole bucket and every top-level
// prefix, with and without the delimiter, at each page size.
func s3Walks(keys [][]byte, pageSizes []int, maxTop int) []walk {
	var out []walk
	tops := topLevel(keys)
	if len(tops) > maxTop {
		tops = tops[:maxTop]
	}
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
	}
	return out
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
