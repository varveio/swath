// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// fakeGCS serves objects.list over keys (sorted by bytes) as the JSON API
// defines it: prefix, delimiter, startOffset (inclusive), endOffset
// (exclusive), pageToken, maxResults (default and cap 1,000), and a
// partial-response fields selector. Page tokens are opaque (random), bound
// on the server to the query that started their chain: a token sent with
// other parameters, or one never issued, is a 400, as a service that binds
// tokens to their request may answer. With empty set, every empty-th page
// is an empty page with a token (the API documents them); with short set,
// nonempty pages are cut short at varying lengths; cap lowers the page
// size limit; shortIf cuts the selected requests' pages to one entry.
// Every object carries the full metadata resource, sizes and generations
// as strings, as on the wire.
type fakeGCS struct {
	keys  []string
	empty int64
	short bool
	cap   int
	// shortIf, when set, cuts the pages of the requests it selects to one
	// entry (a service may cut any page short).
	shortIf  func(url.Values) bool
	requests atomic.Int64
	empties  atomic.Int64
	last     atomic.Value // the last request's query

	secret  [16]byte
	mu      sync.Mutex
	tokens  map[string]position
	queries []url.Values
}

// position is where an issued token continues: after the last name or
// prefix returned, in the chain started by query; nonce tells apart the
// tokens of empty pages at one position.
type position struct{ chain, after, nonce string }

// chainOf is the query a token's chain started with.
func chainOf(q url.Values) string {
	b, _ := json.Marshal([]string{q.Get("prefix"), q.Get("delimiter"), q.Get("startOffset"), q.Get("endOffset"), q.Get("maxResults")})
	return string(b)
}

func newFake(t testing.TB, keys []string) (*fakeGCS, string) {
	f := &fakeGCS{keys: append([]string(nil), keys...), tokens: map[string]position{}}
	_, _ = rand.Read(f.secret[:])
	sort.Strings(f.keys)
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return f, srv.URL + "/storage/v1/"
}

// issue returns the opaque token for pos: a keyed hash, so one position
// always gets the same token (the oracle compares tokens) and nothing in
// it can be parsed back.
func (f *fakeGCS) issue(pos position) string {
	h := sha256.Sum256([]byte(string(f.secret[:]) + pos.chain + "\x00" + pos.after + "\x00" + pos.nonce))
	t := "CgR" + hex.EncodeToString(h[:12])
	f.mu.Lock()
	f.tokens[t] = pos
	f.mu.Unlock()
	return t
}

// sent is every query received so far.
func (f *fakeGCS) sent() []url.Values {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]url.Values(nil), f.queries...)
}

// object is the objects resource as the JSON API sends it, with fields the
// listing does not read.
func object(i int, k string) map[string]any {
	return map[string]any{
		"kind": "storage#object", "id": "bkt/" + k + "/17", "selfLink": "https://example.invalid/" + url.PathEscape(k),
		"name": k, "bucket": "bkt", "size": strconv.Itoa(len(k)), "updated": "2026-11-06T01:02:03.456789Z",
		"timeCreated": "2026-11-05T00:00:00Z", "etag": "e" + strconv.Itoa(i), "generation": strconv.Itoa(1700000000000000 + i),
		"metageneration": strconv.Itoa(1 + i%3), "storageClass": []string{"STANDARD", "NEARLINE", "ARCHIVE"}[i%3],
		"md5Hash": "md5-" + strconv.Itoa(i) + "==", "crc32c": "c" + strconv.Itoa(i), "contentType": "application/x-" + strconv.Itoa(i%5),
	}
}

func (f *fakeGCS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	serial := f.requests.Add(1)
	q := r.URL.Query()
	f.last.Store(q)
	f.mu.Lock()
	f.queries = append(f.queries, q)
	f.mu.Unlock()
	bucket, ok := strings.CutPrefix(r.URL.Path, "/storage/v1/b/")
	if !ok || !strings.HasSuffix(bucket, "/o") {
		http.Error(w, `{"error":{"code":404,"message":"no route"}}`, 404)
		return
	}
	prefix, delim, start, end, token := q.Get("prefix"), q.Get("delimiter"), q.Get("startOffset"), q.Get("endOffset"), q.Get("pageToken")
	if token != "" {
		f.mu.Lock()
		pos, ok := f.tokens[token]
		f.mu.Unlock()
		if !ok || pos.chain != chainOf(q) {
			http.Error(w, `{"error":{"code":400,"message":"Invalid argument: the page token does not match the request"}}`, 400)
			return
		}
		token = pos.after
	}
	resp := map[string]any{}
	if f.empty > 0 && serial%f.empty == 0 {
		// An empty page that still makes progress: a new token that
		// continues from the request's own position.
		f.empties.Add(1)
		resp["nextPageToken"] = f.issue(position{chainOf(q), token, strconv.FormatInt(serial, 10)})
		f.write(w, q, resp)
		return
	}
	max := 1000
	if f.cap > 0 {
		max = f.cap
	}
	if s := q.Get("maxResults"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 && n < max {
			max = n
		}
	}
	if f.short {
		max = 1 + int(serial)%max
	}
	if f.shortIf != nil && f.shortIf(q) {
		max = 1
	}
	var items []any
	var prefixes []string
	n, lastOut := 0, ""
	for i := sort.SearchStrings(f.keys, prefix); i < len(f.keys); i++ {
		k := f.keys[i]
		if !strings.HasPrefix(k, prefix) {
			break
		}
		if k < start || (end != "" && k >= end) || (token != "" && k <= token) {
			continue
		}
		out, isPrefix := k, false
		if delim != "" {
			if j := strings.Index(k[len(prefix):], delim); j >= 0 {
				out, isPrefix = k[:len(prefix)+j+len(delim)], true
				if out == lastOut || (token != "" && out <= token) {
					continue
				}
			}
		}
		if n == max {
			resp["nextPageToken"] = f.issue(position{chainOf(q), lastOut, ""})
			break
		}
		n++
		lastOut = out
		if isPrefix {
			prefixes = append(prefixes, out)
		} else {
			items = append(items, object(i, k))
		}
	}
	if items != nil {
		resp["items"] = items
	}
	if prefixes != nil {
		resp["prefixes"] = prefixes
	}
	f.write(w, q, resp)
}

// write sends resp, reduced to the fields selector when the request has
// one (fields=a,b,items(c,d)).
func (f *fakeGCS) write(w http.ResponseWriter, q url.Values, resp map[string]any) {
	if sel := q.Get("fields"); sel != "" {
		top, sub := parseFields(sel)
		for k := range resp {
			if !top[k] {
				delete(resp, k)
			}
		}
		if items, ok := resp["items"].([]any); ok {
			for _, it := range items {
				for k := range it.(map[string]any) {
					if !sub["items"][k] {
						delete(it.(map[string]any), k)
					}
				}
			}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// parseFields reads a one-level partial-response selector.
func parseFields(sel string) (top map[string]bool, sub map[string]map[string]bool) {
	top, sub = map[string]bool{}, map[string]map[string]bool{}
	depth, from := 0, 0
	field := func(s string) {
		name, inner, nested := strings.Cut(s, "(")
		top[name] = true
		if nested {
			sub[name] = map[string]bool{}
			for _, x := range strings.Split(strings.TrimSuffix(inner, ")"), ",") {
				sub[name][x] = true
			}
		}
	}
	for i, c := range sel {
		switch c {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				field(sel[from:i])
				from = i + 1
			}
		}
	}
	field(sel[from:])
	return top, sub
}
