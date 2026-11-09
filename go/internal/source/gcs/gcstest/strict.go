// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcstest

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
	"time"
)

// Strict serves objects.list over keys (sorted by bytes) as the JSON API
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
// as strings, as on the wire, and every response carries the envelope's
// kind (storage#objects) unless a fields selector leaves it out.
type Strict struct {
	keys  []string
	Empty int64
	Short bool
	Cap   int
	// ShortIf, when set, cuts the pages of the requests it selects to one
	// entry (a service may cut any page short).
	ShortIf func(url.Values) bool
	// Delay is waited before each answer.
	Delay time.Duration
	// Lean sends objects with only name, size, updated, generation and
	// etag: the protocol is the same, and a run-level test that does not
	// read the rest of the metadata lists and encodes less.
	Lean bool
	// Resource, when set, is the object resource sent for key k (the
	// i-th in byte order) in place of Object's: a test that needs
	// particular metadata, such as an object without an MD5, states it.
	// It returns a new map on each call: the fields selector trims it.
	Resource func(i int, k string) map[string]any
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

// NewStrict is the strict fake over keys, in any order.
func NewStrict(keys []string) *Strict {
	f := &Strict{keys: append([]string(nil), keys...), tokens: map[string]position{}}
	_, _ = rand.Read(f.secret[:])
	sort.Strings(f.keys)
	return f
}

// Serve serves f and returns its endpoint (…/storage/v1/), closed at the
// test's end.
func (f *Strict) Serve(t testing.TB) string {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return srv.URL + Path
}

// Requests is the number of requests received; Empties the empty pages
// sent; Last the last request's query.
func (f *Strict) Requests() int64 { return f.requests.Load() }
func (f *Strict) Empties() int64  { return f.empties.Load() }
func (f *Strict) Last() url.Values {
	q, _ := f.last.Load().(url.Values)
	return q
}

// issue returns the opaque token for pos: a keyed hash, so one position
// always gets the same token (the oracle compares tokens) and nothing in
// it can be parsed back.
func (f *Strict) issue(pos position) string {
	h := sha256.Sum256([]byte(string(f.secret[:]) + pos.chain + "\x00" + pos.after + "\x00" + pos.nonce))
	t := "CgR" + hex.EncodeToString(h[:12])
	f.mu.Lock()
	f.tokens[t] = pos
	f.mu.Unlock()
	return t
}

// Sent is every query received so far.
func (f *Strict) Sent() []url.Values {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]url.Values(nil), f.queries...)
}

// Object is the objects resource as the JSON API sends it, with fields the
// listing does not read.
func Object(i int, k string) map[string]any {
	o := leanObject(i, k)
	for name, v := range map[string]any{
		"kind": "storage#object", "id": "bkt/" + k + "/17", "selfLink": "https://example.invalid/" + url.PathEscape(k),
		"bucket": "bkt", "timeCreated": created(i),
		"metageneration": strconv.Itoa(1 + i%3), "storageClass": []string{"STANDARD", "NEARLINE", "ARCHIVE"}[i%3],
		"md5Hash": "md5-" + strconv.Itoa(i) + "==", "crc32c": "c" + strconv.Itoa(i), "contentType": "application/x-" + strconv.Itoa(i%5),
	} {
		o[name] = v
	}
	return o
}

// created is the i-th object's timeCreated: before its updated time, a
// distinct instant per object, in the millisecond form GCS writes.
func created(i int) string {
	t := time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC).Add(time.Duration(i) * 1001 * time.Millisecond)
	return t.Format("2006-01-02T15:04:05.000Z07:00")
}

// leanObject is Object's listing fields: name, size, updated, generation
// and etag.
func leanObject(i int, k string) map[string]any {
	return map[string]any{"name": k, "size": strconv.Itoa(len(k)), "updated": "2026-11-06T01:02:03.456789Z",
		"etag": "e" + strconv.Itoa(i), "generation": strconv.Itoa(1700000000000000 + i)}
}

func (f *Strict) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	time.Sleep(f.Delay)
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
	resp := map[string]any{"kind": "storage#objects"}
	if f.Empty > 0 && serial%f.Empty == 0 {
		// An empty page that still makes progress: a new token that
		// continues from the request's own position.
		f.empties.Add(1)
		resp["nextPageToken"] = f.issue(position{chainOf(q), token, strconv.FormatInt(serial, 10)})
		f.write(w, q, resp)
		return
	}
	limit := 1000
	if f.Cap > 0 {
		limit = f.Cap
	}
	if s := q.Get("maxResults"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 && n < limit {
			limit = n
		}
	}
	if f.Short {
		limit = 1 + int(serial)%limit
	}
	if f.ShortIf != nil && f.ShortIf(q) {
		limit = 1
	}
	var items []any
	var prefixes []string
	n, lastOut := 0, ""
	// Names below the prefix, startOffset or the token's position are
	// never listed (a name's rolled-up prefix is not above the name), so
	// the scan starts at the greatest of them: a request costs its page,
	// not the keyspace before it.
	for i := sort.SearchStrings(f.keys, max(prefix, start, token)); i < len(f.keys); i++ {
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
		if n == limit {
			resp["nextPageToken"] = f.issue(position{chainOf(q), lastOut, ""})
			break
		}
		n++
		lastOut = out
		if isPrefix {
			prefixes = append(prefixes, out)
		} else {
			items = append(items, f.object(i, k))
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
func (f *Strict) write(w http.ResponseWriter, q url.Values, resp map[string]any) {
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

// object is key i (k) as this fake sends it: Resource's, Object, or with
// Lean its listing fields only.
func (f *Strict) object(i int, k string) map[string]any {
	if f.Resource != nil {
		return f.Resource(i, k)
	}
	if f.Lean {
		return leanObject(i, k)
	}
	return Object(i, k)
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
