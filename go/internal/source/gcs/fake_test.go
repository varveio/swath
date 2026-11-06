// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

// fakeGCS serves objects.list over keys (sorted by bytes) as the JSON API
// defines it: prefix, delimiter, startOffset (inclusive), endOffset
// (exclusive), pageToken, maxResults (default and cap 1,000). The token
// holds the last name or prefix returned and the query that started its
// chain: a token sent with other parameters is a 400, as a service that
// binds tokens to their request may answer. With empty set, every empty-th
// page is an empty page with a token (the API documents them). Sizes are
// strings, as on the wire.
type fakeGCS struct {
	keys     []string
	empty    int64
	requests atomic.Int64
	empties  atomic.Int64
	last     atomic.Value // the last request's query
}

// chainOf is the query a token's chain started with.
func chainOf(q url.Values) string {
	return strings.Join([]string{q.Get("prefix"), q.Get("delimiter"), q.Get("startOffset"), q.Get("endOffset"), q.Get("maxResults")}, "\x00")
}

func newFake(t testing.TB, keys []string) (*fakeGCS, string) {
	f := &fakeGCS{keys: append([]string(nil), keys...)}
	sort.Strings(f.keys)
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return f, srv.URL + "/storage/v1/"
}

func (f *fakeGCS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.requests.Add(1)
	q := r.URL.Query()
	f.last.Store(q)
	bucket, ok := strings.CutPrefix(r.URL.Path, "/storage/v1/b/")
	if !ok || !strings.HasSuffix(bucket, "/o") {
		http.Error(w, `{"error":{"code":404,"message":"no route"}}`, 404)
		return
	}
	prefix, delim, start, end, token := q.Get("prefix"), q.Get("delimiter"), q.Get("startOffset"), q.Get("endOffset"), q.Get("pageToken")
	if token != "" {
		raw, err := base64.RawURLEncoding.DecodeString(token)
		parts := strings.SplitN(string(raw), "\x01", 3)
		if err != nil || len(parts) != 3 || parts[0] != chainOf(q) {
			http.Error(w, `{"error":{"code":400,"message":"Invalid argument: the page token does not match the request"}}`, 400)
			return
		}
		token = parts[1]
	}
	next := func(after, nonce string) string {
		return base64.RawURLEncoding.EncodeToString([]byte(chainOf(q) + "\x01" + after + "\x01" + nonce))
	}
	if serial := f.requests.Load(); f.empty > 0 && serial%f.empty == 0 {
		// An empty page that still makes progress: a new token that
		// continues from the request's own position.
		f.empties.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"nextPageToken":"` + next(token, strconv.FormatInt(serial, 10)) + `"}`))
		return
	}
	max := 1000
	if s := q.Get("maxResults"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 && n < max {
			max = n
		}
	}
	type item struct {
		Name    string `json:"name"`
		Size    string `json:"size"`
		Updated string `json:"updated"`
		ETag    string `json:"etag"`
		Gen     string `json:"generation"`
	}
	var resp struct {
		Items    []item   `json:"items,omitempty"`
		Prefixes []string `json:"prefixes,omitempty"`
		Next     string   `json:"nextPageToken,omitempty"`
	}
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
			resp.Next = next(lastOut, "")
			break
		}
		n++
		lastOut = out
		if isPrefix {
			resp.Prefixes = append(resp.Prefixes, out)
		} else {
			resp.Items = append(resp.Items, item{Name: k, Size: strconv.Itoa(len(k)), Updated: "2026-11-06T01:02:03.456789Z", ETag: "e" + strconv.Itoa(i), Gen: "17"})
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}
