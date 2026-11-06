// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package gcstest serves a minimal GCS JSON API bucket for run-level and
// command-level tests (test support; nothing in the tool imports it).
package gcstest

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Keys are n names spread over directories, as the run-level tests use.
func Keys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("d%02d/sub%d/obj+%06d", i%9, i%4, i)
	}
	return keys
}

// Bucket serves Handler(keys, delay) and returns its endpoint
// (…/storage/v1/), closed at the test's end.
func Bucket(t testing.TB, keys []string, delay time.Duration) string {
	t.Helper()
	srv := httptest.NewServer(Handler(keys, delay))
	t.Cleanup(srv.Close)
	return srv.URL + Path
}

// Path is the endpoint's path on the server.
const Path = "/storage/v1/"

// Handler serves objects.list of bucket "bkt" over keys, each request after
// delay: prefix, startOffset (inclusive), endOffset (exclusive),
// maxResults, and a token naming the last name; no delimiter (the default
// policy sends none).
func Handler(keys []string, delay time.Duration) http.Handler {
	keys = append([]string(nil), keys...)
	sort.Strings(keys)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(delay)
		q := r.URL.Query()
		if q.Get("delimiter") != "" || r.URL.Path != "/storage/v1/b/bkt/o" {
			http.Error(w, `{"error":{"code":400,"message":"unsupported"}}`, 400)
			return
		}
		after := ""
		if tok := q.Get("pageToken"); tok != "" {
			b, err := base64.RawURLEncoding.DecodeString(tok)
			if err != nil {
				http.Error(w, `{"error":{"code":400,"message":"token"}}`, 400)
				return
			}
			after = string(b)
		}
		limit := 1000
		if n, err := strconv.Atoi(q.Get("maxResults")); err == nil && n > 0 && n < limit {
			limit = n
		}
		var resp struct {
			Items []map[string]string `json:"items,omitempty"`
			Next  string              `json:"nextPageToken,omitempty"`
		}
		start, end, prefix := q.Get("startOffset"), q.Get("endOffset"), q.Get("prefix")
		for i := sort.SearchStrings(keys, max(start, prefix)); i < len(keys); i++ {
			k := keys[i]
			if !strings.HasPrefix(k, prefix) || (end != "" && k >= end) {
				break
			}
			if k <= after {
				continue
			}
			if len(resp.Items) == limit {
				resp.Next = base64.RawURLEncoding.EncodeToString([]byte(resp.Items[limit-1]["name"]))
				break
			}
			resp.Items = append(resp.Items, map[string]string{"name": k, "size": strconv.Itoa(len(k)),
				"updated": "2026-01-01T00:00:00Z", "generation": strconv.Itoa(1000 + i), "etag": "e" + strconv.Itoa(i)})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
}
