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
// delay: prefix, delimiter, startOffset (inclusive), endOffset (exclusive),
// maxResults, and a token naming the last name or prefix returned.
func Handler(keys []string, delay time.Duration) http.Handler {
	keys = append([]string(nil), keys...)
	sort.Strings(keys)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(delay)
		q := r.URL.Query()
		if r.URL.Path != "/storage/v1/b/bkt/o" {
			http.Error(w, `{"error":{"code":404,"message":"no such bucket"}}`, 404)
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
			Items    []map[string]string `json:"items,omitempty"`
			Prefixes []string            `json:"prefixes,omitempty"`
			Next     string              `json:"nextPageToken,omitempty"`
		}
		start, end, prefix, delim := q.Get("startOffset"), q.Get("endOffset"), q.Get("prefix"), q.Get("delimiter")
		n, last := 0, ""
		for i := sort.SearchStrings(keys, max(start, prefix)); i < len(keys); i++ {
			k := keys[i]
			if !strings.HasPrefix(k, prefix) || (end != "" && k >= end) {
				break
			}
			out, isPrefix := k, false
			if delim != "" {
				if j := strings.Index(k[len(prefix):], delim); j >= 0 {
					// Names at or after startOffset roll up into their
					// prefix, even one at or below the offset (as GCS).
					out, isPrefix = k[:len(prefix)+j+len(delim)], true
				}
			}
			if out <= after || (isPrefix && out == last) {
				continue
			}
			if n == limit {
				resp.Next = base64.RawURLEncoding.EncodeToString([]byte(last))
				break
			}
			n, last = n+1, out
			if isPrefix {
				resp.Prefixes = append(resp.Prefixes, out)
				continue
			}
			resp.Items = append(resp.Items, map[string]string{"name": k, "size": strconv.Itoa(len(k)),
				"updated": "2026-01-01T00:00:00Z", "generation": strconv.Itoa(1000 + i), "etag": "e" + strconv.Itoa(i)})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
}
