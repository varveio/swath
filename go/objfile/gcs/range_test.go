// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"

	"github.com/varveio/swath/go/objfile/gcs"
	"github.com/varveio/swath/go/objfile/internal/objfiletest"
	"github.com/varveio/swath/go/objfile/internal/ranged"
)

// misserve serves one object, "obj" at generation 7, answering its ranged
// reads with serve, which is told the requested bytes and the attempt.
func misserve(t *testing.T, data []byte, serve func(w http.ResponseWriter, start, end int, attempt int32)) string {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/storage/v1/") {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"bucket": bucket, "name": "obj", "size": strconv.Itoa(len(data)), "generation": "7"})
			return
		}
		var start, end int
		if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil {
			http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		w.Header().Set("X-Goog-Generation", "7")
		serve(w, start, end, attempts.Add(1))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// partial writes data[first:last+1] as a 206 stating that range.
func partial(w http.ResponseWriter, data []byte, first, last int) {
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", first, last, len(data)))
	w.Header().Set("Content-Length", strconv.Itoa(last-first+1))
	w.WriteHeader(http.StatusPartialContent)
	_, _ = w.Write(data[first : last+1])
}

// TestServedRange: a read returns exactly the requested bytes of the pinned
// object or fails, when the store ignores the range or serves another one,
// on the first request (gcs.New, which checks the reader's start offset) and
// on the request the SDK's reader sends on its own for the rest of a body cut
// short (ranged.Transport on the client, as remote.Open builds it; varve's
// TestGCSClientRejectsUnboundedResponses and rangeTransport).
func TestServedRange(t *testing.T) {
	data := objfiletest.Data(1<<16, 3)
	const off, n = 1000, 4000
	const exact, fails, wrong = "exact", "fails", "wrong bytes"
	for _, c := range []struct {
		name string
		// unguarded and guarded are the outcome without and with the
		// transport.
		unguarded, guarded string
		serve              func(w http.ResponseWriter, start, end int, attempt int32)
	}{
		{"exact", exact, exact, func(w http.ResponseWriter, start, end int, _ int32) { partial(w, data, start, end) }},
		{"ignored", fails, fails, func(w http.ResponseWriter, _, _ int, _ int32) {
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			_, _ = w.Write(data)
		}},
		{"shifted", fails, fails, func(w http.ResponseWriter, start, end int, _ int32) { partial(w, data, start+1, end+1) }},
		{"cut, then the rest", exact, exact, func(w http.ResponseWriter, start, end int, attempt int32) {
			if attempt == 1 {
				cut(w, data, start, end)
				return
			}
			partial(w, data, start, end)
		}},
		// The SDK checks only the status of the request it sends for the
		// rest: without the transport the read returns another range's bytes.
		{"cut, then the rest shifted", wrong, fails, func(w http.ResponseWriter, start, end int, attempt int32) {
			if attempt == 1 {
				cut(w, data, start, end)
				return
			}
			partial(w, data, start+1, end+1)
		}},
	} {
		for _, guard := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/guarded=%t", c.name, guard), func(t *testing.T) {
				url := misserve(t, data, c.serve)
				opts := []option.ClientOption{option.WithEndpoint(url + "/storage/v1/"), option.WithoutAuthentication()}
				if guard {
					opts = append(opts, option.WithHTTPClient(&http.Client{Transport: ranged.Transport(http.DefaultTransport)}))
				}
				client, err := storage.NewClient(context.Background(), opts...)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = client.Close() })
				f, err := gcs.New(client, bucket).Open(context.Background(), "obj")
				if err != nil {
					t.Fatal(err)
				}
				p := make([]byte, n)
				_, err = f.ReadAt(p, off)
				got := fails
				if err == nil {
					got = wrong
					if bytes.Equal(p, data[off:off+n]) {
						got = exact
					}
				}
				want := c.unguarded
				if guard {
					want = c.guarded
				}
				if got != want {
					t.Fatalf("read: %s (%v), want %s", got, err, want)
				}
			})
		}
	}
}

// cut writes the first half of the range's body, then breaks the connection.
func cut(w http.ResponseWriter, data []byte, start, end int) {
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
	w.Header().Set("Content-Length", strconv.Itoa(end-start+1))
	w.WriteHeader(http.StatusPartialContent)
	_, _ = w.Write(data[start : start+(end-start+1)/2])
	w.(http.Flusher).Flush()
	panic(http.ErrAbortHandler)
}
