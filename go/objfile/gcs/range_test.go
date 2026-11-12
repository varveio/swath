// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"

	"github.com/varveio/swath/go/objfile"
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
				cut(w, data, start, end, 2)
				return
			}
			partial(w, data, start, end)
		}},
		// The SDK checks only the status of the request it sends for the
		// rest: without the transport the read returns another range's bytes.
		{"cut, then the rest shifted", wrong, fails, func(w http.ResponseWriter, start, end int, attempt int32) {
			if attempt == 1 {
				cut(w, data, start, end, 2)
				return
			}
			partial(w, data, start+1, end+1)
		}},
		// An object stored with Content-Encoding: gzip, read as stored: the
		// SDK requests the rest itself and skips as many bytes of that
		// answer as it had read, so without the transport (which ends the
		// cut body instead) the read joins two parts of the object.
		{"gzip, cut at a quarter, then the rest", wrong, exact, func(w http.ResponseWriter, start, end int, attempt int32) {
			w.Header().Set("Content-Encoding", "gzip")
			if attempt == 1 {
				cut(w, data, start, end, 4)
				return
			}
			partial(w, data, start, end)
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

// cut writes the first 1/part of the range's body, then breaks the
// connection.
func cut(w http.ResponseWriter, data []byte, start, end, part int) {
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
	w.Header().Set("Content-Length", strconv.Itoa(end-start+1))
	w.WriteHeader(http.StatusPartialContent)
	_, _ = w.Write(data[start : start+(end-start+1)/part])
	w.(http.Flusher).Flush()
	panic(http.ErrAbortHandler)
}

// wholeServer serves one object, "obj" at generation 7, gzip-labelled: a
// read of the whole object is answered by whole (with the attempt number);
// a ranged read, closed or to the end, by a 206 of that range.
func wholeServer(t *testing.T, data []byte, whole func(w http.ResponseWriter, r *http.Request, attempt int32)) string {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/storage/v1/") {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"bucket": bucket, "name": "obj", "size": strconv.Itoa(len(data)), "generation": "7"})
			return
		}
		w.Header().Set("X-Goog-Generation", "7")
		w.Header().Set("Content-Encoding", "gzip")
		rng := r.Header.Get("Range")
		if rng == "" {
			whole(w, r, attempts.Add(1))
			return
		}
		start, end := 0, len(data)-1
		if open, ok := strings.CutSuffix(rng, "-"); ok {
			_, _ = fmt.Sscanf(open, "bytes=%d", &start)
		} else {
			_, _ = fmt.Sscanf(rng, "bytes=%d-%d", &start, &end)
		}
		partial(w, data, start, end)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func wholeClient(t *testing.T, url string, guard bool) *storage.Client {
	opts := []option.ClientOption{option.WithEndpoint(url + "/storage/v1/"), option.WithoutAuthentication()}
	if guard {
		opts = append(opts, option.WithHTTPClient(&http.Client{Transport: ranged.Transport(http.DefaultTransport)}))
	}
	client, err := storage.NewClient(context.Background(), opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// TestReadObjectCut: a whole-object read of a gzip-labelled object whose
// body is cut at a quarter. The SDK requests the rest itself and skips as
// many bytes of that answer as it had read, joining two parts of the
// object; with the transport the body ends where it was cut instead, and
// ReadObject reads the rest as a checked ranged read.
func TestReadObjectCut(t *testing.T) {
	data := objfiletest.Data(10000, 4) // a quarter is not a multiple of Data's period
	for _, guard := range []bool{false, true} {
		url := wholeServer(t, data, func(w http.ResponseWriter, _ *http.Request, attempt int32) {
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			if attempt == 1 {
				_, _ = w.Write(data[:len(data)/4])
				w.(http.Flusher).Flush()
				panic(http.ErrAbortHandler)
			}
			_, _ = w.Write(data)
		})
		b, id, err := gcs.New(wholeClient(t, url, guard), bucket).(objfile.ObjectReader).ReadObject(context.Background(), "obj", 1<<20)
		exact := err == nil && bytes.Equal(b, data)
		if guard && (!exact || id != "7") {
			t.Fatalf("guarded ReadObject of a cut body: %d bytes, %q, %v; want the object", len(b), id, err)
		}
		if !guard && exact {
			t.Fatal("unguarded ReadObject of a cut gzip body read the object: the SDK no longer joins parts, so the transport may not be needed")
		}
	}
}

// TestReadObjectChunked: a whole-object answer without Content-Length is
// read to its end, within the limit.
func TestReadObjectChunked(t *testing.T) {
	data := objfiletest.Data(5000, 5)
	url := wholeServer(t, data, func(w http.ResponseWriter, r *http.Request, _ int32) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush() // headers sent with no length: the body is chunked
		_, _ = w.Write(data)
	})
	o := gcs.New(wholeClient(t, url, true), bucket).(objfile.ObjectReader)
	if b, _, err := o.ReadObject(context.Background(), "obj", 1<<20); err != nil || !bytes.Equal(b, data) {
		t.Fatalf("ReadObject of a chunked answer = %d bytes, %v; want %d", len(b), err, len(data))
	}
	if _, _, err := o.ReadObject(context.Background(), "obj", 4999); !errors.Is(err, objfile.ErrTooLarge) {
		t.Fatalf("ReadObject of a chunked answer over the limit = %v", err)
	}

	// A chunked body broken part way, or cancelled part way, is a failure,
	// not the object: its end is not the object's end. (Without the
	// transport the SDK requests the rest itself, as the package says.)
	for _, c := range []struct {
		name   string
		cancel bool
	}{{"aborted", false}, {"cancelled", true}} {
		ctx, cancel := context.WithCancel(context.Background())
		url := wholeServer(t, data, func(w http.ResponseWriter, r *http.Request, _ int32) {
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			_, _ = w.Write(data[:1000])
			w.(http.Flusher).Flush()
			if c.cancel {
				cancel()
				<-r.Context().Done() // until the client has gone
			}
			panic(http.ErrAbortHandler)
		})
		b, _, err := gcs.New(wholeClient(t, url, true), bucket).(objfile.ObjectReader).ReadObject(ctx, "obj", 1<<20)
		if err == nil {
			t.Fatalf("%s chunked answer: ReadObject = %d bytes, nil; want an error", c.name, len(b))
		}
		if c.cancel && !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled chunked answer: %v, want context.Canceled", err)
		}
		cancel()
	}
}
