// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob_test

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"testing"

	"github.com/varveio/swath/go/objfile"
	"github.com/varveio/swath/go/objfile/azblob"
	"github.com/varveio/swath/go/objfile/internal/objfiletest"
)

// BenchmarkUpload measures the upload path's cost on the client: a 64 MiB
// object (the default Parquet part size) written 4 KiB at a time, as the
// part writer writes, staged in 8 MiB blocks (the default) with CRC64
// checksums to the in-process fake, which keeps the bytes. b.SetBytes
// makes the result bytes per second; the allocations are the client's and
// the fake's together. Discard runs the same requests against a server
// that reads and drops every block body, to separate the fake's own work
// (its commit then fails, and is not counted).
func BenchmarkUpload(b *testing.B) {
	data := objfiletest.Data(64<<20, 9)
	for _, discard := range []bool{false, true} {
		b.Run("discard="+strconv.FormatBool(discard), func(b *testing.B) {
			var wrap func(http.Handler) http.Handler
			if discard {
				wrap = func(h http.Handler) http.Handler {
					return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if r.URL.Query().Get("comp") == "block" {
							_, _ = io.Copy(io.Discard, r.Body) // a block: taken and dropped
							w.WriteHeader(http.StatusCreated)
							return
						}
						h.ServeHTTP(w, r)
					})
				}
			}
			f, c := fakeClient(b, wrap)
			w, err := azblob.NewWriter(c, containerName, azblob.WriteOptions{})
			if err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				u, err := w.NewWriter(context.Background(), "obj"+strconv.Itoa(i), objfile.CreateOnly)
				if err != nil {
					b.Fatal(err)
				}
				for off := 0; off < len(data); off += 4096 {
					if _, err := u.Write(data[off : off+4096]); err != nil {
						b.Fatal(err)
					}
				}
				if err := u.Close(); err != nil && !discard {
					b.Fatal(err)
				}
				f.Remove("obj" + strconv.Itoa(i))
			}
		})
	}
}
