// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs_test

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"testing"

	"github.com/varveio/swath/objfile"
	"github.com/varveio/swath/objfile/gcs"
	"github.com/varveio/swath/objfile/internal/objfiletest"
)

// BenchmarkUpload measures the upload path's cost on the client: a 64 MiB
// object (the default part size) written 4 KiB at a time, as the part
// writer writes, through a resumable session of 8 MiB chunks (the
// default) to the in-process fake, which keeps the bytes (one copy) and
// checks the CRC32C. b.SetBytes makes the result bytes per second; the
// allocations are the client's and the fake's together. Discard runs the
// same requests against a server that reads and drops every body, to
// separate the fake's own copy.
func BenchmarkUpload(b *testing.B) {
	data := objfiletest.Data(64<<20, 9)
	for _, discard := range []bool{false, true} {
		b.Run("discard="+strconv.FormatBool(discard), func(b *testing.B) {
			wrap := plain
			if discard {
				wrap = func(h http.Handler) http.Handler {
					return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if r.URL.Query().Get("upload_id") != "" && r.Header.Get("Content-Range") != "" && r.Header.Get("Content-Range")[len(r.Header.Get("Content-Range"))-1] == '*' {
							_, _ = io.Copy(io.Discard, r.Body) // a middle chunk: taken and dropped
							w.Header().Set("X-Http-Status-Code-Override", "308")
							w.WriteHeader(http.StatusOK)
							return
						}
						h.ServeHTTP(w, r)
					})
				}
			}
			f, c := client(b, wrap)
			w := gcs.NewWriter(c, bucket, gcs.WriteOptions{})
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
