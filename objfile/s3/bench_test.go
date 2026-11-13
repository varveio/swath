// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3_test

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"testing"

	"github.com/varveio/swath/objfile"
	"github.com/varveio/swath/objfile/internal/objfiletest"
	"github.com/varveio/swath/objfile/s3"
)

// BenchmarkUpload measures the upload path's cost on the client: a 64 MiB
// object (the default Parquet part size) written 4 KiB at a time, as the
// part writer writes, as a multipart upload of 8 MiB parts (the default)
// with CRC32 checksums to the in-process fake, which keeps the bytes and
// computes each part's MD5 (its ETag). b.SetBytes makes the result bytes
// per second; the allocations are the client's and the fake's together.
// Discard runs the same requests against a server that reads and drops
// every part body, to separate the fake's own work (its completion then
// fails, and is not counted).
func BenchmarkUpload(b *testing.B) {
	data := objfiletest.Data(64<<20, 9)
	for _, discard := range []bool{false, true} {
		b.Run("discard="+strconv.FormatBool(discard), func(b *testing.B) {
			var wrap func(http.Handler) http.Handler
			if discard {
				wrap = func(h http.Handler) http.Handler {
					return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if r.URL.Query().Get("partNumber") != "" {
							_, _ = io.Copy(io.Discard, r.Body) // a part: taken and dropped
							w.Header().Set("ETag", `"dropped"`)
							w.WriteHeader(http.StatusOK)
							return
						}
						h.ServeHTTP(w, r)
					})
				}
			}
			f, c := writeFake(b, false, wrap)
			f.MinPartSize = s3.MinPartSize
			w, err := s3.NewWriter(c, bucket, s3.WriteOptions{})
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
