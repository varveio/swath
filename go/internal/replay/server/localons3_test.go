// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/listing"
	"github.com/varveio/swath/go/internal/replay/store"
	"github.com/varveio/swath/go/internal/source/localfs"
)

// recordLocal writes files (relative path to body) under a temporary
// tree, sets every file's and directory's modification time to mtime,
// lists the tree with the Go lister and returns the swath-local/v1
// dataset it wrote.
func recordLocal(t *testing.T, files map[string]string, mtime time.Time) string {
	t.Helper()
	root := t.TempDir()
	tree := filepath.Join(root, "tree")
	var paths []string
	for name, body := range files {
		path := filepath.Join(tree, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		for p := path; p != tree; p = filepath.Dir(p) {
			paths = append(paths, p)
		}
	}
	// Files first, then their directories, whose times the writes moved.
	for _, path := range paths {
		if err := os.Chtimes(path, mtime, mtime); err != nil {
			t.Fatal(err)
		}
	}
	out := filepath.Join(root, "ds")
	if _, err := listing.Run(context.Background(), listing.Config{
		Source: listing.SourceConfig{URI: tree, Options: &localfs.Options{}},
		Output: listing.OutputConfig{Dir: out},
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

// serveRecording serves dir on the S3, GCS and Azure routes.
func serveRecording(t *testing.T, dir, schemaID string, opts store.Options) *Server {
	t.Helper()
	st, meta, err := store.Open(context.Background(), dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	if meta.SchemaID != schemaID {
		t.Fatalf("schema %q, want %q", meta.SchemaID, schemaID)
	}
	cfg := config(st)
	cfg.Protocols, cfg.AzureAccount = []string{"s3", "gcs", ProtocolAzure}, DefaultAzureAccount
	return start(t, cfg)
}

// s3Walk lists the bucket with query on the S3 route, following the
// continuation tokens, and returns every Contents element.
func s3Walk(t *testing.T, s *Server, query string) []string {
	t.Helper()
	var got []string
	for token := ""; ; {
		q := "/bucket?list-type=2" + query
		if token != "" {
			q += "&continuation-token=" + token
		}
		r := get(t, s, q)
		if r.status != 200 {
			t.Fatalf("%s: %d %s", q, r.status, r.body)
		}
		got = append(got, contentsRe.FindAllString(r.body, -1)...)
		m := regexp.MustCompile(`<NextContinuationToken>([^<]*)<`).FindStringSubmatch(r.body)
		if m == nil {
			return got
		}
		token = m[1]
	}
}

// TestLocalRecordingOnTheS3Route serves a recording of a local tree on the
// S3 route and requires each Contents element as docs/replay.md's route
// table states it: the key, the modification time in milliseconds and
// the size mapped; the ETag the placeholder swath-s3-synthetic-v1 and the
// StorageClass STANDARD, which S3 lists every object with; no Owner with
// fetch-owner=true (a local tree has nothing an S3 owner id maps from);
// with the window cache and without it, flat, paged and rolled up. The
// GCS and Azure routes answer the same rows unchanged. Before, the S3
// route sent no ETag and no StorageClass.
func TestLocalRecordingOnTheS3Route(t *testing.T) {
	mtime := time.Date(2025, 11, 6, 3, 33, 20, 123456789, time.UTC)
	dir := recordLocal(t, map[string]string{"a/b/x": "x", "a/z.txt": "zzz", "top.txt": "", "c&d<e>.bin": "1234"}, mtime)
	contents := func(key string, size string) string {
		return "<Contents><Key>" + key + "</Key><LastModified>2025-11-06T03:33:20.123Z</LastModified>" +
			"<ETag>&quot;swath-s3-synthetic-v1&quot;</ETag><Size>" + size + "</Size><StorageClass>STANDARD</StorageClass></Contents>"
	}
	// Directories are rows of a local recording, with their own size.
	dirSize := func(key string) string {
		info, err := os.Stat(filepath.Join(filepath.Dir(dir), "tree", filepath.FromSlash(key)))
		if err != nil {
			t.Fatal(err)
		}
		return strconv.FormatInt(info.Size(), 10)
	}
	all := []string{
		contents("a/", dirSize("a")),
		contents("a/b/", dirSize("a/b")),
		contents("a/b/x", "1"),
		contents("a/z.txt", "3"),
		contents("c&amp;d&lt;e&gt;.bin", "4"),
		contents("top.txt", "0"),
	}
	for _, opts := range []store.Options{{}, {Prefetch: store.Prefetch{Off: true}}} {
		s := serveRecording(t, dir, "swath-local/v1", opts)
		for _, c := range []struct {
			query string
			want  []string
		}{
			{"", all},
			{"&fetch-owner=true", all},
			{"&max-keys=2&fetch-owner=true", all},
			{"&delimiter=/", all[4:]},
			{"&prefix=a/&delimiter=/", []string{all[0], all[3]}},
		} {
			if got := s3Walk(t, s, c.query); strings.Join(got, "\n") != strings.Join(c.want, "\n") {
				t.Errorf("prefetch off %v, query %q:\n%s\nwant\n%s", opts.Prefetch.Off, c.query, strings.Join(got, "\n"), strings.Join(c.want, "\n"))
			}
		}
		r := get(t, s, "/storage/v1/b/bucket/o?prefix=a/b/x")
		if want := `{"kind":"storage#object","bucket":"bucket","name":"a/b/x","size":"1","updated":"2025-11-06T03:33:20.123456Z",` +
			`"etag":"swath-gcs-synthetic-v1","generation":"1","metageneration":"1","storageClass":"STANDARD","contentType":"application/octet-stream"}`; r.status != 200 || !strings.Contains(r.body, want) {
			t.Errorf("GCS: %d %s, want %s", r.status, r.body, want)
		}
		r = azureDo(t, s, "GET", "/replay/bucket?restype=container&comp=list&prefix=a/b/x", "x-ms-version", "2026-06-06")
		if want := "<Name>a/b/x</Name><Properties><Last-Modified>Thu, 06 Nov 2025 03:33:20 GMT</Last-Modified><Etag>0x000000000000001</Etag>" +
			"<Content-Length>1</Content-Length><Content-Type>application/octet-stream</Content-Type><BlobType>BlockBlob</BlobType>"; r.status != 200 || !strings.Contains(r.body, want) {
			t.Errorf("Azure: %d %s, want %s", r.status, r.body, want)
		}
	}
}
