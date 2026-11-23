// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"context"
	"encoding/base64"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	seekparquet "github.com/varveio/seekparquet/go"
	"golang.org/x/sys/unix"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/reader"
)

// /dev supplies real different-device boundaries without mount privileges.
// Exercise the command, both traversal engines, publication, and reader.
func TestFilesystemBoundariesCommand(t *testing.T) {
	var root unix.Stat_t
	if err := unix.Stat("/dev", &root); err != nil {
		t.Skip(err)
	}
	entries, err := os.ReadDir("/dev")
	if err != nil {
		t.Skip(err)
	}
	var want []string
	for _, e := range entries {
		var s unix.Stat_t
		if e.IsDir() && unix.Lstat("/dev/"+e.Name(), &s) == nil && s.Dev != root.Dev {
			want = append(want, e.Name()+"/")
		}
	}
	if len(want) == 0 {
		t.Skip("no different-device directories mounted below /dev")
	}
	for _, engine := range []string{"", "portable"} {
		for _, noDirs := range []bool{false, true} {
			t.Setenv("SWATH_LOCALFS_ENGINE", engine)
			out := filepath.Join(t.TempDir(), "dataset")
			args := []string{"/dev", "-o", out, "--one-file-system", "--no-stats"}
			if noDirs {
				args = append(args, "--no-dirs")
			}
			if err := run(context.Background(), args, io.Discard, false); err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(filepath.Join(out, dataset.ManifestName))
			if err != nil {
				t.Fatal(err)
			}
			sm, err := seekparquet.ParseManifest(raw, dataset.ManifestLimits)
			if err != nil {
				t.Fatal(err)
			}
			m, err := reader.ParseMetadata(seekparquet.Metadata{SchemaID: sm.SchemaID, Extensions: sm.Extensions})
			if err != nil {
				t.Fatal(err)
			}
			b := m.Coverage.FilesystemBoundaries
			if b == nil || !b.Complete || b.TotalCount != int64(len(want)) {
				t.Fatalf("boundaries %+v, want %q", b, want)
			}
			var got []string
			for _, encoded := range b.KeysBase64 {
				k, err := base64.StdEncoding.DecodeString(encoded)
				if err != nil {
					t.Fatal(err)
				}
				got = append(got, string(k))
			}
			slices.Sort(got)
			if !slices.Equal(got, want) {
				t.Fatalf("boundaries %q, want %q", got, want)
			}
			keys := datasetKeys(t, out)
			for _, boundary := range want {
				if slices.Contains(keys, boundary) == noDirs {
					t.Fatalf("noDirs=%v: boundary row %q in %q", noDirs, boundary, keys)
				}
				for _, key := range keys {
					if key != boundary && strings.HasPrefix(key, boundary) {
						t.Fatalf("entered %q: %q", boundary, key)
					}
				}
			}
		}
	}
}
