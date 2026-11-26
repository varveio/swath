// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/varveio/swath/internal/dataset"
)

// TestDuLinksTier: on Windows, swath du --metadata links counts a
// hard-linked file once, under the first of its names in byte order, and
// says so (links_once true); --count-links, and the stat tier, count every
// name (links_once false). swath list at the tier gives both names nlink
// 2, a dataset listed at it records the tier (collected.metadata) and swath
// du --from-dataset totals it as the live run does.
func TestDuLinksTier(t *testing.T) {
	captureStderr(t)
	root := t.TempDir()
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, d := range []string{"a", "b"} {
		if err := os.Mkdir(filepath.Join(root, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for name, n := range map[string]int{"a/f": 10000, "c": 3} {
		p := filepath.Join(root, filepath.FromSlash(name))
		if err := os.WriteFile(p, bytes.Repeat([]byte{'x'}, n), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(p, at, at); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Link(filepath.Join(root, "a", "f"), filepath.Join(root, "b", "f-link")); err != nil {
		t.Fatal(err)
	}

	const newest = "2026-01-02T03:04:05.000000Z"
	header := "row_type\tdepth\tobjects\tdirs\tsize\tsize_missing\tmax_last_modified\tprimary\tsort\tlinks_once\tprefix\n"
	tail := func(once bool) string { return "\tsize\tname\t" + strconv.FormatBool(once) + "\t" }
	once := header +
		"total\t0\t2\t2\t10003\t0\t" + newest + tail(true) + "\n" +
		"prefix\t1\t1\t0\t10000\t0\t" + newest + tail(true) + "a/\n" +
		"prefix\t1\t0\t0\t0\t0\t\\N" + tail(true) + "b/\n"
	every := header +
		"total\t0\t3\t2\t20003\t0\t" + newest + tail(false) + "\n" +
		"prefix\t1\t1\t0\t10000\t0\t" + newest + tail(false) + "a/\n" +
		"prefix\t1\t1\t0\t10000\t0\t" + newest + tail(false) + "b/\n"
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"--metadata", "links"}, once},
		{[]string{"--metadata", "links", "--count-links"}, every},
		{nil, every},
	} {
		out, err := duRun(t, append([]string{root, "-d", "1", "--format", "tsv", "--apparent-size"}, tc.args...)...)
		if err != nil {
			t.Fatalf("%q: %v", tc.args, err)
		}
		if got := dropColumns(out, "allocated_bytes", "allocated_bytes_missing"); got != tc.want {
			t.Errorf("%q: got\n%s\nwant\n%s", tc.args, got, tc.want)
		}
	}

	var out bytes.Buffer
	if err := run(context.Background(), []string{root, "--metadata", "links", "--format", "tsv", "--columns", "key,nlink", "--no-stats"}, &out, false); err != nil {
		t.Fatal(err)
	}
	if want := "key\tnlink\na/\t1\na/f\t2\nb/\t1\nb/f-link\t2\nc\t1\n"; out.String() != want {
		t.Errorf("list: got\n%s\nwant\n%s", out.String(), want)
	}

	ds := filepath.Join(t.TempDir(), "ds")
	if err := run(context.Background(), []string{root, "--metadata", "links", "-o", ds, "--no-stats"}, io.Discard, false); err != nil {
		t.Fatal(err)
	}
	var m dataset.Manifest
	if readJSON(t, filepath.Join(ds, dataset.ManifestName), &m); m.Collected.Metadata != "links" {
		t.Fatalf("manifest collected %+v", m.Collected)
	}
	for _, extra := range [][]string{nil, {"--count-links"}, {"--apparent-size"}, {"--allocated"}} {
		for depth := range 3 {
			for _, format := range []string{"tsv", "jsonl", "table"} {
				flags := append([]string{"-d", strconv.Itoa(depth), "--format", format}, extra...)
				liveAndSaved(t, append([]string{root, "--metadata", "links"}, flags...), append([]string{"--from-dataset", ds}, flags...))
			}
		}
	}
}
