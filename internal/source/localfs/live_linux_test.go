// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/varveio/swath/internal/live"
)

// untyped makes every level read leave its entries untyped, as some
// filesystems do (DT_UNKNOWN), until the test ends.
func untyped(t *testing.T) {
	getdents := levelGetdents
	t.Cleanup(func() { levelGetdents = getdents })
	levelGetdents = func(fd int, buf []byte) (int, error) {
		n, err := getdents(fd, buf)
		for rec := buf[:max(n, 0)]; len(rec) >= direntHeader; {
			rec[18] = unix.DT_UNKNOWN
			rec = rec[binary.NativeEndian.Uint16(rec[16:18]):]
		}
		return n, err
	}
}

// keysOf is the keys of a level: "kind key" for an entry, "key" for a
// prefix.
func keysOf(lvl *live.Level) []string {
	var got []string
	for _, e := range lvl.Objects {
		got = append(got, e.Kind.String()+" "+string(e.Key))
	}
	for _, p := range lvl.Prefixes {
		got = append(got, string(p))
	}
	return got
}

// TestLiveUntyped: entries the filesystem leaves untyped are typed by a
// stat relative to the directory, a child directory still a prefix; in a
// directory that can be read but not searched, where that stat fails,
// each is an entry of unknown kind and the page does not fail.
func TestLiveUntyped(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root searches any directory")
	}
	root := t.TempDir()
	mk(t, root, "e/x", false)
	mk(t, root, "e/y/z", false)
	mk(t, root, "d/a", false)
	mk(t, root, "d/s/z", false)
	d := filepath.Join(root, "d")
	if err := os.Chmod(d, 0o444); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(d, 0o755) })
	untyped(t)
	st, err := OpenLive(holdDir(t, root), 1<<20)
	if err != nil {
		t.Fatal(err)
	}

	for prefix, want := range map[string][]string{
		"e/": {"dir e/", "file e/x", "e/y/"},
		"d/": {"dir d/", "unknown d/a", "unknown d/s"},
	} {
		lvl, err := st.List(context.Background(), []byte(prefix), "", 10)
		if err != nil {
			t.Errorf("%s: %v", prefix, err)
			continue
		}
		if got := keysOf(lvl); !slices.Equal(got, want) {
			t.Errorf("%s: %q, want %q", prefix, got, want)
		}
	}
}

// TestLiveCanceledAfterRead: a stop of the context once the directory is
// read, before its entries are statted, fails the page with the
// context's error instead of answering it whole.
func TestLiveCanceledAfterRead(t *testing.T) {
	root := t.TempDir()
	mk(t, root, "d/a", false)
	mk(t, root, "d/b", false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	getdents := levelGetdents
	t.Cleanup(func() { levelGetdents = getdents })
	levelGetdents = func(fd int, buf []byte) (int, error) {
		n, err := getdents(fd, buf)
		if n == 0 && err == nil {
			cancel()
		}
		return n, err
	}
	st, err := OpenLive(holdDir(t, root), 1<<20)
	if err != nil {
		t.Fatal(err)
	}

	if lvl, err := st.List(ctx, []byte("d/"), "", 10); !errors.Is(err, context.Canceled) {
		t.Errorf("%+v %v, want context.Canceled", lvl, err)
	}
	if _, err := st.List(context.Background(), []byte("d/"), "", 10); err != nil {
		t.Errorf("a page not stopped: %v", err)
	}
}
