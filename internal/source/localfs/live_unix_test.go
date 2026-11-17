// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux || darwin

package localfs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/varveio/swath/internal/live"
	"github.com/varveio/swath/reader/listing"
)

// TestLiveOtherDevice: a directory on another device than the held root,
// which a test cannot mount, made so by the device check's stat: it is
// listed as a child prefix of its parent; its own level, and one below
// it, fail as on another device; a lookup through it is unknown, in that
// class; the directory itself is present.
func TestLiveOtherDevice(t *testing.T) {
	root := t.TempDir()
	mk(t, root, "m/x/f", false)
	mk(t, root, "f", false)
	var m unix.Stat_t
	if err := unix.Stat(filepath.Join(root, "m"), &m); err != nil {
		t.Fatal(err)
	}
	fstat := lookupFstat
	t.Cleanup(func() { lookupFstat = fstat })
	lookupFstat = func(fd int, s *unix.Stat_t) error {
		err := fstat(fd, s)
		if err == nil && s.Ino == m.Ino {
			s.Dev++
		}
		return err
	}
	st, err := OpenLive(holdDir(t, root))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	lvl, err := st.List(ctx, nil, "", 10)
	if err != nil || len(lvl.Prefixes) != 1 || string(lvl.Prefixes[0]) != "m/" || len(lvl.Objects) != 1 {
		t.Fatalf("the root: %+v %v", lvl, err)
	}
	for _, prefix := range []string{"m/", "m/x/"} {
		if _, err := st.List(ctx, []byte(prefix), "", 10); !errors.Is(err, listing.ErrOtherDevice) {
			t.Errorf("%s: %v, want ErrOtherDevice", prefix, err)
		}
	}
	r, err := st.Lookup(ctx, []byte("m/x/f"))
	if err != nil || r.State != listing.LookupUnknown || !errors.Is(r.Reason, listing.ErrOtherDevice) ||
		!strings.Contains(r.Reason.Error(), errOtherDevice.Error()) {
		t.Errorf("m/x/f: %+v %v, want unknown on another device", r, err)
	}
	if r, err := st.Lookup(ctx, []byte("m")); err != nil || r.State != listing.LookupPresent || string(r.Entry.Key) != "m/" {
		t.Errorf("m: %+v %v", r, err)
	}
}

// TestLiveLevelSelf: a level's first page of a prefix begins with the
// directory's own entry, which counts toward the page's entries: a page
// of one holds it alone and continues after it.
func TestLiveLevelSelf(t *testing.T) {
	root := t.TempDir()
	mk(t, root, "d/a", false)
	mk(t, root, "d/b/c", false)
	st, err := OpenLive(holdDir(t, root))
	if err != nil {
		t.Fatal(err)
	}
	var levels []*live.Level
	token := ""
	for {
		lvl, err := st.List(context.Background(), []byte("d/"), token, 1)
		if err != nil {
			t.Fatal(err)
		}
		levels = append(levels, lvl)
		if !lvl.Truncated {
			break
		}
		token = lvl.Next
	}
	if len(levels) != 3 || string(levels[0].Objects[0].Key) != "d/" || levels[0].Objects[0].Kind != listing.KindDir || levels[0].Next != "d/" ||
		string(levels[1].Objects[0].Key) != "d/a" || string(levels[2].Prefixes[0]) != "d/b/" {
		t.Errorf("levels %+v", levels)
	}
	if _, err := st.List(context.Background(), []byte("d/"), "e/a", 1); !errors.Is(err, listing.ErrInvalidToken) {
		t.Errorf("a token outside the prefix: %v", err)
	}
	f, err := os.Open(filepath.Join(root, "d/a"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := OpenLive(f); !errors.Is(err, listing.ErrNotDirectory) {
		t.Errorf("a file root: %v", err)
	}
}
