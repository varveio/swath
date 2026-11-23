// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux || darwin

package localfs

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/reader/du"
	"github.com/varveio/swath/reader/listing"
)

// volumeFixture changes only the volume identity of m, never its stat
// device/inode. The hook acts on an opened descriptor, so replacement and
// descriptor-reopening tests exercise the same decision as a real volume.
func volumeFixture(t *testing.T) (string, *atomic.Int64) {
	t.Helper()
	root := t.TempDir()
	mk(t, root, "m/hidden/file", false)
	mk(t, root, "kept", false)
	mk(t, root, "empty", true)
	var m, hidden unix.Stat_t
	if err := unix.Stat(filepath.Join(root, "m"), &m); err != nil {
		t.Fatal(err)
	}
	if err := unix.Stat(filepath.Join(root, "m/hidden"), &hidden); err != nil {
		t.Fatal(err)
	}
	old := readFilesystemID
	t.Cleanup(func() { readFilesystemID = old })
	calls := &atomic.Int64{}
	readFilesystemID = func(fd int) ([2]int32, error) {
		var s unix.Stat_t
		if err := unix.Fstat(fd, &s); err != nil {
			return [2]int32{}, err
		}
		if (s.Ino == m.Ino || s.Ino == hidden.Ino) && s.Dev == m.Dev {
			calls.Add(1)
			return [2]int32{2, 1}, nil
		}
		return [2]int32{1, 1}, nil
	}
	return root, calls
}

func TestVolumeBoundaryWalk(t *testing.T) {
	root, calls := volumeFixture(t)
	for _, portable := range []bool{false, true} {
		for _, count := range []bool{false, true} {
			for _, noDirs := range []bool{false, true} {
				t.Run(fmt.Sprintf("portable=%v/count=%v/noDirs=%v", portable, count, noDirs), func(t *testing.T) {
					o, err := Open(root, Options{Portable: portable, OneFilesystem: true, NoDirs: noDirs}, nil)
					if err != nil {
						t.Fatal(err)
					}
					c := &collect{}
					if count {
						err = countOpened(context.Background(), o, 4, c)
					} else {
						_, err = order.Run[Range, Entry](context.Background(), o.p, c, &Schema, order.Options{Workers: 4, Budget: 1 << 20})
					}
					if err != nil {
						t.Fatal(err)
					}
					var keys []string
					for _, r := range c.rows {
						keys = append(keys, r.Key)
					}
					slices.Sort(keys)
					want := []string{"empty/", "kept", "m/"}
					if noDirs {
						want = []string{"kept"}
					}
					if !slices.Equal(keys, want) {
						t.Fatalf("keys %q, want %q (same-device volume must not be entered)", keys, want)
					}
					cov := o.Report().Coverage
					b := cov.FilesystemBoundaries
					if !cov.Complete || cov.Unreadable != 0 || b == nil || !b.Complete || b.TotalCount != 1 ||
						!slices.Equal(b.KeysBase64, []string{base64.StdEncoding.EncodeToString([]byte("m/"))}) {
						t.Fatalf("coverage %+v boundaries %+v", cov, b)
					}
					st := o.p.Stats()
					if st.OtherVolume != 1 || st.OtherDevice != 0 || st.PrunedDirs != 0 || st.Errors() != 0 || st.FilesystemBoundary != filesystemMechanism {
						t.Fatalf("stats %+v", st)
					}
					tally, err := du.New(du.Options{Prefixes: []string{""}})
					if err != nil {
						t.Fatal(err)
					}
					if err := tally.Add(walkEntries(c.rows)); err != nil {
						t.Fatal(err)
					}
					if got := tally.Result()[0].Size; got != 5 {
						t.Fatalf("du size %v, want 5 bytes", got)
					}
				})
			}
		}
	}
	if calls.Load() != 8 {
		t.Fatalf("foreign volume checked %d times, want 8", calls.Load())
	}
}

func TestVolumeBoundaryLive(t *testing.T) {
	root, calls := volumeFixture(t)
	st, err := OpenLive(holdDir(t, root))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, prefix := range []string{"m/", "m/hidden/"} {
		if _, err := st.List(ctx, []byte(prefix), "", 10); !errors.Is(err, listing.ErrOtherDevice) {
			t.Fatalf("page %q: %v", prefix, err)
		}
	}
	r, err := st.Lookup(ctx, []byte("m/hidden/file"))
	if err != nil || r.State != listing.LookupUnknown || !errors.Is(r.Reason, listing.ErrOtherDevice) {
		t.Fatalf("lookup: %+v %v", r, err)
	}
	r, err = st.Lookup(ctx, []byte("m"))
	if err != nil || r.State != listing.LookupPresent {
		t.Fatalf("boundary's row: %+v %v", r, err)
	}
	if calls.Load() == 0 {
		t.Fatal("volume check did not engage")
	}
	// Selecting the foreign volume itself as root accepts that volume.
	st, err = OpenLive(holdDir(t, filepath.Join(root, "m")))
	if err != nil {
		t.Fatal(err)
	}
	lvl, err := st.List(ctx, nil, "", 10)
	if err != nil || len(lvl.Objects) != 1 || string(lvl.Objects[0].Key) != "hidden/" {
		t.Fatalf("selected root: %+v %v", lvl, err)
	}
	lvl, err = st.List(ctx, []byte("hidden/"), "", 10)
	if err != nil || len(lvl.Objects) != 2 || string(lvl.Objects[1].Key) != "hidden/file" {
		t.Fatalf("selected volume descendants: %+v %v", lvl, err)
	}
}

func TestVolumeIdentityFailure(t *testing.T) {
	root, _ := volumeFixture(t)
	identify := readFilesystemID
	readFilesystemID = func(fd int) ([2]int32, error) {
		id, err := identify(fd)
		if id[0] == 2 {
			return id, unix.EIO
		}
		return id, err
	}
	o, err := Open(root, Options{OneFilesystem: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	keys := countKeys(t, o, 4)
	if !slices.Equal(keys, []string{"empty/", "kept", "m/"}) {
		t.Fatalf("keys %q: failed identity must prevent descent", keys)
	}
	cov, st := o.Report().Coverage, o.p.Stats()
	if cov.Complete || cov.Unreadable != 1 || cov.FilesystemBoundaries.TotalCount != 0 || st.OpenErrors != 1 || st.FilesystemIdentityErrors != 1 || st.OtherVolume != 0 {
		t.Fatalf("identity failure mislabeled: coverage %+v, stats %+v", cov, st)
	}
	live, err := OpenLive(holdDir(t, root))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := live.List(context.Background(), []byte("m/"), "", 10); !errors.Is(err, listing.ErrUnavailable) {
		t.Fatalf("page: %v", err)
	}
	if r, err := live.Lookup(context.Background(), []byte("m/file")); err != nil || r.State != listing.LookupUnknown || !errors.Is(r.Reason, listing.ErrUnavailable) {
		t.Fatalf("lookup %+v %v", r, err)
	}
	if _, err := Open(filepath.Join(root, "m"), Options{OneFilesystem: true}, nil); !errors.Is(err, unix.EIO) {
		t.Fatalf("root identity failure: %v", err)
	}
}

func TestVolumeBoundaryReopen(t *testing.T) {
	root, _ := volumeFixture(t)
	f := holdDir(t, root)
	o := mustOpenDir(t, f, Options{})
	// A queued descendant is opened from the held root because intermediate
	// descriptors were not retained. Coverage must name m/, not m/hidden/.
	r := Range{path: filepath.Join(root, "m/hidden"), prefix: "m/hidden/", at: o.p.held, below: 2}
	var st Stats
	if _, ok, err := o.p.openRange(context.Background(), r, &st); ok || err != nil {
		t.Fatalf("open: %v %v", ok, err)
	}
	b := o.Report().Coverage.FilesystemBoundaries
	if b.TotalCount != 1 || !slices.Equal(b.KeysBase64, []string{"bS8="}) {
		t.Fatalf("wrong boundary %+v", b)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, ok, err := o.p.openRange(ctx, r, &st); ok || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled descent: %v %v", ok, err)
	}
	if got := o.Report().Coverage.FilesystemBoundaries.TotalCount; got != 1 {
		t.Fatalf("cancelled descent recorded a boundary: %d", got)
	}
}

func TestVolumeBoundaryReplacement(t *testing.T) {
	root, calls := volumeFixture(t)
	mk(t, root, "pending/old", false)
	f := holdDir(t, root)
	o := mustOpenDir(t, f, Options{})
	// Construct the queued state, then replace it with the foreign volume
	// before expansion. No sleeps or scheduling assumptions.
	r := Range{path: filepath.Join(root, "pending"), prefix: "pending/", at: o.p.held, below: 1}
	if err := os.Rename(filepath.Join(root, "pending"), filepath.Join(root, "old")); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(root, "m"), filepath.Join(root, "pending")); err != nil {
		t.Fatal(err)
	}
	var st Stats
	if _, ok, err := o.p.openRange(context.Background(), r, &st); ok || err != nil {
		t.Fatalf("replacement entered: %v %v", ok, err)
	}
	if calls.Load() != 1 || st.OtherVolume != 1 || o.Report().Coverage.FilesystemBoundaries.KeysBase64[0] != "cGVuZGluZy8=" {
		t.Fatalf("replacement not identified: %+v", st)
	}
}
