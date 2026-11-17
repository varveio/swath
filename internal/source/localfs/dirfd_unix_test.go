// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux || darwin

package localfs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/source"
)

// swapAt is a provider whose walk calls swap once, just before it expands
// the directory range at prefix: after the directory's parent was listed
// and before the directory is opened. It is the tests' hook into a walk;
// nothing waits on timing.
type swapAt struct {
	*Provider
	prefix string
	swap   func() error
	err    error
	once   sync.Once
}

func (s *swapAt) Expand(ctx context.Context, r Range, e source.Emitter[Range, Entry]) error {
	if r.prefix == s.prefix && r.chunk == nil {
		s.once.Do(func() { s.err = s.swap() })
	}
	return s.Provider.Expand(ctx, r, e)
}

// TestSwappedDirectoryNotFollowed: a directory below the root replaced by
// a symbolic link to a directory outside it, after its parent was listed
// and before the walk opens it or a directory below it, is never
// followed, with either engine. The walk lists what it opened: a
// directory below the replaced one is opened from its parent's
// descriptor, which still holds the directory moved aside. Opened again
// from the root (no descriptor kept), it refuses the link, as it refuses
// a link in place of the directory it opens: unsafe_dirs, unreadable,
// named in the error samples (with the level refused, from the root), the
// directory told to the run (Opened.Begin's unreadable) and reported
// unread, its subtree behind a symbolic link (unread).
func TestSwappedDirectoryNotFollowed(t *testing.T) {
	listed := []string{"a/", "a/b/", "a/b/c/"}
	whole := append(slices.Clone(listed), "a/b/c/d/", "a/b/c/d/e", "a/b/c/in")
	cases := []struct {
		name     string
		replaced string // the directory replaced by a link to its double outside the root
		kept     bool   // descriptors kept for queued subdirectories, else every one opened from the root
		want     []string
		level    int // the level refused (unsafe), 0 for none
	}{
		{"intermediate", "a/b", true, whole, 0},
		{"intermediate-from-root", "a/b", false, listed, 2},
		{"opened", "a/b/c", true, listed, 3},
		{"opened-from-root", "a/b/c", false, listed, 3},
	}
	for _, c := range cases {
		for _, portable := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/portable=%v", c.name, portable), func(t *testing.T) {
				base := t.TempDir()
				root, outside := filepath.Join(base, "root"), filepath.Join(base, "outside")
				mk(t, root, "a/b/c/in", false)
				mk(t, root, "a/b/c/d/e", false)
				mk(t, outside, "a/b/c/out", false)
				mk(t, outside, "a/b/c/d/secret", false)
				p, err := New(root, Options{Portable: portable})
				if err != nil {
					t.Fatal(err)
				}
				if !c.kept {
					p.keep.limit = 0
				}
				var unread []string
				p.unreadable = func(prefix string) { unread = append(unread, prefix) }
				var u unreads
				p.unreadTo = u.add
				s := &swapAt{Provider: p, prefix: "a/b/c/", swap: func() error {
					at := filepath.Join(root, filepath.FromSlash(c.replaced))
					if err := os.Rename(at, at+".moved"); err != nil {
						return err
					}
					return os.Symlink(filepath.Join(outside, filepath.FromSlash(c.replaced)), at)
				}}
				col := &collect{}
				if _, err := order.Run[Range, Entry](context.Background(), s, col, &Schema, order.Options{Workers: 4, Budget: 1 << 20}); err != nil {
					t.Fatal(err)
				}
				if s.err != nil {
					t.Fatal(s.err)
				}
				var got []string
				for _, r := range col.rows {
					got = append(got, r.Key)
				}
				if !slices.Equal(got, c.want) {
					t.Fatalf("keys %q, want %q", got, c.want)
				}
				st := p.Stats()
				if c.level == 0 {
					if st.Errors() != 0 || len(unread) != 0 || len(u.got) != 0 {
						t.Fatalf("errors %d %q, unreadable %q, unread %q", st.Errors(), st.Samples, unread, u.got)
					}
					return
				}
				if !slices.Equal(u.sorted(), []string{"subtree symlink_ancestor a/b/c/"}) {
					t.Fatalf("unread %q", u.sorted())
				}
				want := "a symbolic link, not followed"
				if !c.kept { // a descent of several names names the level refused
					want = fmt.Sprintf("level %d below the root: %s", c.level, want)
				}
				if st.Unsafe != 1 || st.Errors() != 1 || st.DirOpens.Failed["symlink"] != 1 || len(st.Samples) != 1 || !strings.Contains(st.Samples[0], want) ||
					!slices.Equal(unread, []string{"a/b/c/"}) {
					t.Fatalf("unsafe %d, errors %d, dir_opens %+v, samples %q, unreadable %q; want one sample with %q",
						st.Unsafe, st.Errors(), st.DirOpens, st.Samples, unread, want)
				}
			})
		}
	}
}

// cancelAfter is a sink that cancels its run once it has seen n rows.
type cancelAfter struct {
	n, seen int
	cancel  context.CancelFunc
}

func (c *cancelAfter) Write(_ context.Context, segs [][]Row) error {
	for _, s := range segs {
		c.seen += len(s)
	}
	if c.seen >= c.n {
		c.cancel()
	}
	return nil
}

// TestWalkReleasesDescriptors: a walk closes every directory descriptor it
// opened, whether it completes, is cancelled or fails, with either engine,
// with descriptors kept for queued subdirectories and without; a small
// row budget leaves ranges queued, and so references held, when the run
// stops.
func TestWalkReleasesDescriptors(t *testing.T) {
	root := t.TempDir()
	for i := range 6 {
		for j := range 6 {
			mk(t, root, fmt.Sprintf("d%d/e%d/f%d/g", i, j, j), false)
		}
	}
	for i := range 5000 { // chunked by the native engine
		mk(t, root, fmt.Sprintf("big/f%05d", i), false)
	}
	mk(t, root, "big/sub/h", false)
	_, warm := list(t, root, Options{}, 4, 1<<20) // the runtime's own descriptors (poller) open once
	before := countFDs(t)
	for _, kept := range []bool{true, false} {
		for _, portable := range []bool{false, true} {
			for _, end := range []string{"completed", "cancelled", "failed"} {
				p, err := New(root, Options{Portable: portable})
				if err != nil {
					t.Fatal(err)
				}
				if !kept {
					p.keep.limit = 0
				}
				ctx, cancel := context.WithCancel(context.Background())
				var sink order.Sink[Entry] = &collect{}
				switch end {
				case "cancelled":
					sink = &cancelAfter{n: 100, cancel: cancel}
				case "failed":
					sink = &failAfter{n: 100}
				}
				_, err = order.Run[Range, Entry](ctx, p, sink, &Schema, order.Options{Workers: 4, Budget: 7 * 256})
				cancel()
				if (err == nil) != (end == "completed") || end == "cancelled" && !errors.Is(err, context.Canceled) {
					t.Fatalf("kept=%v portable=%v %s: %v", kept, portable, end, err)
				}
				st := p.Stats()
				if end == "completed" && (st.Files != warm.Files || st.Dirs != warm.Dirs || st.DirOpens.Opened != warm.Dirs) {
					t.Fatalf("kept=%v portable=%v: files %d dirs %d opened %d, want %d %d %d", kept, portable, st.Files, st.Dirs, st.DirOpens.Opened, warm.Files, warm.Dirs, warm.Dirs)
				}
				if n := p.keep.open.Load(); n != 0 {
					t.Fatalf("kept=%v portable=%v %s: %d descriptors still kept", kept, portable, end, n)
				}
				if n := countFDs(t); n != before {
					t.Fatalf("kept=%v portable=%v %s: %d descriptors open, %d before", kept, portable, end, n, before)
				}
			}
		}
	}
}

// TestOtherDeviceListedNotEntered: with OneFilesystem, the filesystems
// mounted on directories below the root (/dev's: pts, shm, mqueue on
// Linux) are listed and not entered, with either engine; one of them at
// least holds entries.
func TestOtherDeviceListedNotEntered(t *testing.T) {
	const root = "/dev"
	var rs unix.Stat_t
	if err := unix.Stat(root, &rs); err != nil {
		t.Skip(err)
	}
	ents, err := os.ReadDir(root)
	if err != nil {
		t.Skip(err)
	}
	var mounts []string
	held := false
	for _, e := range ents {
		var s unix.Stat_t
		if e.IsDir() && unix.Lstat(root+"/"+e.Name(), &s) == nil && s.Dev != rs.Dev {
			mounts = append(mounts, e.Name()+"/")
			in, _ := os.ReadDir(root + "/" + e.Name())
			held = held || len(in) > 0
		}
	}
	if !held {
		t.Skip("no filesystem holding entries is mounted on a directory of /dev here")
	}
	for _, portable := range []bool{false, true} {
		rows, st := list(t, root, Options{OneFilesystem: true, Portable: portable}, 4, 1<<20)
		listed := 0
		for _, r := range rows {
			for _, m := range mounts {
				if r.Key == m {
					listed++
				} else if strings.HasPrefix(r.Key, m) {
					t.Fatalf("portable=%v: %s entered (%s)", portable, m, r.Key)
				}
			}
		}
		if listed != len(mounts) || st.OtherDevice < int64(len(mounts)) {
			t.Fatalf("portable=%v: %d of %q listed, other_device_dirs %d", portable, listed, mounts, st.OtherDevice)
		}
	}
}
