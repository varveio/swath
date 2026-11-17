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

	"github.com/varveio/swath/internal/bind"
	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/reader/listing"
)

// Directory sizes past the Linux engine's thresholds, so that a count
// reads one in several slabs (slabEntries) and a listing chunks one
// (chunkThreshold); the portable engine reads them in batches.
const (
	slabbed = 2*8192 + 100
	chunked = 4096 + 100
)

// holdDir opens dir as a caller of OpenDir does: O_DIRECTORY, not
// following a symbolic link. It is closed when the test ends.
func holdDir(t *testing.T, dir string) *os.File {
	t.Helper()
	f, err := os.OpenFile(dir, os.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return f
}

// unreads collects what a listing reports it did not read (unread),
// each "scope class key".
type unreads struct {
	mu  sync.Mutex
	got []string
}

func (u *unreads) add(key string, scope listing.UnreadScope, class listing.UnreadClass) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.got = append(u.got, fmt.Sprintf("%s %s %s", scope, class, key))
}

// to has o's listing report to u.
func (u *unreads) to(o *Opened) *Opened {
	o.p.unreadTo = u.add
	return o
}

func (u *unreads) sorted() []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return slices.Sorted(slices.Values(u.got))
}

// countOpened lists o as a library walk does: Begin a count, then the
// bound provider unordered into sink.
func countOpened(ctx context.Context, o *Opened, workers int, sink order.Sink[Entry]) error {
	env := bind.Env{Order: order.Options{Workers: workers}}
	end, err := o.Begin(nil, nil, true, &env.Order)
	if err != nil {
		return err
	}
	defer end()
	l, err := o.Bound().Prepare(env)
	if err != nil {
		return err
	}
	_, err = l.Count(ctx, sink)
	return err
}

// countKeys counts o into a collector and returns its keys, sorted.
func countKeys(t *testing.T, o *Opened, workers int) []string {
	t.Helper()
	c := &collect{}
	if err := countOpened(context.Background(), o, workers, c); err != nil {
		t.Fatal(err)
	}
	return rowKeys(c.rows)
}

func rowKeys(rows []Row) []string {
	keys := make([]string, len(rows))
	for i := range rows {
		keys[i] = rows[i].Key
	}
	slices.Sort(keys)
	return keys
}

// TestHeldRootNotResolvedByPath: a root held open lists the directory the
// caller opened after its path was moved aside and replaced by a symbolic
// link to a directory outside, with either engine, as a count and as a
// sorted listing; a prefix lists its directory's subtree with keys
// relative to the root; a symbolic link on the way to the prefix is not
// followed and is reported.
func TestHeldRootNotResolvedByPath(t *testing.T) {
	base := t.TempDir()
	root, outside := filepath.Join(base, "root"), filepath.Join(base, "outside")
	mk(t, root, "in/a", false)
	mk(t, root, "in/sub/b", false)
	mk(t, outside, "in/secret", false)
	f := holdDir(t, root)
	if err := os.Rename(root, root+".moved"); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, root); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("in", filepath.Join(root+".moved", "lnk")); err != nil {
		t.Fatal(err)
	}

	for _, portable := range []bool{false, true} {
		t.Run(fmt.Sprintf("portable=%v", portable), func(t *testing.T) {
			want := []string{"in/", "in/a", "in/sub/", "in/sub/b", "lnk"}
			o, err := OpenDir(f, "", Options{Fields: Stat, Portable: portable})
			if err != nil {
				t.Fatal(err)
			}
			if got := countKeys(t, o, 4); !slices.Equal(got, want) {
				t.Fatalf("count: %q, want %q", got, want)
			}
			o, err = OpenDir(f, "", Options{Fields: Stat, Portable: portable})
			if err != nil {
				t.Fatal(err)
			}
			if got, _ := walk(t, o.p, 4, 1<<20); !slices.Equal(rowKeys(got), want) {
				t.Fatalf("sorted listing: %q, want %q", rowKeys(got), want)
			}

			o, err = OpenDir(f, "in/", Options{Fields: Stat, Portable: portable})
			if err != nil {
				t.Fatal(err)
			}
			if got := countKeys(t, o, 4); !slices.Equal(got, []string{"in/a", "in/sub/", "in/sub/b"}) {
				t.Fatalf("prefix in/: %q", got)
			}

			var u unreads
			o, err = OpenDir(f, "lnk/sub/", Options{Fields: Stat, Portable: portable})
			if err != nil {
				t.Fatal(err)
			}
			if got := countKeys(t, u.to(o), 4); len(got) != 0 || !slices.Equal(u.sorted(), []string{"subtree symlink_ancestor lnk/sub/"}) {
				t.Fatalf("prefix through a link: keys %q, unread %q", got, u.sorted())
			}
			if st := o.p.Stats(); st.Unsafe != 1 || len(st.Samples) != 1 || !strings.Contains(st.Samples[0], "level 1 below the root") {
				t.Fatalf("prefix through a link: unsafe %d, samples %q", st.Unsafe, st.Samples)
			}
		})
	}
	for _, bad := range []string{"in", "/in/", "in//", "./in/", "in/../", "in\x00/"} {
		if _, err := OpenDir(f, bad, Options{Fields: Stat}); err == nil {
			t.Errorf("prefix %q accepted", bad)
		}
	}
	if _, err := OpenDir(f, "", Options{Fields: Names}); err == nil {
		t.Error("the names tier accepted: no device to check")
	}
}

// TestHeldWalkReleasesDescriptors: a held root's walk closes every
// descriptor it opened and leaves the caller's open, whether it completes,
// is cancelled or fails, as a count and as a sorted listing, with either
// engine and with nothing kept but the root.
func TestHeldWalkReleasesDescriptors(t *testing.T) {
	root := t.TempDir()
	for i := range 6 {
		for j := range 6 {
			mk(t, root, fmt.Sprintf("d%d/e%d/f%d/g", i, j, j), false)
		}
	}
	for i := range slabbed {
		mk(t, root, fmt.Sprintf("big/f%05d", i), false)
	}
	f := holdDir(t, root)
	countKeys(t, mustOpenDir(t, f, Options{Fields: Stat}), 4) // the runtime's own descriptors open once
	before := countFDs(t)
	for _, kept := range []bool{true, false} {
		for _, portable := range []bool{false, true} {
			for _, count := range []bool{false, true} {
				for _, end := range []string{"completed", "cancelled", "failed"} {
					name := fmt.Sprintf("kept=%v/portable=%v/count=%v/%s", kept, portable, count, end)
					o := mustOpenDir(t, f, Options{Fields: Stat, Portable: portable})
					if !kept {
						o.p.keep.limit = 0
					}
					ctx, cancel := context.WithCancel(context.Background())
					var sink order.Sink[Entry] = &collect{}
					switch end {
					case "cancelled":
						sink = &cancelAfter{n: 100, cancel: cancel}
					case "failed":
						sink = &failAfter{n: 100}
					}
					var err error
					if count {
						err = countOpened(ctx, o, 4, serial(sink))
					} else {
						_, err = order.Run[Range, Entry](ctx, o.p, sink, &Schema, order.Options{Workers: 4, Budget: 7 * 256})
					}
					cancel()
					if (err == nil) != (end == "completed") {
						t.Fatalf("%s: %v", name, err)
					}
					if n := countFDs(t); n != before || o.p.keep.open.Load() != 0 {
						t.Fatalf("%s: %d descriptors open, %d before; %d kept", name, n, before, o.p.keep.open.Load())
					}
				}
			}
		}
	}
	var s unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &s); err != nil {
		t.Fatalf("the caller's root: %v", err)
	}
}

func mustOpenDir(t *testing.T, f *os.File, opt Options) *Opened {
	t.Helper()
	o, err := OpenDir(f, "", opt)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

// TestStoppedWalkReportsUnread: a count stopped at any point reports every
// place it did not read: each key of the tree it did not list lies at or
// under an UnreadSubtree key, or below a child an UnreadListing directory
// did not list. Large directories make chunks and continuations to stop
// in.
func TestStoppedWalkReportsUnread(t *testing.T) {
	root := t.TempDir()
	for i := range 4 {
		for j := range 4 {
			mk(t, root, fmt.Sprintf("d%d/e%d/f", i, j), false)
		}
	}
	for i := range slabbed {
		mk(t, root, fmt.Sprintf("d1/big/f%05d", i), false)
	}
	for i := range chunked {
		mk(t, root, fmt.Sprintf("d2/mid/f%05d", i), false)
	}
	all := oracle(t, root, true)
	f := holdDir(t, root)
	for _, portable := range []bool{false, true} {
		for _, stop := range []int{1, 20, 3000, 9000, 15000} {
			var u unreads
			o := u.to(mustOpenDir(t, f, Options{Fields: Stat, Portable: portable}))
			ctx, cancel := context.WithCancel(context.Background())
			c := &cancelAfter{n: stop, cancel: cancel}
			got := &collect{}
			err := countOpened(ctx, o, 4, serial(c, got))
			cancel()
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("portable=%v stop=%d: %v", portable, stop, err)
			}
			if missing := unaccounted(all, rowKeys(got.rows), u.sorted()); len(missing) > 0 {
				t.Fatalf("portable=%v stop=%d: %d keys neither listed nor reported unread, first %q; unread %q",
					portable, stop, len(missing), missing[0], u.sorted())
			}
		}
	}
}

// serial hands each batch to every sink in turn, one batch at a time: a
// count calls its sink concurrently, and cancelAfter and failAfter are
// not safe for that.
func serial(s ...order.Sink[Entry]) order.Sink[Entry] { return &serialSinks{s: s} }

type serialSinks struct {
	mu sync.Mutex
	s  []order.Sink[Entry]
}

func (s *serialSinks) Write(ctx context.Context, segs [][]Row) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range s.s {
		if err := k.Write(ctx, segs); err != nil {
			return err
		}
	}
	return nil
}

// unaccounted is the keys of all neither listed nor in an unread place
// (each "scope class key"): at or under an UnreadSubtree key, or at or
// under a child of an UnreadListing directory that was not listed.
func unaccounted(all, listed, unread []string) []string {
	got := map[string]bool{}
	for _, k := range listed {
		got[k] = true
	}
	var missing []string
	for _, k := range all {
		if got[k] || covered(k, got, unread) {
			continue
		}
		missing = append(missing, k)
	}
	return missing
}

func covered(k string, listed map[string]bool, unread []string) bool {
	for _, u := range unread {
		f := strings.SplitN(u, " ", 3)
		scope, key := f[0], f[2]
		if !strings.HasPrefix(k, key) {
			continue
		}
		if scope == string(listing.UnreadSubtree) {
			return true
		}
		// The child of key on the way to k: covered unless it was listed.
		rest := k[len(key):]
		child := key + rest
		if i := strings.IndexByte(rest, '/'); i >= 0 {
			child = key + rest[:i+1]
		}
		if rest != "" && !listed[child] {
			return true
		}
	}
	return false
}

// TestLookup: each answer a lookup from a held root gives, by what is at
// the key and on the way to it.
func TestLookup(t *testing.T) {
	root := t.TempDir()
	mk(t, root, "f", false)
	mk(t, root, "d/g", false)
	mk(t, root, "locked/x", false)
	for _, l := range [][2]string{{"f", "ln"}, {"d", "lnd"}} {
		if err := os.Symlink(l[0], filepath.Join(root, l[1])); err != nil {
			t.Fatal(err)
		}
	}
	long := strings.Repeat("n", 300)
	o := mustOpenDir(t, holdDir(t, root), Options{Fields: Stat, LinkTarget: true})
	cases := []struct {
		key   string
		state listing.LookupState
		kind  Kind   // present
		row   string // present: the key it answers with
		class listing.UnreadClass
	}{
		{"f", listing.LookupPresent, KindFile, "f", ""},
		{"d", listing.LookupPresent, KindDir, "d/", ""},
		{"d/", listing.LookupPresent, KindDir, "d/", ""},
		{"d/g", listing.LookupPresent, KindFile, "d/g", ""},
		{"ln", listing.LookupPresent, KindSymlink, "ln", ""},
		{"lnd", listing.LookupPresent, KindSymlink, "lnd", ""},
		{"f/", listing.LookupAbsent, 0, "", ""},
		{"ln/", listing.LookupAbsent, 0, "", ""},
		{"nope", listing.LookupAbsent, 0, "", ""},
		{"nope/x", listing.LookupAbsent, 0, "", ""},
		{"d/nope/x", listing.LookupAbsent, 0, "", ""},
		{"f/x", listing.LookupAbsent, 0, "", ""},
		{"lnd/g", listing.LookupUnknown, 0, "", listing.UnreadSymlinkAncestor},
		{"ln/x", listing.LookupUnknown, 0, "", listing.UnreadSymlinkAncestor},
		{"d/" + long, listing.LookupUnknown, 0, "", listing.UnreadNameTooLong},
		{long + "/x", listing.LookupUnknown, 0, "", listing.UnreadNameTooLong},
	}
	if os.Getuid() != 0 {
		if err := os.Chmod(filepath.Join(root, "locked"), 0); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.Chmod(filepath.Join(root, "locked"), 0o755) })
		cases = append(cases, struct {
			key   string
			state listing.LookupState
			kind  Kind
			row   string
			class listing.UnreadClass
		}{"locked/x", listing.LookupUnknown, 0, "", listing.UnreadPermission})
	}
	for _, c := range cases {
		got, err := o.Lookup(context.Background(), c.key)
		if err != nil {
			t.Fatalf("%q: %v", c.key, err)
		}
		ok := got.State == c.state && got.Class == c.class
		if c.state == listing.LookupPresent {
			ok = ok && got.Row.Data.Kind == c.kind && got.Row.Key == c.row && got.Row.Data.Has(HasStat)
		}
		if !ok || (c.state == listing.LookupUnknown) != (got.Err != nil) {
			t.Errorf("%q: %v %q %q %v (kind %v), want %v %q %q", c.key, got.State, got.Class, got.Row.Key, got.Err, got.Row.Data.Kind, c.state, c.class, c.row)
		}
	}
	if got, _ := o.Lookup(context.Background(), "ln"); got.Row.Data.LinkTarget != "f" {
		t.Errorf("ln: target %q", got.Row.Data.LinkTarget)
	}
	for _, bad := range []string{"", "/f", "d//g", "./f", "d/../f", "f\x00"} {
		if _, err := o.Lookup(context.Background(), bad); err == nil {
			t.Errorf("key %q accepted", bad)
		}
	}
	p, err := New(root, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (&Opened{p: p}).Lookup(context.Background(), "f"); err == nil {
		t.Error("a lookup without a held root accepted")
	}
}

// TestLookupFailures: a lookup whose open of a directory on the way, its
// device check, or the stat of the key fails with EIO or EPERM, as a disk
// or a security module does, is unknown with the error's class, never
// absent, and closes every descriptor it opened.
func TestLookupFailures(t *testing.T) {
	root := t.TempDir()
	mk(t, root, "a/b/c", false)
	o := mustOpenDir(t, holdDir(t, root), Options{Fields: Stat})
	if got, err := o.Lookup(context.Background(), "a/b/c"); err != nil || got.State != listing.LookupPresent {
		t.Fatalf("a/b/c: %+v %v", got, err)
	}
	openat, fstat, stat := lookupOpenat, lookupFstat, lookupStatAt
	t.Cleanup(func() { lookupOpenat, lookupFstat, lookupStatAt = openat, fstat, stat })
	before := countFDs(t)
	for _, fail := range []unix.Errno{unix.EIO, unix.EPERM} {
		class := map[unix.Errno]listing.UnreadClass{unix.EIO: listing.UnreadIO, unix.EPERM: listing.UnreadPermission}[fail]
		for _, at := range []string{"open b", "fstat b", "stat c"} {
			lookupOpenat, lookupFstat, lookupStatAt = openat, fstat, stat
			switch at {
			case "open b":
				lookupOpenat = func(dir int, name string, flags int, mode uint32) (int, error) {
					if name == "b" {
						return -1, fail
					}
					return openat(dir, name, flags, mode)
				}
			case "fstat b":
				opened := 0
				lookupFstat = func(fd int, s *unix.Stat_t) error {
					if opened++; opened == 2 { // a's, then b's
						return fail
					}
					return fstat(fd, s)
				}
			case "stat c":
				lookupStatAt = func(dir int, name string, f Fields, r *Entry) error {
					if name == "c" {
						return fail
					}
					return stat(dir, name, f, r)
				}
			}
			got, err := o.Lookup(context.Background(), "a/b/c")
			if err != nil || got.State != listing.LookupUnknown || got.Class != class || !errors.Is(got.Err, fail) {
				t.Errorf("%v at %s: %v %q %v (%v), want unknown %q", fail, at, got.State, got.Class, got.Err, err, class)
			}
			if n := countFDs(t); n != before {
				t.Errorf("%v at %s: %d descriptors open, %d before", fail, at, n, before)
			}
		}
	}
}

// TestHeldOtherDevice: under a held /dev, a filesystem mounted on one of
// its directories is not entered: a prefix at or below the mount point
// reads nothing and is reported on another device, a lookup through it is
// unknown, and the mount point itself is present.
func TestHeldOtherDevice(t *testing.T) {
	const root = "/dev"
	var rs unix.Stat_t
	if err := unix.Stat(root, &rs); err != nil {
		t.Skip(err)
	}
	ents, err := os.ReadDir(root)
	if err != nil {
		t.Skip(err)
	}
	mount := ""
	for _, e := range ents {
		var s unix.Stat_t
		if e.IsDir() && unix.Lstat(root+"/"+e.Name(), &s) == nil && s.Dev != rs.Dev {
			mount = e.Name()
			break
		}
	}
	if mount == "" {
		t.Skip("no filesystem is mounted on a directory of /dev here")
	}
	f := holdDir(t, root)
	for _, prefix := range []string{mount + "/", mount + "/x/"} {
		var u unreads
		o := u.to(mustOpenDirAt(t, f, prefix, Options{Fields: Stat}))
		if keys := countKeys(t, o, 2); len(keys) != 0 || !slices.Equal(u.sorted(), []string{"subtree other_device " + prefix}) {
			t.Fatalf("prefix %q: keys %q, unread %q", prefix, keys, u.sorted())
		}
	}
	o := mustOpenDirAt(t, f, "", Options{Fields: Stat})
	if got, err := o.Lookup(context.Background(), mount+"/x"); err != nil || got.State != listing.LookupUnknown || got.Class != listing.UnreadOtherDevice {
		t.Fatalf("lookup through %s: %+v %v", mount, got, err)
	}
	if got, err := o.Lookup(context.Background(), mount); err != nil || got.State != listing.LookupPresent || got.Row.Key != mount+"/" {
		t.Fatalf("lookup of %s: %+v %v", mount, got, err)
	}
}

func mustOpenDirAt(t *testing.T, f *os.File, prefix string, opt Options) *Opened {
	t.Helper()
	o, err := OpenDir(f, prefix, opt)
	if err != nil {
		t.Fatal(err)
	}
	return o
}
