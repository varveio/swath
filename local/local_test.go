// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux || darwin

package local_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/varveio/swath"
	"github.com/varveio/swath/local"
	"github.com/varveio/swath/objfile"
	"github.com/varveio/swath/reader/listing"
)

// write makes the file rel below root, and the directories on the way.
func write(t *testing.T, root, rel string) {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
}

// symlink makes the link rel below root, pointing at target.
func symlink(t *testing.T, root, target, rel string) {
	t.Helper()
	if err := os.Symlink(target, filepath.Join(root, filepath.FromSlash(rel))); err != nil {
		t.Fatal(err)
	}
}

// hold opens dir as a caller of Open does, until the test ends.
func hold(t *testing.T, dir string) *os.File {
	t.Helper()
	f, err := os.OpenFile(dir, os.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return f
}

// open opens the directory f holds as a source, until the test ends.
func open(t *testing.T, f *os.File) *swath.Source {
	t.Helper()
	src, err := local.Open(f)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { src.Close() })
	return src
}

// level pages prefix whole, limit entries a page, and returns each entry
// as "kind key", Self first as "self kind key".
func level(ctx context.Context, src *swath.Source, prefix string, limit int) ([]string, error) {
	var got []string
	req := listing.PageRequest{Prefix: []byte(prefix), Limit: limit}
	for {
		page, err := src.Page(ctx, req)
		if err != nil {
			return got, err
		}
		if page.Self != nil {
			got = append(got, fmt.Sprintf("self %v %s", page.Self.Kind, page.Self.Key))
		}
		for _, e := range page.Entries {
			got = append(got, fmt.Sprintf("%v %s", e.Kind, e.Key))
		}
		if page.Next == "" {
			return got, nil
		}
		req.Token = page.Next
	}
}

// TestPage: a level in byte order of keys with "/" on directories, each
// child once, at any page size; each child is an entry of its kind with
// what lstat gives; the prefix's own directory is the first page's Self.
func TestPage(t *testing.T) {
	root := t.TempDir()
	for _, rel := range []string{"a-b", "a.b", "a/x", "ab", "e/x/y"} {
		write(t, root, rel)
	}
	symlink(t, root, "a", "ln")
	if err := unix.Mkfifo(filepath.Join(root, "p"), 0o644); err != nil {
		t.Fatal(err)
	}
	src := open(t, hold(t, root))
	ctx := context.Background()

	want := []string{"file a-b", "file a.b", "dir a/", "file ab", "dir e/", "symlink ln", "other p"}
	for _, limit := range []int{1, 2, 3, 100} {
		got, err := level(ctx, src, "", limit)
		if err != nil || !slices.Equal(got, want) {
			t.Fatalf("limit %d: %q %v, want %q", limit, got, err, want)
		}
		got, err = level(ctx, src, "e/", limit)
		if want := []string{"self dir e/", "dir e/x/"}; err != nil || !slices.Equal(got, want) {
			t.Fatalf("e/, limit %d: %q %v, want %q", limit, got, err, want)
		}
	}

	page, err := src.Page(ctx, listing.PageRequest{Prefix: []byte("a/"), Limit: 10})
	if err != nil || len(page.Entries) != 1 || page.Self == nil || page.Next != "" {
		t.Fatalf("a/: %+v %v", page, err)
	}
	f, self := page.Entries[0], page.Self
	if string(f.Key) != "a/x" || f.Kind != listing.KindFile || *f.Size != 5 || f.AllocatedBytes == nil ||
		f.LastModified == nil || f.Device == nil || f.Inode == nil || *f.Links != 1 {
		t.Errorf("a/x: %+v", f)
	}
	if self.Kind != listing.KindDir || *self.Device != *f.Device || self.Inode == nil || self.LastModified == nil {
		t.Errorf("self of a/: %+v", self)
	}
	if page.Order != listing.OrderBytes || page.Evidence.Origin != listing.OriginLive || page.Evidence.Stop != listing.StopCompleted {
		t.Errorf("a/: order %q, evidence %+v", page.Order, page.Evidence)
	}
}

// TestPageByteName: a name that is not UTF-8 pages, as a file and as a
// directory's prefix, and is looked up.
func TestPageByteName(t *testing.T) {
	root := t.TempDir()
	name := "n\xff"
	write(t, root, name)
	write(t, root, name+"d/x")
	src := open(t, hold(t, root))
	ctx := context.Background()

	if got, err := level(ctx, src, "", 10); err != nil || !slices.Equal(got, []string{"file " + name, "dir " + name + "d/"}) {
		t.Errorf("the root: %q %v", got, err)
	}
	if got, err := level(ctx, src, name+"d/", 10); err != nil || !slices.Equal(got, []string{"self dir " + name + "d/", "file " + name + "d/x"}) {
		t.Errorf("%q: %q %v", name+"d/", got, err)
	}
	r, err := src.Lookup(ctx, []byte(name))
	if err != nil || r.State != listing.LookupPresent || string(r.Entry.Key) != name || r.Entry.Kind != listing.KindFile {
		t.Errorf("lookup %q: %+v %v", name, r, err)
	}
}

// TestPageChildDirectory: a child directory is a KindDir entry with what
// lstat gives of it, its modification time included.
func TestPageChildDirectory(t *testing.T) {
	root := t.TempDir()
	write(t, root, "d/x")
	when := time.Date(2024, 5, 6, 7, 8, 9, 0, time.UTC)
	if err := os.Chtimes(filepath.Join(root, "d"), when, when); err != nil {
		t.Fatal(err)
	}
	src := open(t, hold(t, root))

	page, err := src.Page(context.Background(), listing.PageRequest{Limit: 10})
	if err != nil || len(page.Entries) != 1 {
		t.Fatalf("the root: %+v %v", page, err)
	}
	d := page.Entries[0]
	if string(d.Key) != "d/" || d.Kind != listing.KindDir || d.LastModified == nil || !d.LastModified.Equal(when) || d.Inode == nil {
		t.Errorf("d/: %+v", d)
	}
}

// TestPageErrors: a prefix that cannot be listed fails in its class, and
// the message names no more of the path than the caller's key.
func TestPageErrors(t *testing.T) {
	root := t.TempDir()
	write(t, root, "f")
	write(t, root, "d/g")
	write(t, root, "locked/x")
	long := strings.Repeat("n", 300)
	cases := []struct {
		prefix string
		class  error
	}{
		{"nope/", listing.ErrNotFound},
		{"nope/x/", listing.ErrNotFound},
		{"f/", listing.ErrNotDirectory},
		{"f/x/", listing.ErrNotFound},
		{"d/g/", listing.ErrNotDirectory},
		{long + "/", listing.ErrUnsupported},
		{"d/" + long + "/x/", listing.ErrUnsupported},
		{"d", listing.ErrInvalidRequest},
		{"d//", listing.ErrInvalidRequest},
		{"./d/", listing.ErrInvalidRequest},
		{"d/../", listing.ErrInvalidRequest},
	}
	if os.Getuid() != 0 {
		if err := os.Chmod(filepath.Join(root, "locked"), 0); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.Chmod(filepath.Join(root, "locked"), 0o755) })
		cases = append(cases, struct {
			prefix string
			class  error
		}{"locked/", listing.ErrAuth}, struct {
			prefix string
			class  error
		}{"locked/x/", listing.ErrAuth})
	}
	src := open(t, hold(t, root))
	for _, c := range cases {
		_, err := src.Page(context.Background(), listing.PageRequest{Prefix: []byte(c.prefix), Limit: 10})
		if !errors.Is(err, c.class) || strings.Contains(err.Error(), root) {
			t.Errorf("%q: %v, want %v", c.prefix, err, c.class)
		}
	}
	if _, err := local.Open(nil); !errors.Is(err, listing.ErrInvalidRequest) {
		t.Errorf("no root: %v", err)
	}
	file, err := os.Open(filepath.Join(root, "f"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := local.Open(file); !errors.Is(err, listing.ErrNotDirectory) {
		t.Errorf("a file root: %v", err)
	}
}

// TestLookup: each of the three answers, by what is at the key and on
// the way to it.
func TestLookup(t *testing.T) {
	root := t.TempDir()
	write(t, root, "f")
	write(t, root, "d/g")
	write(t, root, "locked/x")
	symlink(t, root, "d", "lnd")
	long := strings.Repeat("n", 300)
	cases := []struct {
		key   string
		state listing.LookupState
		want  string // present: "kind key"; unknown: the class, "" for none
	}{
		{"f", listing.LookupPresent, "file f"},
		{"d", listing.LookupPresent, "dir d/"},
		{"d/", listing.LookupPresent, "dir d/"},
		{"lnd", listing.LookupPresent, "symlink lnd"},
		{"lnd/", listing.LookupAbsent, ""},
		{"f/", listing.LookupAbsent, ""},
		{"f/x", listing.LookupAbsent, ""},
		{"nope", listing.LookupAbsent, ""},
		{"nope/x", listing.LookupAbsent, ""},
		{"lnd/g", listing.LookupUnknown, listing.ErrNotDirectory.Error()},
		{long + "/x", listing.LookupUnknown, listing.ErrUnsupported.Error()},
	}
	if os.Getuid() != 0 {
		if err := os.Chmod(filepath.Join(root, "locked"), 0); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.Chmod(filepath.Join(root, "locked"), 0o755) })
		cases = append(cases, struct {
			key   string
			state listing.LookupState
			want  string
		}{"locked/x", listing.LookupUnknown, listing.ErrAuth.Error()})
	}
	src := open(t, hold(t, root))
	classes := []error{listing.ErrAuth, listing.ErrUnsupported, listing.ErrUnavailable, listing.ErrOtherDevice, listing.ErrNotFound, listing.ErrNotDirectory}
	for _, c := range cases {
		r, err := src.Lookup(context.Background(), []byte(c.key))
		if err != nil || r.State != c.state {
			t.Errorf("%q: %v %v, want %v", c.key, r.State, err, c.state)
			continue
		}
		// An unknown answer's evidence must not read as complete.
		if complete := r.Evidence.Stop == listing.StopCompleted && r.Evidence.Unlocated != nil && *r.Evidence.Unlocated == 0; complete == (r.State == listing.LookupUnknown) {
			t.Errorf("%q: %v with evidence %+v", c.key, r.State, r.Evidence)
		}
		switch r.State {
		case listing.LookupPresent:
			if got := fmt.Sprintf("%v %s", r.Entry.Kind, r.Entry.Key); got != c.want || r.Entry.Size == nil || r.Entry.Inode == nil {
				t.Errorf("%q: %+v, want %s", c.key, r.Entry, c.want)
			}
		case listing.LookupUnknown:
			class := ""
			for _, k := range classes {
				if errors.Is(r.Reason, k) {
					class = k.Error()
				}
			}
			if r.Reason == nil || class != c.want || strings.Contains(r.Reason.Error(), root) {
				t.Errorf("%q: reason %v, want class %q", c.key, r.Reason, c.want)
			}
		}
	}
	for _, bad := range []string{"/f", "d//g", "./f", "d/../f"} {
		if _, err := src.Lookup(context.Background(), []byte(bad)); !errors.Is(err, listing.ErrInvalidRequest) {
			t.Errorf("%q: %v", bad, err)
		}
	}
}

// TestRootNotResolvedByPath: after the caller opened the root, its path is
// moved aside and replaced by a symbolic link to another directory; pages
// and lookups still answer from the directory the caller opened.
func TestRootNotResolvedByPath(t *testing.T) {
	base := t.TempDir()
	root, outside := filepath.Join(base, "root"), filepath.Join(base, "outside")
	write(t, root, "in/a")
	write(t, outside, "secret")
	write(t, outside, "in/secret")
	f := hold(t, root)
	if err := os.Rename(root, root+".moved"); err != nil {
		t.Fatal(err)
	}
	symlink(t, base, outside, "root")
	src := open(t, f)
	ctx := context.Background()

	if got, err := level(ctx, src, "", 10); err != nil || !slices.Equal(got, []string{"dir in/"}) {
		t.Errorf("the root: %q %v", got, err)
	}
	if got, err := level(ctx, src, "in/", 10); err != nil || !slices.Equal(got, []string{"self dir in/", "file in/a"}) {
		t.Errorf("in/: %q %v", got, err)
	}
	for key, state := range map[string]listing.LookupState{"in/a": listing.LookupPresent, "secret": listing.LookupAbsent, "in/secret": listing.LookupAbsent} {
		if r, err := src.Lookup(ctx, []byte(key)); err != nil || r.State != state {
			t.Errorf("%q: %v %v, want %v", key, r.State, err, state)
		}
	}
}

// TestSymlinkPrefixNotFollowed: a prefix that is a symbolic link, or has
// one on the way, is not followed: the link is no directory, and a page or
// a lookup through it is not reached, in that class, neither listed nor
// shown absent.
func TestSymlinkPrefixNotFollowed(t *testing.T) {
	root := t.TempDir()
	write(t, root, "real/sub/x")
	symlink(t, root, "real", "lnk")
	src := open(t, hold(t, root))
	ctx := context.Background()

	got, err := level(ctx, src, "lnk/", 10)
	if !errors.Is(err, listing.ErrNotDirectory) || len(got) != 0 {
		t.Errorf("lnk/: %q %v, want ErrNotDirectory", got, err)
	}
	got, err = level(ctx, src, "lnk/sub/", 10)
	if !errors.Is(err, listing.ErrNotDirectory) || errors.Is(err, listing.ErrNotFound) || len(got) != 0 {
		t.Errorf("lnk/sub/: %q %v, want ErrNotDirectory (not reached)", got, err)
	}
	if r, err := src.Lookup(ctx, []byte("lnk/sub/x")); err != nil || r.State != listing.LookupUnknown || !errors.Is(r.Reason, listing.ErrNotDirectory) {
		t.Errorf("lnk/sub/x: %+v %v, want unknown, ErrNotDirectory", r, err)
	}
}

// TestReplacedBetweenPages: a directory, or a directory on the way to it,
// replaced by a symbolic link between two pages of its level is not
// followed by the next page.
func TestReplacedBetweenPages(t *testing.T) {
	for _, c := range []struct {
		prefix, replaced string
		class            error
	}{
		{"d/", "d", listing.ErrNotDirectory},
		{"p/d/", "p", listing.ErrNotDirectory},
	} {
		base := t.TempDir()
		root, outside := filepath.Join(base, "root"), filepath.Join(base, "outside")
		for i := range 10 {
			write(t, root, fmt.Sprintf("%sf%02d", c.prefix, i))
			write(t, outside, fmt.Sprintf("%sz%02d", c.prefix, i)) // under outside/d/ and outside/p/d/
		}
		src := open(t, hold(t, root))
		ctx := context.Background()

		page, err := src.Page(ctx, listing.PageRequest{Prefix: []byte(c.prefix), Limit: 3})
		if err != nil || page.Next == "" {
			t.Fatalf("%s: first page %+v %v", c.prefix, page, err)
		}
		at := filepath.Join(root, c.replaced)
		if err := os.Rename(at, at+".old"); err != nil {
			t.Fatal(err)
		}
		symlink(t, root, filepath.Join(outside, c.replaced), c.replaced)
		next, err := src.Page(ctx, listing.PageRequest{Prefix: []byte(c.prefix), Limit: 3, Token: page.Next})
		if !errors.Is(err, c.class) || len(next.Entries) != 0 {
			t.Errorf("%s replaced by a link: %+v %v, want %v", c.replaced, next.Entries, err, c.class)
		}
	}
}

// TestChangedBetweenPages: a level paged while entries are removed from
// and added to its directory repeats no key, keeps byte order across
// pages, and skips no name that stayed, before or after the page boundary.
func TestChangedBetweenPages(t *testing.T) {
	root := t.TempDir()
	for i := range 20 {
		write(t, root, fmt.Sprintf("c/f%02d", i))
	}
	src := open(t, hold(t, root))
	ctx := context.Background()

	req := listing.PageRequest{Prefix: []byte("c/"), Limit: 5}
	var keys []string
	for i := 0; ; i++ {
		page, err := src.Page(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range page.Entries {
			keys = append(keys, string(e.Key))
		}
		if i == 0 { // f00 to f03 returned, after the directory's own entry
			for _, name := range []string{"f01", "f02", "f07"} {
				if err := os.Remove(filepath.Join(root, "c", name)); err != nil {
					t.Fatal(err)
				}
			}
			for _, name := range []string{"f00a", "f10a"} {
				write(t, root, "c/"+name)
			}
		}
		if page.Next == "" {
			break
		}
		req.Token = page.Next
	}

	if !slices.IsSorted(keys) || len(slices.Compact(slices.Clone(keys))) != len(keys) {
		t.Fatalf("keys repeat or are out of order: %q", keys)
	}
	for i := range 20 {
		if k := fmt.Sprintf("c/f%02d", i); k != "c/f07" && !slices.Contains(keys, k) {
			t.Errorf("%s stayed and is not listed: %q", k, keys)
		}
	}
	if !slices.Contains(keys, "c/f10a") || slices.Contains(keys, "c/f07") || len(keys) != 4+15+1 {
		t.Errorf("an entry added after the boundary is not listed, or one removed before it is: %q", keys)
	}
}

// openFDs counts the process's open descriptors (Linux: /proc/self/fd;
// macOS: /dev/fd), the one reading them included.
func openFDs(t *testing.T) int {
	t.Helper()
	dir := "/proc/self/fd"
	if runtime.GOOS == "darwin" {
		dir = "/dev/fd"
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	return len(ents)
}

// TestDescriptorsClosed: each page and lookup closes every descriptor it
// opened, whether it answers, fails or is cancelled, and leaves the
// caller's root open.
func TestDescriptorsClosed(t *testing.T) {
	root := t.TempDir()
	write(t, root, "a/b/c/f")
	write(t, root, "f")
	for i := range 3000 {
		write(t, root, fmt.Sprintf("w/f%04d", i))
	}
	symlink(t, root, "a", "lnk")
	f := hold(t, root)
	src := open(t, f)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	ctx := context.Background()

	calls := []func() error{
		func() error { _, err := level(ctx, src, "a/b/c/", 1); return err },
		func() error { _, err := level(ctx, src, "w/", 10); return err },
		func() error { _, err := level(ctx, src, "a/b/nope/", 1); return err },
		func() error { _, err := level(ctx, src, "a/f/", 1); return err },
		func() error { _, err := level(ctx, src, "lnk/b/", 1); return err },
		func() error { _, err := level(cancelled, src, "a/b/c/", 1); return err },
		func() error { _, err := src.Lookup(ctx, []byte("a/b/c/f")); return err },
		func() error { _, err := src.Lookup(ctx, []byte("lnk/b/c")); return err },
		func() error { _, err := src.Lookup(cancelled, []byte("a/b/c/f")); return err },
	}
	calls[0]() // the runtime's own descriptors open once
	before := openFDs(t)
	for i, call := range calls {
		err := call()
		if n := openFDs(t); n != before {
			t.Errorf("call %d (%v): %d descriptors open, %d before", i, err, n, before)
		}
	}
	var s unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &s); err != nil {
		t.Errorf("the caller's root: %v", err)
	}
}

// TestConcurrentPages: pages of one source, of one level and of several,
// called at once, each read through its own descriptor, answer as they
// do one at a time.
func TestConcurrentPages(t *testing.T) {
	root := t.TempDir()
	for i := range 200 {
		write(t, root, fmt.Sprintf("f%03d", i))
		write(t, root, fmt.Sprintf("d/g%03d", i))
	}
	src := open(t, hold(t, root))
	ctx := context.Background()
	want := map[string][]string{}
	for _, prefix := range []string{"", "d/"} {
		got, err := level(ctx, src, prefix, 7)
		if err != nil {
			t.Fatal(err)
		}
		want[prefix] = got
	}

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := range 8 {
		prefix := []string{"", "d/"}[i%2]
		wg.Go(func() {
			got, err := level(ctx, src, prefix, 7+i)
			if err != nil || !slices.Equal(got, want[prefix]) {
				errs <- fmt.Errorf("%q, limit %d: %d entries, %v", prefix, 7+i, len(got), err)
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestTokens: a token continues only the level of the source and prefix
// it was issued for.
func TestTokens(t *testing.T) {
	a, b := t.TempDir(), t.TempDir()
	for _, root := range []string{a, b} {
		for i := range 5 {
			write(t, root, fmt.Sprintf("d/f%d", i))
			write(t, root, fmt.Sprintf("e/f%d", i))
		}
	}
	src := open(t, hold(t, a))
	other := open(t, hold(t, b))
	ctx := context.Background()
	page, err := src.Page(ctx, listing.PageRequest{Prefix: []byte("d/"), Limit: 2})
	if err != nil || page.Next == "" {
		t.Fatalf("%+v %v", page, err)
	}
	for name, c := range map[string]struct {
		src    *swath.Source
		prefix string
	}{"another prefix": {src, "e/"}, "another root": {other, "d/"}} {
		if _, err := c.src.Page(ctx, listing.PageRequest{Prefix: []byte(c.prefix), Limit: 2, Token: page.Next}); !errors.Is(err, listing.ErrInvalidToken) {
			t.Errorf("%s: %v, want ErrInvalidToken", name, err)
		}
	}
}

// observed records the requests an objfile.Observer is told of.
type observed struct {
	mu   sync.Mutex
	seen []string
}

func (o *observed) Begin(r objfile.Request) func(objfile.RequestResult) {
	return func(res objfile.RequestResult) {
		o.mu.Lock()
		defer o.mu.Unlock()
		o.seen = append(o.seen, fmt.Sprintf("%v %q failed=%t", r.Kind, r.Name, res.Err != nil))
	}
}

func (o *observed) BatchPlanned(int64, int64, int64) {}

func (o *observed) CacheHit(int64) {}

// TestObserved: a page is one list request, a lookup one metadata
// request, each named by the caller's key.
func TestObserved(t *testing.T) {
	root := t.TempDir()
	write(t, root, "d/f")
	src := open(t, hold(t, root))
	obs := &observed{}
	ctx := objfile.WithObserver(context.Background(), obs)
	src.Page(ctx, listing.PageRequest{Prefix: []byte("d/"), Limit: 10})
	src.Page(ctx, listing.PageRequest{Prefix: []byte("nope/"), Limit: 10})
	src.Lookup(ctx, []byte("d/f"))
	want := []string{`list "d/" failed=false`, `list "nope/" failed=true`, `metadata "d/f" failed=false`}
	if !slices.Equal(obs.seen, want) {
		t.Errorf("observed %q, want %q", obs.seen, want)
	}
}

// tree makes a small tree below a new root: files, a nested and an empty
// directory, a symbolic link to a directory, and, unless the test runs as
// root, a directory it may not read holding a file.
func tree(t *testing.T) (root string, locked bool) {
	t.Helper()
	root = t.TempDir()
	write(t, root, "a.txt")
	write(t, root, "d/b.txt")
	write(t, root, "d/e/c.txt")
	write(t, root, "locked/x.txt")
	if err := os.Mkdir(filepath.Join(root, "empty"), 0o755); err != nil {
		t.Fatal(err)
	}
	symlink(t, root, "d", "link")
	if os.Getuid() == 0 {
		return root, false
	}
	if err := os.Chmod(filepath.Join(root, "locked"), 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(filepath.Join(root, "locked"), 0o755) })
	return root, true
}

// walkDir is what filepath.WalkDir finds below root: every key, a
// directory's ending in "/", and the files' count and bytes.
func walkDir(t *testing.T, root string) (keys []string, files, bytes int64) {
	t.Helper()
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil || p == root {
			return nil // a directory it cannot read is reported again, after its own entry
		}
		key := filepath.ToSlash(p[len(root)+1:])
		if d.IsDir() {
			key += "/"
		}
		keys = append(keys, key)
		if d.Type().IsRegular() {
			info, err := d.Info()
			if err != nil {
				t.Fatal(err)
			}
			files++
			bytes += info.Size()
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(keys)
	return keys, files, bytes
}

// TestWalk: a walk of the root, and of a directory below it, hands on
// every entry filepath.WalkDir finds there, the directory's own below the
// root, the symbolic link as itself,
// and the directory it may not read as an Unread of class ErrAuth. It
// leaves GOMAXPROCS as it was.
func TestWalk(t *testing.T) {
	root, locked := tree(t)
	src := open(t, hold(t, root))
	want, _, _ := walkDir(t, root)
	procs := runtime.GOMAXPROCS(0)
	defer func() {
		if now := runtime.GOMAXPROCS(0); now != procs {
			t.Errorf("GOMAXPROCS %d after walks, %d before", now, procs)
		}
	}()
	for _, workers := range []int{0, 1} {
		var got, unread []string
		sum, err := src.Walk(context.Background(), listing.WalkRequest{Workers: workers}, func(b listing.WalkBatch) error {
			for _, e := range b.Entries {
				got = append(got, string(e.Key))
			}
			for _, u := range b.Unread {
				if !errors.Is(u.Reason, listing.ErrAuth) {
					t.Errorf("%q: reason %v", u.Key, u.Reason)
				}
				unread = append(unread, string(u.Key))
			}
			return nil
		})
		slices.Sort(got)
		if err != nil || !slices.Equal(got, want) || sum.Entries != int64(len(want)) || sum.Evidence.Stop != listing.StopCompleted {
			t.Fatalf("workers %d: %q, %+v, %v; want %q", workers, got, sum, err, want)
		}
		if wantUnread := map[bool]int{true: 1}[locked]; len(unread) != wantUnread || sum.Unread != int64(wantUnread) || locked && unread[0] != "locked/" {
			t.Errorf("workers %d: unread %q, counted %d", workers, unread, sum.Unread)
		}
	}

	var got []string
	if _, err := src.Walk(context.Background(), listing.WalkRequest{Prefix: []byte("d/")}, func(b listing.WalkBatch) error {
		for _, e := range b.Entries {
			got = append(got, string(e.Key))
		}
		return nil
	}); err != nil || !slices.Equal(slices.Sorted(slices.Values(got)), []string{"d/", "d/b.txt", "d/e/", "d/e/c.txt"}) {
		t.Errorf("walk of d/: %q, %v", got, err)
	}
	emit := func(listing.WalkBatch) error { return nil }
	for prefix, class := range map[string]error{"d": listing.ErrInvalidRequest, "nope/": listing.ErrNotFound, "a.txt/": listing.ErrNotDirectory, "link/": listing.ErrNotDirectory} {
		if _, err := src.Walk(context.Background(), listing.WalkRequest{Prefix: []byte(prefix)}, emit); !errors.Is(err, class) {
			t.Errorf("walk of %q: %v, want %v", prefix, err, class)
		}
	}
}

// TestWalkCancelled: a walk whose context ends returns the context's
// error, and its summary says it did not finish.
func TestWalkCancelled(t *testing.T) {
	root := t.TempDir()
	for i := range 200 {
		write(t, root, fmt.Sprintf("d%03d/f", i))
	}
	src := open(t, hold(t, root))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sum, err := src.Walk(ctx, listing.WalkRequest{Workers: 2}, func(listing.WalkBatch) error {
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) || sum.Evidence.Stop == listing.StopCompleted || sum.Entries == 0 {
		t.Fatalf("%+v, %v", sum, err)
	}
}

// TestSummarize: a target's totals are what filepath.WalkDir counts
// under it; one with a directory it may not read under it is not
// complete and counts it, one without is complete; a subtree's state is a
// lookup of its directory.
func TestSummarize(t *testing.T) {
	root, locked := tree(t)
	src := open(t, hold(t, root))
	_, files, bytes := walkDir(t, root)
	targets := []listing.Target{{Subtree: true}, {Key: []byte("d/"), Subtree: true}, {Key: []byte("empty/"), Subtree: true}, {Key: []byte("nope/"), Subtree: true}, {Key: []byte("a.txt")}}
	sums, ev, err := src.Summarize(context.Background(), targets, listing.SummaryOptions{})
	if err != nil || ev.Stop != listing.StopCompleted {
		t.Fatalf("%+v, %v", ev, err)
	}

	if s := sums[0]; s.Entries != files || s.Bytes == nil || *s.Bytes != bytes || s.Complete == locked || s.Unread != map[bool]int64{true: 1}[locked] {
		t.Errorf("the root: %d files, bytes %v, complete %t, unread %d; want %d, %d", s.Entries, s.Bytes, s.Complete, s.Unread, files, bytes)
	}
	for i, want := range []struct {
		state   listing.LookupState
		entries int64
	}{{}, {listing.LookupPresent, 2}, {listing.LookupPresent, 0}, {listing.LookupAbsent, 0}, {listing.LookupPresent, 1}} {
		if s := sums[i]; i > 0 && (s.State != want.state || s.Entries != want.entries || !s.Complete || s.Unread != 0) {
			t.Errorf("%q: %v, %d entries, complete %t, unread %d; want %v, %d", targets[i].Key, s.State, s.Entries, s.Complete, s.Unread, want.state, want.entries)
		}
	}
	if s := sums[1]; s.Self == nil || string(s.Self.Key) != "d/" || s.Self.Kind != listing.KindDir {
		t.Errorf("d/: self %+v", s.Self)
	}

	dirs, _, err := src.Summarize(context.Background(), targets[1:2], listing.SummaryOptions{Kinds: []listing.Kind{listing.KindDir}})
	if err != nil || dirs[0].Entries != 2 { // d/ itself and d/e/
		t.Errorf("d/, counting directories: %+v, %v", dirs, err)
	}
}
