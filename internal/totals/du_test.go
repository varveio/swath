// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package totals_test

import (
	"testing"
	"time"

	"github.com/varveio/swath/internal/totals"
	"github.com/varveio/swath/reader/du"
	"github.com/varveio/swath/reader/listing"
)

// row is entry e as swath du's binding makes it a Row (internal/bind's
// totalsSink): a directory is a Dir and every other entry an Object, and
// a local entry's links are told apart by its link count, device and
// inode, or it is Unidentified. A common prefix is no row of a dataset.
func row(e listing.Entry) (totals.Row, bool) {
	if e.Kind == listing.KindPrefix {
		return totals.Row{}, false
	}

	r := totals.Row{Key: string(e.Key), Object: e.Kind != listing.KindDir, Dir: e.Kind == listing.KindDir}
	if e.Size != nil {
		r.Size, r.HasSize = *e.Size, true
	}
	if e.AllocatedBytes != nil {
		r.Allocated, r.HasAllocated = *e.AllocatedBytes, true
	}
	if e.LastModified != nil {
		r.Mtime, r.HasMtime = e.LastModified.UnixMicro(), true
	}
	if r.Object && e.Kind != listing.KindObject {
		if e.Links != nil && e.Device != nil && e.Inode != nil {
			r.Link, r.Dev, r.Inode = *e.Links > 1, int64(*e.Device), int64(*e.Inode)
		} else {
			r.Unidentified = true
		}
	}
	return r, true
}

// command is what swath du gives for one prefix.
type command struct {
	Objects, Size, SizeMissing, Allocated, AllocatedMissing int64
}

func ptr[T any](v T) *T { return &v }

// entry is a local entry; links 0 leaves its link count, device and inode
// unknown.
func entry(k listing.Kind, key string, size, alloc int64, links uint32, ino uint64) listing.Entry {
	e := listing.Entry{Key: []byte(key), Kind: k, Size: ptr(size), AllocatedBytes: ptr(alloc), LastModified: ptr(time.Unix(1000, 0))}
	if links > 0 {
		e.Links, e.Device, e.Inode = ptr(links), ptr(uint64(1)), ptr(ino)
	}
	return e
}

// TestDuAgainstTheCommand feeds one fixture per kind of evidence to
// package du and to swath du's tally (LinksOnce, led by the space on disk,
// lenient), each figure written by hand: their bytes agree where the
// contracts do, and each keeps its own counts where they differ on
// purpose.
func TestDuAgainstTheCommand(t *testing.T) {
	noAlloc := entry(listing.KindFile, "p/x", 4000, 0, 1, 1)
	noAlloc.AllocatedBytes = nil
	for _, c := range []struct {
		name    string
		objects bool // an object store's: no allocation, no directories
		depth   int  // 0: the command's total over "p/"; 1: its prefixes under ""
		entries []listing.Entry
		du      []du.Totals
		command []command
		// agree names the figures both give the same value.
		agree []string
	}{
		{
			name: "single-link files",
			entries: []listing.Entry{
				entry(listing.KindFile, "p/x", 4000, 4096, 1, 1),
				entry(listing.KindFile, "p/y", 10, 512, 1, 2),
			},
			du:      []du.Totals{{Prefix: "p/", Files: 2, Size: 4010, Allocated: 4608}},
			command: []command{{Objects: 2, Size: 4010, Allocated: 4608}},
			agree:   []string{"size", "allocated"},
		},
		{
			name: "directories' own blocks",
			entries: []listing.Entry{
				{Key: []byte("p/"), Kind: listing.KindDir, AllocatedBytes: ptr(int64(4096))},
				{Key: []byte("p/d/"), Kind: listing.KindDir, AllocatedBytes: ptr(int64(4096))},
				entry(listing.KindFile, "p/d/x", 4000, 4096, 1, 1),
			},
			du:      []du.Totals{{Prefix: "p/", Files: 1, Size: 4000, Allocated: 12288}},
			command: []command{{Objects: 1, Size: 4000, Allocated: 12288}},
			agree:   []string{"size", "allocated"},
		},
		{
			// du counts neither in any figure; the command counts the
			// file and its length.
			name: "missing allocation",
			entries: []listing.Entry{
				noAlloc,
				{Key: []byte("p/d/"), Kind: listing.KindDir},
			},
			du:      []du.Totals{{Prefix: "p/", AllocatedMissing: 2}},
			command: []command{{Objects: 1, Size: 4000, AllocatedMissing: 2}},
			agree:   []string{"allocated", "allocated missing"},
		},
		{
			name: "a linked file",
			entries: []listing.Entry{
				entry(listing.KindFile, "p/x", 4000, 4096, 2, 7),
				entry(listing.KindFile, "p/y", 4000, 4096, 2, 7),
			},
			du:      []du.Totals{{Prefix: "p/", Files: 1, Size: 4000, Allocated: 4096}},
			command: []command{{Objects: 1, Size: 4000, Allocated: 4096}},
			agree:   []string{"size", "allocated"},
		},
		{
			// du counts the names as symbolic links and no file; the
			// command counts one object.
			name: "a linked symbolic link",
			entries: []listing.Entry{
				entry(listing.KindSymlink, "p/l1", 7, 512, 2, 8),
				entry(listing.KindSymlink, "p/l2", 7, 512, 2, 8),
			},
			du:      []du.Totals{{Prefix: "p/", Symlinks: 2, Size: 7, Allocated: 512}},
			command: []command{{Objects: 1, Size: 7, Allocated: 512}},
			agree:   []string{"size", "allocated"},
		},
		{
			// du excludes a name it cannot tell apart; the command counts
			// it under its name.
			name:    "an unidentified file",
			entries: []listing.Entry{entry(listing.KindFile, "p/x", 4000, 4096, 0, 0)},
			du:      []du.Totals{{Prefix: "p/", Unidentified: 1}},
			command: []command{{Objects: 1, Size: 4000, Allocated: 4096}},
		},
		{
			// du ignores an entry of unknown kind; the command counts an
			// object without figures.
			name:    "an entry of unknown kind",
			entries: []listing.Entry{{Key: []byte("p/u"), Kind: listing.KindUnknown}},
			du:      []du.Totals{{Prefix: "p/"}},
			command: []command{{Objects: 1, SizeMissing: 1, AllocatedMissing: 1}},
			agree:   []string{"size", "allocated"},
		},
		{
			name:    "another entry",
			entries: []listing.Entry{entry(listing.KindOther, "p/fifo", 0, 0, 1, 3)},
			du:      []du.Totals{{Prefix: "p/"}},
			command: []command{{Objects: 1}},
			agree:   []string{"size", "allocated", "allocated missing"},
		},
		{
			// The command counts objects as missing their allocation.
			name:    "objects",
			objects: true,
			entries: []listing.Entry{
				{Key: []byte("p/"), Kind: listing.KindObject, Size: ptr(int64(1))},
				{Key: []byte("p/a"), Kind: listing.KindObject, Size: ptr(int64(10))},
				{Key: []byte("p/b/"), Kind: listing.KindPrefix},
				{Key: []byte("p/b/c"), Kind: listing.KindObject, Size: ptr(int64(100))},
			},
			du:      []du.Totals{{Prefix: "p/", Files: 3, Size: 111, NewestMissing: 3}},
			command: []command{{Objects: 3, Size: 111, AllocatedMissing: 3}},
			agree:   []string{"files", "size", "allocated"},
		},
		{
			// du counts the file within each prefix; the command once,
			// under its smallest name.
			name:  "a linked file in two sibling prefixes",
			depth: 1,
			entries: []listing.Entry{
				entry(listing.KindFile, "a/x", 4000, 4096, 2, 9),
				entry(listing.KindFile, "b/y", 4000, 4096, 2, 9),
			},
			du: []du.Totals{
				{Prefix: "a/", Files: 1, Size: 4000, Allocated: 4096},
				{Prefix: "b/", Files: 1, Size: 4000, Allocated: 4096},
			},
			command: []command{{Objects: 1, Size: 4000, Allocated: 4096}, {}},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := tallyDu(t, c.du, c.entries)
			cmd := tallyCommand(t, c.objects, c.depth, c.du, c.entries)
			for i, want := range c.du {
				if g := got[i]; g != want {
					t.Errorf("du %q:\n got %+v\nwant %+v", want.Prefix, g, want)
				}
				if cmd[i] != c.command[i] {
					t.Errorf("command %q:\n got %+v\nwant %+v", want.Prefix, cmd[i], c.command[i])
				}
				figures := map[string][2]int64{
					"files":             {got[i].Files, cmd[i].Objects},
					"size":              {got[i].Size, cmd[i].Size},
					"allocated":         {got[i].Allocated, cmd[i].Allocated},
					"allocated missing": {got[i].AllocatedMissing, cmd[i].AllocatedMissing},
				}
				for _, f := range c.agree {
					if v := figures[f]; v[0] != v[1] {
						t.Errorf("%q: %s is %d by du, %d by the command", want.Prefix, f, v[0], v[1])
					}
				}
			}
		})
	}
}

// tallyDu sums entries with package du over want's prefixes, dropping
// Newest (the fixtures' times are all one).
func tallyDu(t *testing.T, want []du.Totals, entries []listing.Entry) []du.Totals {
	t.Helper()
	var prefixes []string
	for _, w := range want {
		prefixes = append(prefixes, w.Prefix)
	}
	tl, err := du.New(du.Options{Prefixes: prefixes})
	if err != nil {
		t.Fatal(err)
	}
	if err := tl.Add(entries); err != nil {
		t.Fatal(err)
	}
	got := tl.Result()
	for i := range got {
		got[i].Newest = nil
	}
	return got
}

// tallyCommand sums entries with swath du's tally, at depth 0 from "p/"
// or depth 1 from "", and returns its figures for want's prefixes.
func tallyCommand(t *testing.T, objects bool, depth int, want []du.Totals, entries []listing.Entry) []command {
	t.Helper()
	opt := totals.Options{Origin: "p/", LinksOnce: true, Primary: totals.AllocatedBytes, Lenient: true}
	if depth > 0 {
		opt.Origin, opt.Depth = "", depth
	}
	if objects {
		opt.Primary = totals.Size
	}
	tl := totals.New(opt)
	if !objects {
		tl.SetDirs()
		tl.SetAllocated()
	}

	b := tl.Batch()
	for _, e := range entries {
		if r, ok := row(e); ok {
			b.Add(&r)
		}
	}
	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}

	byPrefix := map[string]command{}
	for _, p := range tl.Result() {
		byPrefix[p.Prefix] = command{p.Objects, p.Size, p.SizeMissing, p.Allocated, p.AllocatedMissing}
	}
	out := make([]command, len(want))
	for i, w := range want {
		out[i] = byPrefix[w.Prefix]
	}
	return out
}
