// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package tree is the core shared by tree sources: stores listed one
// directory at a time (a filesystem, an Azure hierarchical namespace),
// which have no seekable key order across the whole namespace.
//
// A directory is the unit of work (Range). Its entries are keyed
// prefix+name, and a directory prefix+name+"/", where prefix is the
// directory's own key and names hold no '/'. Sorting a directory's entries
// by key and emitting a directory's row, then its subtree, before the next
// sibling is global unsigned-byte key order: every key of the subtree starts
// with the directory's key, and no sibling sorts between that key and the
// keys that extend it (a-b < a.b < a/ < a/x < a0).
//
// The Walker is the source.Provider; a Reader reads one range and hands its
// content to Out in key order: rows, after each of which a directory's
// subtree may be spliced in (Walk), and segments of the same directory
// (chunks a reader cuts, intervals the range engine splits). The ordered
// mode for stores whose directories are listed in name order and seekable
// is Ordered. The design is docs/internals/tree-sources.md.
package tree

import (
	"context"
	"slices"
	"strings"
	"sync"

	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source"
)

// Range is the tree's unit of work: a directory, or a segment of one.
type Range[D, S any] struct {
	Dir    D      // the directory, as its reader names it (a path, a folder ID)
	Prefix string // the directory's key: "" at the root, "a/b/" below it
	Seg    S      // a segment of the directory, when InSeg
	InSeg  bool
}

// Walk is what the walker does with a listed row.
type Walk uint8

const (
	Leaf    Walk = iota // not a directory: the row only
	Descend             // a directory: its row, then its subtree
	Prune               // a directory listed but not walked (another device)
	Omit                // an entry neither listed nor, for a directory, walked (the run's own output)
)

// Reader is the provider half of a tree source: it reads one range and
// hands its content to out in key order. Read is called concurrently for
// different ranges; it returns an error only when the listing must stop,
// and counts per-entry problems itself.
type Reader[D, S, P any] interface {
	Read(ctx context.Context, r Range[D, S], out *Out[D, S, P]) error
}

// Disposer is a Reader whose segments hold resources (a shared directory
// descriptor): the walker passes on the order stage's Dispose of a segment
// that will never be read.
type Disposer[S any] interface {
	Dispose(s S)
}

// Options configure a Walker.
type Options[P any] struct {
	// NoDirs walks every directory but emits no directory row.
	NoDirs bool
	// Size is a leaf row's bytes, for Stats.Bytes; nil counts none.
	Size func(*P) int64
}

// Stats are what the walker emitted.
type Stats struct {
	Files    int64 // leaf rows
	Dirs     int64 // directory rows walked or pruned (emitted unless NoDirs)
	Bytes    int64 // Size over leaf rows
	Pruned   int64 // directories stopped by the reader (Walk Prune)
	Excluded int64 // directories stopped by the recursive-exclude predicate
	Omitted  int64 // entries neither listed nor walked (Walk Omit)
	Segments int64 // segments readers handed out
}

func (s *Stats) add(o *Stats) {
	s.Files += o.Files
	s.Dirs += o.Dirs
	s.Bytes += o.Bytes
	s.Pruned += o.Pruned
	s.Excluded += o.Excluded
	s.Omitted += o.Omitted
	s.Segments += o.Segments
}

// Walker lists a tree through a Reader. It implements
// source.Provider[Range[D, S], P] and source.Disposer.
type Walker[D, S, P any] struct {
	root  Range[D, S]
	r     Reader[D, S, P]
	opt   Options[P]
	prune func(string) bool // immutable during a walk

	mu    sync.Mutex
	stats Stats
}

var (
	_ source.Provider[Range[string, int], int] = (*Walker[string, int, int])(nil)
	_ source.Disposer[Range[string, int]]      = (*Walker[string, int, int])(nil)
)

// NewWalker walks the tree below root (a directory range) through r.
func NewWalker[D, S, P any](root Range[D, S], r Reader[D, S, P], opt Options[P]) *Walker[D, S, P] {
	return &Walker[D, S, P]{root: root, r: r, opt: opt}
}

// SetPruneDirectories installs a proof that every descendant of a directory is
// excluded. Call before listing starts. It applies only to Descend decisions:
// a reader's Omit or Prune takes precedence. The directory's own row still
// goes through the ordinary row filter, independently of its descendants.
func (w *Walker[D, S, P]) SetPruneDirectories(prune func(string) bool) { w.prune = prune }

// Root is the root directory.
func (w *Walker[D, S, P]) Root() Range[D, S] { return w.root }

// Expand reads one range.
func (w *Walker[D, S, P]) Expand(ctx context.Context, r Range[D, S], e source.Emitter[Range[D, S], P]) error {
	out := Out[D, S, P]{e: e, r: r, noDirs: w.opt.NoDirs, size: w.opt.Size, prune: w.prune}
	err := ctx.Err()
	if err == nil {
		err = w.r.Read(ctx, r, &out)
	} else if d, ok := w.r.(Disposer[S]); ok && r.InSeg {
		// A taken range is no longer queued, so the order stage will not
		// dispose it: a segment not read is released here.
		d.Dispose(r.Seg)
	}
	w.mu.Lock()
	w.stats.add(&out.st)
	w.mu.Unlock()
	return err
}

// Dispose releases a segment that will never be read (source.Disposer).
func (w *Walker[D, S, P]) Dispose(r Range[D, S]) {
	if d, ok := w.r.(Disposer[S]); ok && r.InSeg {
		d.Dispose(r.Seg)
	}
}

// Stats returns the counters so far.
func (w *Walker[D, S, P]) Stats() Stats {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stats
}

// Out is where a Reader puts one range's content, in key order. It is the
// order stage's emitter with the tree's splicing: a directory's subtree
// follows its row.
type Out[D, S, P any] struct {
	e      source.Emitter[Range[D, S], P]
	r      Range[D, S]
	noDirs bool
	size   func(*P) int64
	prune  func(string) bool
	st     Stats
}

// Rows appends rows in key order, ownership passing as with
// source.Emitter.Rows. walk says what each row is; for a Descend row it
// also names the directory, whose subtree is emitted right after the row
// (its key, cloned, is the child's prefix: keys may be views of a reader's
// arena). walk is called once per row, in order, before the row is handed
// on.
func (o *Out[D, S, P]) Rows(rows []schema.Row[P], walk func(*schema.Row[P]) (D, Walk)) error {
	start := 0
	for i := range rows {
		row := &rows[i]
		dir, w := walk(row)
		if w == Leaf {
			o.st.Files++
			if o.size != nil {
				o.st.Bytes += o.size(&row.Data)
			}
			continue
		}
		if w == Omit {
			o.st.Omitted++
			if err := o.e.Rows(rows[start:i]); err != nil {
				return err
			}
			start = i + 1
			continue
		}
		o.st.Dirs++
		end := i + 1
		if o.noDirs {
			end = i
		}
		excluded := w == Descend && o.prune != nil && o.prune(row.Key)
		if w == Prune || excluded {
			if excluded {
				o.st.Excluded++
			} else {
				o.st.Pruned++
			}
			if !o.noDirs {
				continue // listed with the rows after it; its subtree is not walked
			}
			if err := o.e.Rows(rows[start:i]); err != nil {
				return err
			}
			start = i + 1
			continue
		}
		// The child is taken from the row before the rows are handed over:
		// the emitter owns them then, and a filter may clear the row.
		child := Range[D, S]{Dir: dir, Prefix: strings.Clone(row.Key)}
		if err := o.e.Rows(rows[start:end]); err != nil {
			return err
		}
		start = i + 1
		if err := o.e.Child(child); err != nil {
			return err
		}
	}
	return o.e.Rows(rows[start:])
}

// Seg emits a segment of this range's directory at this position. Its
// ownership passes to the order stage even when Seg returns an error (it
// disposes it through the walker).
func (o *Out[D, S, P]) Seg(s S) error {
	o.st.Segments++
	return o.e.Child(Range[D, S]{Dir: o.r.Dir, Prefix: o.r.Prefix, Seg: s, InSeg: true})
}

// Key is an entry's key in the directory whose key is prefix.
func Key(prefix, name string, dir bool) string {
	if dir {
		return prefix + name + "/"
	}
	return prefix + name
}

// SortRows sorts one directory's rows by key, unsigned byte order. Keys in
// a directory are distinct.
func SortRows[P any](rows []schema.Row[P]) {
	slices.SortFunc(rows, func(a, b schema.Row[P]) int { return strings.Compare(a.Key, b.Key) })
}
