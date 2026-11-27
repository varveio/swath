// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/varveio/swath/internal/bind"
	"github.com/varveio/swath/internal/counters"
	"github.com/varveio/swath/internal/filter"
	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/azblob/client"
	"github.com/varveio/swath/internal/source/azblob/order"
	"github.com/varveio/swath/internal/source/keyrange"
	"github.com/varveio/swath/internal/source/objhttp"
	"github.com/varveio/swath/internal/source/tree"
)

// Tree lists an account with a hierarchical namespace (HNS) on the tree
// core's ordered mode (tree.Ordered): List Blobs with delimiter '/' lists
// one directory by name, its subdirectories as entries with their own
// properties (client.ListInput.Tree), and startFrom seeks by name within
// it. The byte-order adapter serves those name pages in byte order, as it
// serves a flat account's keys. A directory is a row keyed with a trailing
// '/' (its blob name plus '/'), with the properties the listing gives it;
// its subtree follows it. A directory larger than one page is split by the
// range engine like a flat container. The fetch flags fill a file's row as
// on a flat account; a directory entry lists no metadata, tags or
// immutability, so those columns stay null on its row (dirLister.object).
//
// The listing mode is part of the resume identity (listing tree); resume
// itself is not implemented (docs/internals/tree-sources.md, "Azure ordered directories").
type Tree struct {
	*tree.Walker[struct{}, keyrange.Range, Object]
	objhttp.Activity
	p              *Provider // the connected, admitted provider: client, options, route
	client         treeClient
	ord            *tree.Ordered[Object]
	pruning        atomic.Pointer[filter.Pruning] // installed before workers start; reports may run concurrently
	requests       atomic.Int64                   // listing requests sent (treeClient): list_requests
	counterBase    int
	includePrefix  string
	includePruned  atomic.Int64
	lookupEmpty    atomic.Int64
	lookupStalled  atomic.Int64
	lookupRequeues atomic.Int64
}

// treeCounters are the tree listing's own, registered after the flat
// provider's (the client's and the adapter's).
var treeCounters = []counters.Def{
	{Key: "list_requests", Kind: counters.Count, Unit: "requests", Help: "listing requests of the tree listing: directories' first pages, segment and resolve pages, lookups (a call the client retried is one; requests counts segment pages only)"},
	{Key: "tree.include_prefix_pruned", Kind: counters.Count, Unit: "directories", Help: "subtrees disjoint from the include prefix"},
	{Key: "tree.lookup_empty_pages", Kind: counters.Count, Unit: "pages", Help: "empty continuing pages followed by directory lookups"},
	{Key: "tree.lookup_requeues", Kind: counters.Count, Unit: "requests", Help: "directory lookup pages retried after the HTTP client's retries were exhausted"},
}

const (
	cListRequests = iota
	cIncludePruned
	cLookupEmpty
	cLookupRequeues
)

var _ source.Provider[tree.OrderedRange, Object] = (*Tree)(nil)

// newTree lists p's account on the tree core. The scope must be the
// container or a directory: a prefix that ends inside a name has no tree
// reading (the flat listing's prefix matches across directories).
func newTree(p *Provider, own bool) (*Tree, error) {
	opt := p.opt
	switch {
	case opt.Prefix != "" && !strings.HasSuffix(opt.Prefix, "/"):
		return nil, source.Unsupported(fmt.Errorf("Azure Blob: account %s has a hierarchical namespace, listed one directory at a time: the prefix %q must name a directory (end it with '/')", opt.Account, opt.Prefix))
	case opt.MaxKeys != 0:
		return nil, source.Unsupported(fmt.Errorf("Azure Blob: account %s has a hierarchical namespace: --max-keys requires a sequential flat listing", opt.Account))
	case opt.Sequential:
		return nil, source.Unsupported(fmt.Errorf("Azure Blob: account %s has a hierarchical namespace: --sequential lists a flat namespace only", opt.Account))
	}
	eo := p.engineOptions(own)
	// The seed and the structure probes read a flat scope's folders; a
	// segment is one directory's names.
	eo.Seed, eo.SeedFirstPage, eo.Structure = false, false, keyrange.StructureOff
	d := p.description()
	t := &Tree{p: p, Activity: p.Activity, counterBase: len(d.Counters)}
	d.Counters = append(d.Counters, treeCounters...)
	t.client = treeClient{c: p.client, n: &t.requests}
	ord, err := tree.NewOrdered[Object](dirLister{c: t.client, p: p, t: t}, eo, d)
	if err != nil {
		return nil, err
	}
	t.ord = ord
	t.Walker = tree.NewWalker[struct{}, keyrange.Range, Object](tree.OrderedRange{Prefix: opt.Prefix}, ord,
		tree.Options[Object]{Size: func(o *Object) int64 { return o.Size }})
	return t, nil
}

// Expand lists one range with its own byte-order adapter, as the flat
// provider's Expand does, released however Expand returns. Every page it
// reads holds rows (the seed and structure probes are off), so each sends
// the run's include, as a flat listing's pages do.
func (t *Tree) Expand(ctx context.Context, r tree.OrderedRange, e source.Emitter[tree.OrderedRange, Object]) error {
	eng := t.ord.Engine()
	a := order.New(t.client, t.p.opt.Container, eng.Set(), eng.ID(len(client.CounterDefs)))
	a.Include = t.p.include()
	defer a.Release()
	return t.Walker.Expand(context.WithValue(ctx, adapterKey{}, a), r, e)
}

// Stats snapshots the counters: the client's, the engine's (segments), the
// adapter's and the tree core's (tree.*).
func (t *Tree) Stats() Stats {
	eng := t.ord.Engine()
	eng.Set().Set(eng.ID(t.counterBase+cListRequests), t.requests.Load())
	eng.Set().Set(eng.ID(t.counterBase+cIncludePruned), t.includePruned.Load())
	eng.Set().Set(eng.ID(t.counterBase+cLookupEmpty), t.lookupEmpty.Load()+t.lookupStalled.Load())
	eng.Set().Set(eng.ID(t.counterBase+cLookupRequeues), t.lookupRequeues.Load())
	st := t.p.stats(eng)
	st.Listing, st.Pruning, st.PrunedDirs = "tree", t.pruning.Load(), max(0, t.Walker.Stats().Excluded-t.includePruned.Load())
	return st
}

// treeClient sends every listing as a hierarchical namespace's directory
// listing (client.ListInput.Tree), for the adapter and Lookup, and counts
// them (n).
type treeClient struct {
	c API
	n *atomic.Int64
}

func (c treeClient) List(ctx context.Context, in *client.ListInput) (*client.ListOutput, error) {
	c.n.Add(1)
	q := *in
	q.Tree = true
	out, err := c.c.List(ctx, &q)
	if err != nil {
		return nil, err
	}
	for _, it := range out.Items {
		if it.Dir && len(it.Name)+1 > MaxKeyBytes {
			return nil, fmt.Errorf("%w: Azure Blob directory key has %d bytes including its trailing slash (limit %d)",
				schema.ErrKeyTooLong, len(it.Name)+1, MaxKeyBytes)
		}
	}
	return out, nil
}

// dirLister is the ordered mode's Lister: one directory's names through
// the Expand's byte-order adapter, and Lookup as a listing with at most one entry.
type dirLister struct {
	c treeClient
	p *Provider
	t *Tree
}

var _ tree.Lister[Object] = dirLister{}

func (l dirLister) List(ctx context.Context, q *tree.DirQuery) (*tree.DirPage[Object], error) {
	a, ok := ctx.Value(adapterKey{}).(*order.Adapter)
	if !ok || a == nil {
		return nil, source.Violation(errors.New("Azure Blob: directory listing without an Expand adapter"))
	}
	after := ""
	if q.After != "" {
		after = q.Dir + q.After
	}
	out, err := a.Fetch(ctx, &keyrange.Query{Prefix: q.Dir, Delimiter: "/", After: after, Token: q.Token, Max: q.Max})
	if err != nil {
		return nil, l.p.tagsRefused(err)
	}
	pg := &tree.DirPage[Object]{Entries: make([]tree.Entry[Object], 0, len(out.Items)), Truncated: out.Truncated, Token: out.Token}
	for i := range out.Items {
		it := &out.Items[i]
		name, ok := strings.CutPrefix(it.Name, q.Dir)
		if !ok || it.Prefix {
			return nil, source.Violation(fmt.Errorf("%w: %q in a listing of directory %q", objhttp.ErrInvalidPage, it.Name, q.Dir))
		}
		pg.Entries = append(pg.Entries, tree.Entry[Object]{Name: name, Dir: it.Dir, Data: l.object(it)})
	}
	return pg, nil
}

// Lookup asks for the first entry under dir+name by name: the directory
// named name, if there is one, sorts before every other name it begins.
// Its row is a directory's, which no include adds to, so it sends none.
func (l dirLister) Lookup(ctx context.Context, dir, name string) (tree.Entry[Object], bool, error) {
	in := &client.ListInput{Container: l.p.opt.Container, Prefix: dir + name, Delimiter: "/", MaxResults: 1}
	var seen *keyrange.TokenWindow
	var empty int
	var wait time.Duration
	for {
		out, err := l.c.List(ctx, in)
		if objhttp.Requeue(err) {
			l.t.lookupRequeues.Add(1)
			if err := keyrange.RequeueBackoff(ctx, &wait); err != nil {
				return tree.Entry[Object]{}, false, err
			}
			continue
		}
		if err != nil {
			return tree.Entry[Object]{}, false, err
		}
		if len(out.Items) > 0 {
			it := &out.Items[0]
			if it.Name == in.Prefix && it.Dir {
				return tree.Entry[Object]{Name: name, Dir: true, Data: l.object(it)}, true, nil
			}
			return tree.Entry[Object]{}, false, nil
		}
		if out.NextMarker == "" {
			return tree.Entry[Object]{}, false, nil
		}
		if seen == nil {
			seen = keyrange.NewTokenWindow(in.Marker)
		}
		if !seen.Add(out.NextMarker) {
			return tree.Entry[Object]{}, false, source.Violation(fmt.Errorf("%w: directory lookup repeats an empty continuation marker", objhttp.ErrInvalidPage))
		}
		empty++
		if empty <= keyrange.EmptyChainLive {
			l.t.lookupEmpty.Add(1)
		} else {
			l.t.lookupStalled.Add(1)
		}
		in.Marker, wait = out.NextMarker, 0
	}
}

// object is an entry's row payload: a file's as a flat listing makes it
// (Provider.object, the asked-for columns filled); a directory's the
// properties its entry (a BlobPrefix) carries, which hold no metadata,
// tags or immutability whatever the include, so those columns stay null:
// not read, as for a blob whose metadata a listing cannot read, rather
// than "{}", which would claim it has none (a directory's metadata holds
// hdi_isfolder at least).
func (l dirLister) object(it *client.Item) Object {
	if it.Dir {
		return itemObject(it)
	}
	return l.p.object(it)
}

// OpenedTree is an opened hierarchical namespace bound to the Azure
// current-objects schema.
type OpenedTree struct {
	objhttp.Activity
	bound bind.Bound[Object]
	t     *Tree
}

var (
	_ Inventory        = (*OpenedTree)(nil)
	_ source.Live      = (*OpenedTree)(nil)
	_ source.Faulted   = (*OpenedTree)(nil)
	_ source.Advancing = (*OpenedTree)(nil)
	_ source.Requested = (*OpenedTree)(nil)
)

func openTree(p *Provider, own bool, includePrefix string) (*OpenedTree, error) {
	t, err := newTree(p, own)
	if err != nil {
		return nil, err
	}
	t.includePrefix = includePrefix
	b, err := bind.Provider[tree.OrderedRange, Object](t, &Schema)
	if err != nil {
		return nil, err
	}
	return &OpenedTree{Activity: t.Activity, t: t, bound: b}, nil
}

// Bound is the provider bound to its row schema.
func (o *OpenedTree) Bound() bind.Bound[Object] { return o.bound }

// Options returns the options the listing opened with.
func (o *OpenedTree) Options() Options { return o.t.p.opt }

// PruneDirectories installs recursive excludes before any worker can
// submit children. Row filtering remains in Bound.
func (o *OpenedTree) PruneDirectories(spec *filter.Spec) error {
	prune, plan, err := spec.PruneDirectories()
	if err != nil {
		return err
	}
	if prefix := o.t.includePrefix; prefix != "" {
		exclude := prune
		prune = func(dir string) bool {
			if !strings.HasPrefix(dir, prefix) && !strings.HasPrefix(prefix, dir) {
				o.t.includePruned.Add(1)
				return true
			}
			return exclude != nil && exclude(dir)
		}
	}
	o.t.SetPruneDirectories(prune)
	o.t.pruning.Store(&plan)
	return nil
}

// Listed is the rows emitted so far: files and directories.
func (o *OpenedTree) Listed() int64 {
	st := o.t.Walker.Stats()
	return st.Files + st.Dirs
}

// Frontier is the walk's key-space progress (source.Advancing), which the
// watchdog takes in place of Answered: an empty continuing page answers
// but does not advance.
func (o *OpenedTree) Frontier() (advanced, emptyChained int64) {
	advanced, emptyChained = o.t.ord.Frontier()
	return advanced + o.t.lookupEmpty.Load(), emptyChained + o.t.lookupStalled.Load()
}

// Locator records the root without its SAS.
func (o *OpenedTree) Locator() source.Locator {
	return (&Opened[Object]{p: o.t.p}).Locator()
}

// Identity is the flat identity's fields and listing tree: a journal of
// one mode never resumes as the other.
func (o *OpenedTree) Identity() source.Identity {
	id := (&Opened[Object]{p: o.t.p}).Identity()
	id.Fields = append(id.Fields, source.IdentityField{Name: "listing", Value: "tree"})
	return id
}

// Capabilities: a directory is listed by name, not the namespace by key;
// no resume yet.
func (o *OpenedTree) Capabilities() source.Capabilities {
	return source.Capabilities{Seek: source.SeekNone, Order: source.OrderOther}
}

// Report is the flat report's shape with a brief of the whole walk: the
// walker's files, directories and bytes, every listing request
// (list_requests) and the directories listed (tree.first_pages), the
// segments' splits and the client's retries. The engine's objects, bytes,
// pages and requests count segment pages only; they stay in the counters.
func (o *OpenedTree) Report() source.Report {
	st := o.t.Stats()
	w := o.t.Walker.Stats()
	v := st.Counters.Value
	r := o.t.p.report(&st)
	r.Brief = fmt.Sprintf("files=%d dirs=%d bytes=%d requests=%d first_pages=%d splits=%d retries=%d truncated=%v",
		w.Files, w.Dirs, w.Bytes, v("list_requests"), v("tree.first_pages"), v("split.accepted_bounded")+v("split.accepted_frontier"),
		v("retries"), !r.Coverage.Complete)
	return r
}
