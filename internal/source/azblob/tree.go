// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/varveio/swath/internal/counters"
	"github.com/varveio/swath/internal/filter"
	orderstage "github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/parquetout"
	"github.com/varveio/swath/internal/registry"
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
// itself is not implemented (docs/internals/tree-sources.md, "Resume").
type Tree struct {
	*tree.Walker[struct{}, keyrange.Range, Object]
	objhttp.Activity
	p        *Provider // the connected, admitted provider: client, options, route
	client   treeClient
	ord      *tree.Ordered[Object]
	pruning  atomic.Pointer[filter.Pruning] // installed before workers start; reports may run concurrently
	requests atomic.Int64                   // listing requests sent (treeClient): list_requests
}

// treeCounters are the tree listing's own, registered after the flat
// provider's (the client's and the adapter's).
var treeCounters = []counters.Def{
	{Key: "list_requests", Kind: counters.Count, Unit: "requests", Help: "listing requests of the tree listing: directories' first pages, segment and resolve pages, lookups (a call the client retried is one; requests counts segment pages only)"},
}

// cListRequests is list_requests' index among the provider's counters.
var cListRequests = len(client.CounterDefs) + len(order.CounterDefs)

var _ source.Provider[tree.OrderedRange, Object] = (*Tree)(nil)

// newTree lists p's account on the tree core. The scope must be the
// container or a directory: a prefix that ends inside a name has no tree
// reading (the flat listing's prefix matches across directories).
func newTree(p *Provider, own bool) (*Tree, error) {
	opt := p.opt
	switch {
	case opt.Prefix != "" && !strings.HasSuffix(opt.Prefix, "/"):
		return nil, source.Unsupported(fmt.Errorf("Azure Blob: account %s has a hierarchical namespace, listed one directory at a time: the prefix %q must name a directory (end it with '/')", opt.Account, opt.Prefix))
	case opt.Sequential:
		return nil, source.Unsupported(fmt.Errorf("Azure Blob: account %s has a hierarchical namespace: --sequential lists a flat namespace only", opt.Account))
	}
	eo := p.engineOptions(own)
	// The seed and the structure probes read a flat scope's folders; a
	// segment is one directory's names.
	eo.Seed, eo.SeedFirstPage, eo.Structure = false, false, keyrange.StructureOff
	d := p.description()
	d.Counters = append(d.Counters, treeCounters...)
	t := &Tree{p: p, Activity: p.Activity}
	t.client = treeClient{c: p.client, n: &t.requests}
	ord, err := tree.NewOrdered[Object](dirLister{c: t.client, p: p}, eo, d)
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
	t.Activity.Store(eng.Set(), eng.ID(0))
	if c, ok := t.p.client.(pooled); ok {
		eng.Set().Set(eng.ID(client.CPoolSize), int64(c.Transports()))
	}
	if c, ok := t.p.client.(capped); ok {
		eng.Set().Set(eng.ID(client.CRetryAfterCapped), c.RetryAfterCapped())
	}
	if c, ok := t.p.client.(formatted); ok {
		f := c.Formats()
		eng.Set().Set(eng.ID(client.CArrowPages), f.ArrowPages)
		eng.Set().Set(eng.ID(client.CArrowItems), f.ArrowItems)
		eng.Set().Set(eng.ID(client.CXMLPages), f.XMLPages)
		eng.Set().Set(eng.ID(client.CXMLItems), f.XMLItems)
	}
	eng.Set().Set(eng.ID(cListRequests), t.requests.Load())
	e := eng.Stats()
	return Stats{Anonymous: t.p.anonymous, Credentials: t.p.creds, Namespace: t.p.namespace, Listing: "tree",
		Pruning: t.pruning.Load(), PrunedDirs: t.Walker.Stats().Excluded,
		NoVerifySSL: t.p.opt.Client == nil && t.p.opt.Trust.NoVerify, MeanActive: e.MeanActive, Counters: e.Counters}
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
	return c.c.List(ctx, &q)
}

// dirLister is the ordered mode's Lister: one directory's names through
// the Expand's byte-order adapter, and Lookup as one listing request.
type dirLister struct {
	c treeClient
	p *Provider
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
	out, err := l.c.List(ctx, &client.ListInput{Container: l.p.opt.Container, Prefix: dir + name, Delimiter: "/", MaxResults: 1})
	if err != nil {
		return tree.Entry[Object]{}, false, err
	}
	if len(out.Items) == 0 || out.Items[0].Name != dir+name || !out.Items[0].Dir {
		return tree.Entry[Object]{}, false, nil
	}
	return tree.Entry[Object]{Name: name, Dir: true, Data: l.object(&out.Items[0])}, true, nil
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

// treeOpened is the registered source of a hierarchical namespace.
type treeOpened struct {
	registry.Bound[Object]
	t *Tree
}

var (
	_ registry.Source   = (*treeOpened)(nil)
	_ source.Identified = (*treeOpened)(nil)
	_ source.Capable    = (*treeOpened)(nil)
	_ source.Reporter   = (*treeOpened)(nil)
	_ source.Live       = (*treeOpened)(nil)
	_ source.Faulted    = (*treeOpened)(nil)
	_ source.Advancing  = (*treeOpened)(nil)
	_ source.Requested  = (*treeOpened)(nil)
)

// Run installs recursive excludes before either whole-directory reads or
// range-engine segments can submit children. Row filtering remains in Bound.
func (o *treeOpened) Run(ctx context.Context, env registry.Env) (orderstage.Stats, parquetout.Encoder, error) {
	prune, plan, err := env.Filter.PruneDirectories()
	if err != nil {
		return orderstage.Stats{}, nil, err
	}
	o.t.SetPruneDirectories(prune)
	o.t.pruning.Store(&plan)
	return o.Bound.Run(ctx, env)
}

// Listed is the rows emitted so far: files and directories.
func (o *treeOpened) Listed() int64 {
	st := o.t.Walker.Stats()
	return st.Files + st.Dirs
}

func (o *treeOpened) Answered() int64 { return o.t.Answered() }

// Frontier is the walk's key-space progress (source.Advancing), which the
// watchdog takes in place of Answered: an empty continuing page answers
// but does not advance.
func (o *treeOpened) Frontier() (advanced, emptyChained int64) { return o.t.ord.Frontier() }

// Requests is the run's live request account (source.Requested).
func (o *treeOpened) Requests() source.Requests           { return o.t.Requests() }
func (o *treeOpened) Faults() (throttles, timeouts int64) { return o.t.Faults() }

// Locator records the root without its SAS.
func (o *treeOpened) Locator() source.Locator {
	return source.Locator{Kind: "azblob", Root: o.t.p.target(), Container: o.t.p.opt.Container}
}

// Identity is the flat identity's fields and listing tree: a journal of
// one mode never resumes as the other.
func (o *treeOpened) Identity() source.Identity {
	id := (&opened[Object]{p: o.t.p}).Identity()
	id.Fields = append(id.Fields, source.IdentityField{Name: "listing", Value: "tree"})
	return id
}

// Capabilities: a directory is listed by name, not the namespace by key;
// no resume yet.
func (o *treeOpened) Capabilities() source.Capabilities {
	return source.Capabilities{Seek: source.SeekNone, Order: source.OrderOther}
}

// Report is the flat report's shape with a brief of the whole walk: the
// walker's files, directories and bytes, every listing request
// (list_requests) and the directories listed (tree.first_pages), the
// segments' splits and the client's retries. The engine's objects, bytes,
// pages and requests count segment pages only; they stay in the counters.
func (o *treeOpened) Report() source.Report {
	st := o.t.Stats()
	w := o.t.Walker.Stats()
	v := st.Counters.Value
	r := keyrange.Report("azblob", o.t.p.target(), o.t.p.opt.Prefix, &st, st.Counters)
	r.Brief = fmt.Sprintf("files=%d dirs=%d bytes=%d requests=%d first_pages=%d splits=%d retries=%d truncated=%v",
		w.Files, w.Dirs, w.Bytes, v("list_requests"), v("tree.first_pages"), v("split.accepted_bounded")+v("split.accepted_frontier"),
		v("retries"), !r.Coverage.Complete)
	return r
}
