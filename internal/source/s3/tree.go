// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"fmt"

	"github.com/varveio/swath/internal/bind"
	"github.com/varveio/swath/internal/counters"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/objhttp"
	"github.com/varveio/swath/internal/source/s3/client"
	"github.com/varveio/swath/internal/source/tree"
)

// Tree lists a bucket folder by folder (--listing tree): the tree core's
// delimited reader (tree.Delimited) over ListObjectsV2 with delimiter '/',
// each folder from its first page by continuation tokens only, never a
// start-after, read whole, checked and sorted by raw bytes, each common
// prefix a child folder listed by its prefix exactly as returned. It
// relies on no page or seek order, so it serves a store whose listing is
// not in byte order (Cloudflare R2). Rows are the range engine's: schema
// swath/s3-objects/v1, the same mapping (provider.rows) and spill codec.
// No resume: a dataset run keeps no journal.
type Tree struct {
	*tree.Walker[struct{}, struct{}, Object]
	objhttp.Activity
	p      *Provider // connected: the client, options, credentials and region
	d      *tree.Delimited[Object]
	set    *counters.Set
	own    bool
	reason string
}

// The tree listing's counters: the client's (clientCounters, from ID 0),
// then the delimited reader's (tree.DelimitedCounters).
var treeCounters = append(append([]counters.Def(nil), clientCounters...), tree.DelimitedCounters...)

var _ source.Provider[tree.FolderRange, Object] = (*Tree)(nil)

// newTree lists p's bucket folder by folder. The scope is p.opt.Prefix,
// whatever it ends in: the walk starts at its deepest folder and keeps
// there only the entries whose raw bytes start with it (tree.Delimited).
func newTree(p *Provider, own bool, reason string) (*Tree, error) {
	if err := treeRefusal(&p.opt, reason); err != nil {
		return nil, err
	}
	t := &Tree{p: p, Activity: p.Activity, own: own, reason: reason, set: counters.New(treeCounters)}
	t.d = tree.NewDelimited[Object](folderLister{p}, tree.DelimitedOptions[Object]{
		Select:       p.opt.Prefix,
		FolderBytes:  p.opt.TreeFolderBytes,
		StagingBytes: p.opt.TreeStagingBytes,
		// A page in flight reserves what its decoded strings can hold at
		// most: its entries' URL-encoded keys (3 bytes a key byte, up to
		// S3's 1,024) and metadata.
		PageHeadroom:  int64(pageKeys(p.opt.Policy.PageSize)) * pageEntryHeadroom,
		HeadroomBytes: p.opt.TreeHeadroomBytes,
		Bytes:         objectBytes,
		Requeue:       objhttp.Requeue,
		Counters:      t.set,
		Base:          counters.ID(len(clientCounters)),
	})
	t.Walker = tree.NewWalker[struct{}, struct{}, Object](t.d.Root(), t.d, tree.Options[Object]{Size: func(o *Object) int64 { return o.Size }})
	return t, nil
}

// pageEntryHeadroom is what one entry of a page in flight is reserved:
// a URL-encoded 1,024-byte key and its metadata, rounded up.
const pageEntryHeadroom = 4 << 10

// pageKeys is the entries a page asks for: the page size, or S3's 1,000.
func pageKeys(size int) int {
	if size > 0 {
		return min(size, maxListKeys)
	}
	return maxListKeys
}

// objectBytes is what a row's payload holds beyond the row: its strings.
func objectBytes(o *Object) int64 {
	return int64(len(o.ETag) + len(o.StorageClass) + len(o.OwnerID) + len(o.OwnerDisplayName) + len(o.ChecksumAlgorithm) + len(o.ChecksumType))
}

// folderLister is S3's delimited list call: ListObjectsV2 with delimiter
// '/' and the folder's prefix, continuing from the service's token, never
// from a start-after; objects mapped to rows as the range engine maps them.
type folderLister struct{ p *Provider }

func (l folderLister) ListFolder(ctx context.Context, prefix, token string) (*tree.FolderPage[Object], error) {
	opt := &l.p.opt
	out, err := l.p.client.ListObjectsV2(ctx, &client.ListInput{Bucket: opt.Bucket, Prefix: prefix, Delimiter: "/",
		ContinuationToken: token, MaxKeys: opt.Policy.PageSize, FetchOwner: opt.FetchOwner})
	if err != nil {
		return nil, err
	}
	return &tree.FolderPage[Object]{Objects: l.p.rows(out), Folders: out.CommonPrefixes, Truncated: out.IsTruncated,
		Token: out.NextContinuationToken, Retained: int64(out.Retained)}, nil
}

// Stats snapshots the counters: the client's and the folder walk's.
func (t *Tree) Stats() Stats {
	t.Activity.Store(t.set, 0)
	if c, ok := t.p.client.(refreshed); ok {
		n, forced := c.CredentialRefreshes()
		t.set.Set(counters.ID(cCredentialRefreshes), n)
		t.set.Set(counters.ID(cCredentialExpired), forced)
	}
	p := t.p
	return Stats{Anonymous: p.anonymous, Credentials: p.creds, Region: p.region, NoVerifySSL: p.opt.Client == nil && p.opt.Trust.NoVerify,
		Listing: "tree", ListingReason: t.reason, R2Endpoint: R2Endpoint(p.opt.endpoint()),
		Counters: t.set.Snapshot(p.opt.Admission.Off(t.own)...)}
}

// OpenedTree is a bucket opened for the folder walk, bound to the S3
// current-objects schema.
type OpenedTree struct {
	objhttp.Activity
	bound bind.Bound[Object]
	t     *Tree
	root  string
}

var (
	_ Inventory        = (*OpenedTree)(nil)
	_ source.Live      = (*OpenedTree)(nil)
	_ source.Faulted   = (*OpenedTree)(nil)
	_ source.Advancing = (*OpenedTree)(nil)
	_ source.Requested = (*OpenedTree)(nil)
	_ source.Priced    = (*OpenedTree)(nil)
)

// Bound is the provider bound to its row schema.
func (o *OpenedTree) Bound() bind.Bound[Object] { return o.bound }

// Options is what the listing opened with.
func (o *OpenedTree) Options() Options { return o.t.p.opt }

// Listed is the rows emitted so far.
func (o *OpenedTree) Listed() int64 { return o.t.Walker.Stats().Files }

// Staged is what the folders being read hold now (tests).
func (o *OpenedTree) Staged() int64 { return o.t.d.Staged() }

// Frontier is the walk's progress for the watchdog (source.Advancing):
// pages that held an entry or ended a folder; an empty continuing page
// only within a bounded run of them.
func (o *OpenedTree) Frontier() (advanced, emptyChained int64) { return o.t.d.Frontier() }

// Locator records the root as given.
func (o *OpenedTree) Locator() source.Locator {
	return source.Locator{Kind: "s3", Root: o.root, Container: o.t.p.opt.Bucket}
}

// Tariff is the range listing's (Opened.Tariff).
func (o *OpenedTree) Tariff() (float64, string) { return o.opened().Tariff() }

func (o *OpenedTree) opened() *Opened[Object] { return &Opened[Object]{p: o.t.p, root: o.root} }

// Identity is the range listing's fields and listing tree: a journal of one
// listing never resumes as the other (a tree run keeps none yet).
func (o *OpenedTree) Identity() source.Identity {
	id := o.opened().Identity()
	id.Fields = append(id.Fields, source.IdentityField{Name: "listing", Value: "tree"})
	return id
}

// Capabilities: a folder is read whole by continuation tokens and sorted
// here; no resume yet (docs/internals/tree-sources.md).
func (o *OpenedTree) Capabilities() source.Capabilities {
	return source.Capabilities{Seek: source.SeekTokenOnly, Order: source.OrderNone}
}

// Report is the walk's statistics: its objects, bytes, folders and
// pages, the client's requests and retries. Its coverage is complete only
// when every folder the walk handed out was read whole: the start folder
// and each child spliced in (tree.Stats.Folders). A run that stopped
// (an error, a cancellation) leaves some unread, and says so.
func (o *OpenedTree) Report() source.Report {
	st := o.t.Stats()
	w := o.t.Walker.Stats()
	v := st.Counters.Value
	complete := v("tree.folders_completed") == 1+w.Folders
	stop := source.StopEndOfScope
	if !complete {
		stop = ""
	}
	r := source.Report{Name: "s3", Stats: &st,
		Brief: fmt.Sprintf("listing=tree objects=%d bytes=%d folders=%d pages=%d requests=%d retries=%d max_folder_entries=%d peak_staged_bytes=%d",
			w.Files, w.Bytes, v("tree.folders_completed"), v("tree.pages"), v("attempts"), v("retries"), v("tree.max_folder_entries"), v("tree.peak_staged_bytes")),
		Coverage: source.Coverage{Target: o.opened().target(), Prefix: o.t.p.opt.Prefix, Complete: complete, StopReason: stop},
	}
	if o.t.p.opt.FetchOwner {
		r.Collected = []string{"owner_id", "owner_display_name"}
	}
	return r
}

// openTree opens p's bucket for the folder walk.
func openTree(p *Provider, own bool, reason, root string) (*OpenedTree, error) {
	t, err := newTree(p, own, reason)
	if err != nil {
		return nil, err
	}
	b, err := bind.Provider[tree.FolderRange, Object](t, &Schema)
	if err != nil {
		return nil, err
	}
	return &OpenedTree{Activity: t.Activity, t: t, bound: b, root: root}, nil
}
