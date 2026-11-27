// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package vercel lists current Vercel Blob objects over its folded list API.
package vercel

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/varveio/swath/internal/bind"
	"github.com/varveio/swath/internal/counters"
	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/objhttp"
	"github.com/varveio/swath/internal/source/tree"
	"github.com/varveio/swath/internal/source/vercel/client"
)

// TokenEnv names the environment variable for the store read-write token.
const TokenEnv = "BLOB_READ_WRITE_TOKEN"

const rootScheme = "vercel://"

// ParseURL splits vercel://store_ID[/prefix]. The store ID is case-sensitive
// and the part after its slash is a literal key prefix.
func ParseURL(root string) (store, prefix string, err error) {
	rest, ok := strings.CutPrefix(root, rootScheme)
	store, prefix, _ = strings.Cut(rest, "/")
	id, qualified := strings.CutPrefix(store, "store_")
	if !ok || !qualified || !validStoreID(id) {
		return "", "", errors.New("want vercel://store_ID[/prefix] with a valid store ID")
	}
	if !utf8.ValidString(prefix) {
		return "", "", errors.New("Vercel Blob key prefix must be valid UTF-8")
	}
	return store, prefix, nil
}

func validStoreID(id string) bool {
	return id != "" && !strings.ContainsFunc(id, func(r rune) bool {
		return !(r >= '0' && r <= '9' || r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z')
	})
}

// KeyPrefix is the raw key prefix named in root.
func KeyPrefix(root string) (string, error) {
	_, prefix, err := ParseURL(root)
	return prefix, err
}

// WithPrefix names a different literal prefix of the same store.
func WithPrefix(root, prefix string) string {
	store, _, err := ParseURL(root)
	if err != nil || !utf8.ValidString(prefix) {
		return root
	}
	return rootScheme + store + "/" + prefix
}

// Options configures the folded listing and its HTTP client. A zero page
// size uses the service's maximum. HTTPClient supplies the transport for tests.
type Options struct {
	Token      string
	Workers    int
	PageSize   int
	Timeout    time.Duration
	Trust      source.Trust
	Admission  objhttp.Admission
	HTTPClient *http.Client
	// FolderBytes and StagingBytes override the shared folder limits.
	FolderBytes  int64
	StagingBytes int64
}

// Opened is a folded listing bound to its object schema.
type Opened struct {
	objhttp.Activity
	bound       bind.Bound[Object]
	walk        *tree.Walker[struct{}, struct{}, Object]
	folders     *tree.Delimited[Object]
	counts      *counters.Set
	off         []string
	root        string
	store       string
	prefix      string
	noVerifySSL bool
}

var (
	_ source.Identified = (*Opened)(nil)
	_ source.Capable    = (*Opened)(nil)
	_ source.Reporter   = (*Opened)(nil)
	_ source.Live       = (*Opened)(nil)
	_ source.Faulted    = (*Opened)(nil)
	_ source.Advancing  = (*Opened)(nil)
	_ source.Requested  = (*Opened)(nil)
)

var reportCounters = append(objhttp.CounterDefs("429 or rate_limited"), tree.DelimitedCounters...)

// Admission reservation per response, before retained strings transfer to
// folder staging. This limits concurrent page work, not exact heap or RSS:
// decoder buffers, temporary strings and garbage collection can add memory.
const pageHeadroom = 4 * client.MaxBodyBytes

// Open validates the root and binds its store's current objects to Schema.
// The token selects the store on the wire, so its store ID must match root.
func Open(_ context.Context, root string, opt Options) (*Opened, error) {
	store, prefix, err := ParseURL(root)
	if err != nil {
		return nil, err
	}
	token := strings.TrimSpace(opt.Token)
	if err := matchToken(token, store); err != nil {
		return nil, err
	}
	if opt.PageSize < 0 || opt.PageSize > client.MaxPageSize {
		return nil, source.Unsupported(fmt.Errorf("Vercel Blob page size must be between 0 and %d", client.MaxPageSize))
	}
	if opt.PageSize == 0 {
		opt.PageSize = client.MaxPageSize
	}
	workers := max(opt.Workers, 1)
	lim, rate := opt.Admission.New(workers)
	c, err := client.New(client.Config{Token: token, Workers: workers, Trust: opt.Trust,
		HTTPClient: opt.HTTPClient, RequestTimeout: opt.Timeout, Retry: objhttp.Retry{Limiter: lim, Rate: rate}})
	if err != nil {
		return nil, err
	}
	set := counters.New(reportCounters)
	folders := tree.NewDelimited(folderLister{c, opt.PageSize}, tree.DelimitedOptions[Object]{
		Select: prefix, Bytes: objectHeap, FolderBytes: opt.FolderBytes, StagingBytes: opt.StagingBytes,
		PageHeadroom: pageHeadroom,
		// HTTP retries cover cut or malformed JSON; do not replay a persistently
		// bad body again at the folder level after those retries are exhausted.
		Requeue: func(err error) bool {
			return !errors.Is(err, objhttp.ErrSyntax) && objhttp.Requeue(err)
		},
		Counters: set, Base: counters.ID(objhttp.NumCounters),
	})
	walk := tree.NewWalker(folders.Root(), folders, tree.Options[Object]{Size: func(o *Object) int64 { return o.Size }})
	bound, err := bind.Provider[tree.FolderRange, Object](walk, &Schema)
	if err != nil {
		return nil, err
	}
	return &Opened{Activity: objhttp.Observe(c), bound: bound, walk: walk, folders: folders, counts: set,
		off: opt.Admission.Off(true), root: root, store: store, prefix: prefix, noVerifySSL: opt.HTTPClient == nil && opt.Trust.NoVerify}, nil
}

func matchToken(token, store string) error {
	var err error
	switch {
	case token == "":
		err = fmt.Errorf("set %s for this Vercel Blob store", TokenEnv)
	default:
		rest, ok := strings.CutPrefix(token, "vercel_blob_rw_")
		id, secret, split := strings.Cut(rest, "_")
		if !ok || !split || !validStoreID(id) || secret == "" || strings.ContainsFunc(secret, func(r rune) bool { return r <= ' ' || r >= '\x7f' }) {
			err = errors.New("expected a Vercel Blob read-write token")
		} else if "store_"+id != store {
			err = errors.New("the token belongs to a different Vercel Blob store")
		}
	}
	if err != nil {
		return &objhttp.CredentialError{Source: TokenEnv, Err: err}
	}
	return nil
}

type folderLister struct {
	client *client.Client
	limit  int
}

func (l folderLister) ListFolder(ctx context.Context, prefix, token string) (*tree.FolderPage[Object], error) {
	out, err := l.client.List(ctx, client.ListInput{Prefix: prefix, Cursor: token, Limit: l.limit})
	if err != nil {
		return nil, err
	}
	rows := make([]schema.Row[Object], len(out.Blobs))
	for i, b := range out.Blobs {
		rows[i] = schema.Row[Object]{Key: b.Pathname, Data: Object{Size: b.Size, UploadedAt: b.UploadedAt.UnixMicro(), ETag: b.ETag}}
	}
	return &tree.FolderPage[Object]{Objects: rows, Folders: out.Folders,
		Truncated: out.NextCursor != "", Token: out.NextCursor}, nil
}

// Bound is the provider bound to its row schema.
func (o *Opened) Bound() bind.Bound[Object] { return o.bound }

// Listed is the number of real blobs handed to the listing so far.
func (o *Opened) Listed() int64 { return o.walk.Stats().Files }

// Frontier reports progress from validated folder pages.
func (o *Opened) Frontier() (advanced, emptyChained int64) { return o.folders.Frontier() }

// Locator records the requested store and root.
func (o *Opened) Locator() source.Locator {
	return source.Locator{Kind: "vercel", Root: o.root, Container: o.store}
}

// Identity names the scope and route independently of credentials.
func (o *Opened) Identity() source.Identity {
	return source.Identity{Provider: "vercel", Fields: []source.IdentityField{
		{Name: "source", Value: rootScheme + o.store + "/" + o.prefix},
		{Name: "endpoint", Value: client.Endpoint},
		{Name: "listing", Value: "tree"},
	}}
}

// Capabilities describes the opaque cursor listing. Each folder is sorted
// before it is emitted; the walk does not depend on service order.
func (o *Opened) Capabilities() source.Capabilities {
	return source.Capabilities{Seek: source.SeekTokenOnly, Order: source.OrderNone}
}

// Stats describes the chosen listing and its client and folder counters.
type Stats struct {
	Credentials   string            `json:"credentials"`
	NoVerifySSL   bool              `json:"no_verify_ssl,omitempty"`
	Listing       string            `json:"listing"`
	ListingReason string            `json:"listing_reason"`
	Counters      counters.Snapshot `json:"counters"`
}

// Report records the scope, work performed and whether every scheduled
// folder, including the root, completed.
func (o *Opened) Report() source.Report {
	o.Activity.Store(o.counts, 0)
	st := Stats{Credentials: "read-write-token", NoVerifySSL: o.noVerifySSL,
		Listing: "tree", ListingReason: "folded_cursor", Counters: o.counts.Snapshot(o.off...)}
	w := o.walk.Stats()
	v := st.Counters.Value
	complete := v("tree.folders_completed") == 1+w.Folders
	var stop string
	if complete {
		stop = source.StopEndOfScope
	}
	return source.Report{Name: "vercel", Stats: &st,
		Brief: fmt.Sprintf("listing=tree objects=%d bytes=%d folders=%d pages=%d requests=%d retries=%d max_folder_entries=%d peak_staged_bytes=%d",
			w.Files, w.Bytes, v("tree.folders_completed"), v("tree.pages"), v("attempts"), v("retries"), v("tree.max_folder_entries"), v("tree.peak_staged_bytes")),
		Coverage: source.Coverage{Target: o.root, Prefix: o.prefix, Complete: complete, StopReason: stop},
	}
}
