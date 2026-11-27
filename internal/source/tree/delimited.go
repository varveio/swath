// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package tree

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"
	"unsafe"

	"golang.org/x/sync/semaphore"
	"golang.org/x/text/unicode/norm"

	"github.com/varveio/swath/internal/counters"
	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/keyrange"
)

// FolderRange is a delimited listing's unit of work: one folder, named by
// its key prefix ("" at the bucket's root, else ending in '/'), read whole.
type FolderRange = Range[struct{}, struct{}]

// FolderPage is one answer of a delimited listing of a folder (delimiter
// '/'), as the service sent it: the objects directly in the folder, keyed
// with their payloads, and its child folders (common prefixes), each
// spelled as returned. Truncated says the folder continues from Token.
// Retained is what the page's strings hold in memory (a decoded page's
// arena, which keeps its encoded size), for the staging account; 0 counts
// their lengths (and DelimitedOptions.Bytes).
type FolderPage[P any] struct {
	Objects   []schema.Row[P]
	Folders   []string
	Truncated bool
	Token     string
	Retained  int64
}

// FolderLister is a provider's delimited list call: the entries directly
// below prefix, from the folder's first page (token "") or from the
// service's own continuation token, never from a start key. The provider
// maps each object to its row, as its key-range listing does; the reader
// owns order, validation and splicing.
type FolderLister[P any] interface {
	ListFolder(ctx context.Context, prefix, token string) (*FolderPage[P], error)
}

// Default staging and headroom bounds of a delimited listing
// (DelimitedOptions).
const (
	DefaultFolderBytes   int64 = 64 << 20
	DefaultStagingBytes  int64 = 256 << 20
	DefaultHeadroomBytes int64 = 256 << 20
)

// DelimitedOptions configure a Delimited reader.
type DelimitedOptions[P any] struct {
	// Select is the scope: the keys that start with it, raw bytes. The
	// walk starts at its deepest folder (Select up to its last '/') and
	// keeps from that folder only the objects and child folders whose
	// keys start with Select; below them every key does.
	Select string
	// FolderBytes bounds what one folder stages while it is read whole
	// (0: DefaultFolderBytes); StagingBytes bounds what every folder
	// being read stages at once (0: DefaultStagingBytes). A folder that
	// would pass either stops the listing (FolderLimitError).
	FolderBytes, StagingBytes int64
	// PageHeadroom is reserved for each page in flight, from its request
	// until it is decoded and charged to staging (the response body and
	// its decoding); every page in flight at once reserves at most
	// HeadroomBytes (0: DefaultHeadroomBytes), and a page waits for its
	// reservation. PageHeadroom 0 reserves nothing.
	PageHeadroom, HeadroomBytes int64
	// Bytes is a row's payload bytes held beyond its key and the row
	// itself (its strings), for the staging account of a page that does
	// not say what it retains (FolderPage.Retained); nil counts none.
	Bytes func(*P) int64
	// Requeue reports a failed page the reader asks again after a
	// backoff, as the range engine requeues one (nil: none).
	Requeue func(error) bool
	// Counters is the table that holds DelimitedCounters from Base.
	Counters *counters.Set
	Base     counters.ID
}

// DelimitedCounters are a delimited listing's counters, which a provider
// places in its table at DelimitedOptions.Base.
var DelimitedCounters = []counters.Def{
	{Key: "tree.folders_started", Kind: counters.Count, Unit: "folders", Help: "folders whose listing began (the walk's root and every child folder)"},
	{Key: "tree.folders_completed", Kind: counters.Count, Unit: "folders", Help: "folders read whole, checked, sorted and handed on"},
	{Key: "tree.pages", Kind: counters.Count, Unit: "pages", Help: "delimited listing pages answered (a page the client retried is one)"},
	{Key: "tree.empty_pages", Kind: counters.Count, Unit: "pages", Help: "continuing pages that held no entry"},
	{Key: "tree.requeues", Kind: counters.Count, Unit: "pages", Help: "pages asked again, after a backoff, once the HTTP client's retries were exhausted on a transient failure"},
	{Key: "tree.objects", Kind: counters.Count, Unit: "objects", Help: "objects listed, the scope's and those of the start folder outside it"},
	{Key: "tree.prefixes", Kind: counters.Count, Unit: "folders", Help: "child folders (common prefixes) listed"},
	{Key: "tree.markers", Kind: counters.Count, Unit: "objects", Help: "objects whose key is their folder's own prefix (folder markers), emitted as rows in that folder"},
	{Key: "tree.empty_segments", Kind: counters.Count, Unit: "folders", Help: "child folders whose last segment is empty (a//), walked as any other"},
	{Key: "tree.non_nfc_names", Kind: counters.Count, Unit: "entries", Help: "entries whose name below the folder is not in Unicode NFC: where a store that matches names in NFC can merge two spellings of one folder"},
	{Key: "tree.out_of_scope", Kind: counters.Count, Unit: "entries", Help: "entries of the start folder whose keys do not start with the source prefix: left out, not walked"},
	{Key: "tree.probes", Kind: counters.Count, Unit: "folders", Help: "folders listed, emitting nothing, to prove that none holds a key of the scope under another spelling: below an out-of-scope child of the start folder whose name or the scope's remainder is not ASCII (an NFC-matching store can merge spellings), and below each of their folders"},
	{Key: "tree.headroom_waits", Kind: counters.Count, Unit: "pages", Help: "pages that waited for their response headroom (every page in flight at once reserves at most tree.limit.headroom_bytes)"},
	{Key: "tree.headroom_wait_ns", Kind: counters.Count, Unit: "ns", Help: "time those pages waited, summed"},
	{Key: "tree.max_folder_entries", Kind: counters.Max, Unit: "entries", Help: "the most entries (objects and child folders) of one folder"},
	{Key: "tree.max_folder_bytes", Kind: counters.Max, Unit: "bytes", Help: "the most one folder staged (what its pages' strings retain, or their lengths; entries; tokens: an estimate)"},
	{Key: "tree.peak_staged_bytes", Kind: counters.Max, Unit: "bytes", Help: "the most every folder being read staged at once"},
	{Key: "tree.limit.folder_bytes", Kind: counters.Max, Unit: "bytes", Help: "the bound on one folder's staging: a folder past it stops the run (folder_limit)"},
	{Key: "tree.limit.staging_bytes", Kind: counters.Max, Unit: "bytes", Help: "the bound on every folder's staging at once: a folder that would pass it stops the run (folder_limit)"},
	{Key: "tree.limit.page_headroom_bytes", Kind: counters.Max, Unit: "bytes", Help: "what each page in flight reserves until it is decoded and charged (0: no reservation)"},
	{Key: "tree.limit.headroom_bytes", Kind: counters.Max, Unit: "bytes", Help: "the bound on what every page in flight reserves at once: a page past it waits"},
	{Key: "tree.budget_refusals", Kind: counters.Count, Unit: "folders", Help: "folders stopped at a staging bound"},
	{Key: "tree.sort_ns", Kind: counters.Count, Unit: "ns", Help: "time spent sorting folders' entries by raw bytes, summed"},
	{Key: "tree.violations.raw_prefix", Kind: counters.Count, Unit: "entries", Help: "an object or child folder whose raw bytes do not start with its folder's (a store that matched the prefix another way): the run stops, protocol"},
	{Key: "tree.violations.delimiter", Kind: counters.Count, Unit: "entries", Help: "an object with a '/' below its folder, or a child folder that is not one segment and a '/': the run stops, protocol"},
	{Key: "tree.violations.self_reference", Kind: counters.Count, Unit: "entries", Help: "a child folder equal to its own folder: the run stops, protocol"},
	{Key: "tree.violations.empty_key", Kind: counters.Count, Unit: "entries", Help: "an object with an empty key: the run stops, protocol"},
	{Key: "tree.violations.duplicate", Kind: counters.Count, Unit: "entries", Help: "an entry listed twice in one folder: the run stops, protocol"},
	{Key: "tree.violations.token", Kind: counters.Count, Unit: "pages", Help: "a truncated page without a continuation token, or with one the folder already returned: the run stops, protocol"},
}

const (
	cFoldersStarted = iota
	cFoldersCompleted
	cPages
	cEmptyPages
	cRequeues
	cObjects
	cPrefixes
	cMarkers
	cEmptySegments
	cNonNFC
	cOutOfScope
	cProbes
	cHeadroomWaits
	cHeadroomWaitNs
	cMaxFolderEntries
	cMaxFolderBytes
	cPeakStaged
	cLimitFolder
	cLimitStaging
	cLimitPageHeadroom
	cLimitHeadroom
	cBudgetRefusals
	cSortNs
	cViolationPrefix
	cViolationDelimiter
	cViolationSelf
	cViolationEmptyKey
	cViolationDuplicate
	cViolationToken
	numDelimitedCounters
)

// FolderLimitError stops a delimited listing whose folder would stage
// more than a bound allows (DelimitedOptions): the folder alone
// (FolderBytes), or every folder being read at once (Staging).
type FolderLimitError struct {
	Folder  string
	Limit   int64
	Staging bool
}

func (e *FolderLimitError) Error() string {
	if e.Staging {
		return fmt.Sprintf("tree listing: the folders being read at once would hold more than %d MiB of entries while reading folder %+q (fewer --workers read fewer folders at once)",
			e.Limit>>20, e.Folder)
	}
	return fmt.Sprintf("tree listing: folder %+q holds more than %d MiB of entries, the most one folder is read with: a folder is listed whole and sorted in memory",
		e.Folder, e.Limit>>20)
}

// Class names the failure for the run's outcome (its cause).
func (e *FolderLimitError) Class() string { return "folder_limit" }

// Delimited is the tree reader of a store listed one folder at a time by
// continuation tokens only, whose pages need not be in byte order (an
// object store's delimited listing): a range is a folder, read whole,
// checked, sorted by raw bytes and handed on, each child folder spliced in
// at its key (Out.Folder) and listed by its prefix exactly as the service
// returned it. It implements Reader for a Walker over FolderRange.
type Delimited[P any] struct {
	l   FolderLister[P]
	opt DelimitedOptions[P]

	staged                atomic.Int64        // what the folders being read hold now
	headroom              *semaphore.Weighted // what pages in flight reserve (nil: none)
	advanced, emptyChains atomic.Int64        // Frontier
	waiting               func()              // test hook: a page is about to wait for headroom
}

var _ Reader[struct{}, struct{}, int] = (*Delimited[int])(nil)

// NewDelimited reads folders through l.
func NewDelimited[P any](l FolderLister[P], opt DelimitedOptions[P]) *Delimited[P] {
	if opt.FolderBytes <= 0 {
		opt.FolderBytes = DefaultFolderBytes
	}
	if opt.StagingBytes <= 0 {
		opt.StagingBytes = DefaultStagingBytes
	}
	if opt.Counters == nil {
		opt.Counters = counters.New(DelimitedCounters)
		opt.Base = 0
	}
	if opt.HeadroomBytes <= 0 {
		opt.HeadroomBytes = DefaultHeadroomBytes
	}
	// One page's reservation never exceeds the bound, so it is granted
	// whenever no other page holds any.
	opt.PageHeadroom = min(max(opt.PageHeadroom, 0), opt.HeadroomBytes)
	d := &Delimited[P]{l: l, opt: opt}
	if opt.PageHeadroom > 0 {
		d.headroom = semaphore.NewWeighted(opt.HeadroomBytes)
	}
	d.set(cLimitFolder, opt.FolderBytes)
	d.set(cLimitStaging, opt.StagingBytes)
	d.set(cLimitPageHeadroom, opt.PageHeadroom)
	d.set(cLimitHeadroom, opt.HeadroomBytes)
	return d
}

// Root is the walk's first folder: the scope's deepest folder.
func (d *Delimited[P]) Root() FolderRange {
	return FolderRange{Prefix: d.opt.Select[:strings.LastIndexByte(d.opt.Select, '/')+1]}
}

// Staged is what the folders being read hold now (0 once every Read has
// returned).
func (d *Delimited[P]) Staged() int64 { return d.staged.Load() }

// Frontier is the walk's progress for the watchdog (source.Advancing):
// pages that held an entry or ended a folder, plus empty continuing pages
// within the first keyrange.EmptyChainLive of their run; emptyChained
// counts the empty pages past it.
func (d *Delimited[P]) Frontier() (advanced, emptyChained int64) {
	return d.advanced.Load(), d.emptyChains.Load()
}

func (d *Delimited[P]) inc(c int)          { d.opt.Counters.Inc(d.opt.Base + counters.ID(c)) }
func (d *Delimited[P]) add(c int, n int64) { d.opt.Counters.Add(d.opt.Base+counters.ID(c), n) }
func (d *Delimited[P]) max(c int, n int64) { d.opt.Counters.Max(d.opt.Base+counters.ID(c), n) }
func (d *Delimited[P]) set(c int, n int64) { d.opt.Counters.Set(d.opt.Base+counters.ID(c), n) }
func (d *Delimited[P]) violation(c int, f string, a ...any) error {
	d.inc(c)
	return source.Violation(fmt.Errorf("%w: "+f, append([]any{errInvalidFolder}, a...)...))
}

// errInvalidFolder heads every validation failure's message.
var errInvalidFolder = errors.New("delimited listing breaks its folder's structure")

// entry is one staged entry of a folder: an object's row, or a child
// folder keyed by its prefix (folder set, no payload).
type entry[P any] struct {
	row    schema.Row[P]
	folder bool
}

// Read lists the folder r.Prefix whole, checks it, sorts it and hands it
// on (list). In the start folder of a scope that does not end in '/', only
// the entries whose raw bytes start with the scope are kept (scope).
func (d *Delimited[P]) Read(ctx context.Context, r FolderRange, out *Out[struct{}, struct{}, P]) error {
	d.inc(cFoldersStarted)
	var held int64
	defer func() { d.staged.Add(-held) }()
	entries, err := d.list(ctx, r.Prefix, &held)
	if err != nil {
		return err
	}
	if len(d.opt.Select) > len(r.Prefix) {
		if entries, err = d.scope(ctx, r.Prefix, entries); err != nil {
			return err
		}
	}
	if err := emit(out, entries); err != nil {
		return err
	}
	d.inc(cFoldersCompleted)
	return nil
}

// list reads the folder prefix whole, its staging charged to *held, and
// returns its entries sorted by raw bytes. Validation stops the run
// (protocol) rather than drop a row:
//
//   - every object key and child folder starts with the folder's raw
//     prefix bytes (a store that matched the prefix in another form, such
//     as Unicode NFC, returns keys that do not);
//   - an object key is not empty and holds no '/' after the prefix: with
//     delimiter '/' every key that does is rolled into a child folder, and
//     the folder's own marker (key equal to the prefix) has an empty
//     remainder;
//   - a child folder is the prefix, one segment without '/' (possibly
//     empty) and one '/', and is not the folder itself;
//   - no entry is listed twice in the folder, across its pages;
//   - a truncated page has a continuation token the folder has not
//     returned before; a terminal page needs none.
//
// No start key is ever sent: each page continues from the service's own
// token. Keys in messages are quoted with %+q, so that two spellings of
// one name (NFC and NFD) read apart.
func (d *Delimited[P]) list(ctx context.Context, prefix string, held *int64) ([]entry[P], error) {
	var entries []entry[P]
	seen := map[string]struct{}{}
	token, empty := "", 0
	var wait time.Duration
	for {
		release, err := d.reserve(ctx)
		if err != nil {
			return nil, err
		}
		pg, err := d.l.ListFolder(ctx, prefix, token)
		if d.opt.Requeue != nil && err != nil && ctx.Err() == nil && d.opt.Requeue(err) {
			release()
			d.inc(cRequeues)
			if err := keyrange.RequeueBackoff(ctx, &wait); err != nil {
				return nil, err
			}
			continue
		}
		if err == nil {
			err = d.page(prefix, pg, held, len(seen))
		}
		release() // decoded and charged (or failed): its headroom is free
		if err != nil {
			return nil, err
		}
		wait = 0
		for i := range pg.Objects {
			entries = append(entries, entry[P]{row: pg.Objects[i]})
		}
		for _, f := range pg.Folders {
			entries = append(entries, entry[P]{row: schema.Row[P]{Key: f}, folder: true})
		}

		if len(pg.Objects)+len(pg.Folders) > 0 || !pg.Truncated {
			empty = 0
			d.advanced.Add(1)
		} else {
			d.inc(cEmptyPages)
			if empty++; empty <= keyrange.EmptyChainLive {
				d.advanced.Add(1)
			} else {
				d.emptyChains.Add(1)
			}
		}
		if !pg.Truncated {
			break
		}
		if pg.Token == "" {
			return nil, d.violation(cViolationToken, "a truncated page of folder %+q has no continuation token", prefix)
		}
		if _, dup := seen[pg.Token]; dup {
			return nil, d.violation(cViolationToken, "folder %+q returned a continuation token twice", prefix)
		}
		token = strings.Clone(pg.Token) // the history keeps no page's arena
		seen[token] = struct{}{}
	}

	start := time.Now()
	sortEntries(entries)
	d.add(cSortNs, int64(time.Since(start)))
	d.max(cMaxFolderEntries, int64(len(entries)))
	for i := range entries {
		if i > 0 && entries[i].row.Key == entries[i-1].row.Key {
			return nil, d.violation(cViolationDuplicate, "%+q is listed twice in folder %+q", entries[i].row.Key, prefix)
		}
		if seg := entries[i].row.Key[len(prefix):]; !norm.NFC.IsNormalString(seg) {
			d.inc(cNonNFC)
		}
	}
	return entries, nil
}

// page checks one page of folder prefix and charges what it stages to
// *held (and the reader's total): what its strings retain (or their
// lengths), its entries, and the token it adds to the history.
func (d *Delimited[P]) page(prefix string, pg *FolderPage[P], held *int64, tokens int) error {
	d.inc(cPages)
	n := pg.Retained
	for i := range pg.Objects {
		row := &pg.Objects[i]
		key := row.Key
		switch {
		case key == "":
			return d.violation(cViolationEmptyKey, "an object with an empty key in folder %+q", prefix)
		case !strings.HasPrefix(key, prefix):
			return d.violation(cViolationPrefix, "object %+q is not in folder %+q by its raw bytes", key, prefix)
		case strings.IndexByte(key[len(prefix):], '/') >= 0:
			return d.violation(cViolationDelimiter, "object %+q has a '/' below folder %+q", key, prefix)
		}
		if key == prefix {
			d.inc(cMarkers)
		}
		n += entryBytes[P]()
		if pg.Retained == 0 {
			n += int64(len(key))
			if d.opt.Bytes != nil {
				n += d.opt.Bytes(&row.Data)
			}
		}
	}
	for _, f := range pg.Folders {
		switch rest, ok := strings.CutPrefix(f, prefix); {
		case !ok:
			return d.violation(cViolationPrefix, "child folder %+q is not in folder %+q by its raw bytes", f, prefix)
		case rest == "":
			return d.violation(cViolationSelf, "folder %+q lists itself as a child", prefix)
		case strings.IndexByte(rest, '/') != len(rest)-1:
			return d.violation(cViolationDelimiter, "child folder %+q is not one segment and a '/' below folder %+q", f, prefix)
		case rest == "/":
			d.inc(cEmptySegments)
		}
		n += entryBytes[P]()
		if pg.Retained == 0 {
			n += int64(len(f))
		}
	}
	if pg.Truncated {
		n += int64(len(pg.Token)) + int64(unsafe.Sizeof("")) + mapSlotBytes
	}
	if err := d.grow(prefix, held, n); err != nil {
		return err
	}
	d.add(cObjects, int64(len(pg.Objects)))
	d.add(cPrefixes, int64(len(pg.Folders)))
	return nil
}

// mapSlotBytes is a token history entry's share of its map, an estimate.
const mapSlotBytes = 16

// grow charges n more bytes to folder prefix (*held) and to the reader's
// total, or refuses them: staging never waits.
func (d *Delimited[P]) grow(prefix string, held *int64, n int64) error {
	if *held+n > d.opt.FolderBytes {
		d.inc(cBudgetRefusals)
		return &FolderLimitError{Folder: prefix, Limit: d.opt.FolderBytes}
	}
	if now := d.staged.Add(n); now > d.opt.StagingBytes {
		d.staged.Add(-n)
		d.inc(cBudgetRefusals)
		return &FolderLimitError{Folder: prefix, Limit: d.opt.StagingBytes, Staging: true}
	}
	*held += n
	d.max(cMaxFolderBytes, *held)
	d.max(cPeakStaged, d.staged.Load())
	return nil
}

// reserve takes one page's headroom, waiting while pages in flight hold
// the bound, and returns its release. It cannot deadlock: the headroom is
// the only thing anyone waits for, a holder takes one reservation at a
// time and releases it once its page is decoded and charged (or failed),
// without waiting on anything but its own request, and staging, which a
// waiter may hold, is never waited for (grow refuses instead). A
// reservation never exceeds the bound, and a cancelled wait returns.
func (d *Delimited[P]) reserve(ctx context.Context) (release func(), err error) {
	if d.headroom == nil {
		return func() {}, nil
	}
	n := d.opt.PageHeadroom
	if !d.headroom.TryAcquire(n) {
		d.inc(cHeadroomWaits)
		if d.waiting != nil {
			d.waiting()
		}
		start := time.Now()
		err := d.headroom.Acquire(ctx, n)
		d.add(cHeadroomWaitNs, int64(time.Since(start)))
		if err != nil {
			return nil, err
		}
	}
	return func() { d.headroom.Release(n) }, nil
}

// scope keeps the entries of the start folder prefix whose raw bytes
// start with the scope (Select). An object outside it is left out. A
// child folder F outside it is left out without listing only when both
// the scope's remainder r (Select after prefix) and F's segment are
// ASCII: a key m that starts with Select and that a store grouping names
// in NFC placed under F has NFC(segment of m) equal to F's segment; m's
// segment starts with r, and NFC leaves an ASCII r in place unless it
// composes r's last character into a character that is not ASCII, so an
// ASCII NFC(segment) starts with r, and F would start with Select. This
// needs folder prefixes ending in '/' (true of every folder here) and NFC
// that is segmentable at '/' (no canonical decomposition holds '/', and
// '/' composes with nothing). Any other F is probed: its subtree is
// listed, emitting nothing, so that a key of the scope spelled under it
// another way reaches a listing whose raw prefix it does not start with
// and stops the run (protocol).
func (d *Delimited[P]) scope(ctx context.Context, prefix string, entries []entry[P]) ([]entry[P], error) {
	sel := d.opt.Select
	r := sel[len(prefix):]
	kept := entries[:0]
	for _, e := range entries {
		if strings.HasPrefix(e.row.Key, sel) {
			kept = append(kept, e)
			continue
		}
		d.inc(cOutOfScope)
		if e.folder && !(ascii(r) && ascii(e.row.Key[len(prefix):])) {
			if err := d.probe(ctx, strings.Clone(e.row.Key)); err != nil {
				return nil, err
			}
		}
	}
	clear(entries[len(kept):]) // dropped rows release their strings
	return kept, nil
}

// probe lists folder f and every folder below it, checked as list checks
// any folder, emitting nothing. A folder's listing is released before its
// children are probed; their prefixes are charged while they wait.
func (d *Delimited[P]) probe(ctx context.Context, f string) error {
	d.inc(cProbes)
	var held int64
	entries, err := d.list(ctx, f, &held)
	var children []string
	for i := range entries {
		if entries[i].folder {
			children = append(children, strings.Clone(entries[i].row.Key))
		}
	}
	d.staged.Add(-held)
	held = 0
	if err != nil {
		return err
	}
	var kept int64
	for _, c := range children {
		kept += int64(len(c)) + int64(unsafe.Sizeof(""))
	}
	defer func() { d.staged.Add(-held) }()
	if err := d.grow(f, &held, kept); err != nil {
		return err
	}
	for _, c := range children {
		if err := d.probe(ctx, c); err != nil {
			return err
		}
	}
	return nil
}

// ascii reports whether s is ASCII, which NFC leaves as it is.
func ascii(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= utf8.RuneSelf {
			return false
		}
	}
	return true
}

// emit hands a sorted folder on: runs of objects as Leaf rows, each child
// folder spliced in at its key.
func emit[P any](out *Out[struct{}, struct{}, P], entries []entry[P]) error {
	leaf := func(*schema.Row[P]) (struct{}, Walk) { return struct{}{}, Leaf }
	rows := make([]schema.Row[P], 0, len(entries))
	start := 0
	for i := range entries {
		if !entries[i].folder {
			rows = append(rows, entries[i].row)
			continue
		}
		if len(rows) > start {
			if err := out.Rows(rows[start:len(rows):len(rows)], leaf); err != nil {
				return err
			}
			start = len(rows)
		}
		if err := out.Folder(struct{}{}, entries[i].row.Key); err != nil {
			return err
		}
	}
	if len(rows) > start {
		return out.Rows(rows[start:], leaf)
	}
	return nil
}

// sortEntries sorts a folder's entries by key, unsigned byte order: a
// child folder by its prefix, which ends in '/', so it sorts where its
// subtree's keys belong.
func sortEntries[P any](entries []entry[P]) {
	slices.SortFunc(entries, func(a, b entry[P]) int { return strings.Compare(a.row.Key, b.row.Key) })
}

// entryBytes is what one staged entry costs beside its strings: its
// staging slot and the row it becomes.
func entryBytes[P any]() int64 {
	return 2 * int64(unsafe.Sizeof(entry[P]{}))
}
