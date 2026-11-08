// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/schema"
)

// Durable spill (go/docs/internals/ordering.md, "Durable spill"): a spill
// file that holds every row of a logical interval (cs, ce] of its range, as
// the provider certified with fences (source.Fencer), outlives the run
// until the dataset's published prefix K covers it, so a resume replays it
// instead of listing it again. Its certificate is a record in the spill
// ledger, a file of its own beside the spill files: the part journal keeps
// its single committer, and no cache failure touches it.
//
// A durable file is a sequence of version-2 frames (the journal's
// envelope: length, CRC-32C over the length and the payload, the payload
// whose first byte is its type): H the header, the rows frames (the
// disposable spill's types, decoded whatever this run's flags), C the
// capture start (frames after it are the certificate's rows; none
// straddles it), T the trailer (written once, at the range's completion,
// certified or not), then a fixed 16-byte tail (the trailer's offset and
// the magic) that locates it in one read.

const (
	durableMagic   = "SWATHDS2" // a durable file's header and tail
	ledgerMagic    = "SWATHDL1" // the ledger's first frame
	durableVersion = 2
	ledgerVersion  = 1
	frameHeader    = 'H'
	frameCapture   = 'C'
	tailSize       = 16
	ledgerSealed   = 'S'
	ledgerRetired  = 'X'
	ledgerHeader   = 'L'
	// maxLedgerFrame bounds a ledger frame read back, before allocation: a
	// record holds three keys (each at most a provider's key, 1 KiB on S3
	// and GCS) and fixed fields.
	maxLedgerFrame = 64 << 10
	// historyCap (H) bounds the ledger's retained durable prefix plus the
	// records reserved and not yet written (a seal and a retirement per
	// certificate): the ledger is never compacted, so this bounds its size
	// and the recovery scan (40,000 records of at most a few KiB). It does
	// not change with the certificate cap; a resume counts the prefix it
	// kept, so history a truncation dropped is refunded.
	historyCap = 40_000
	// maxCertificates is the largest certificate cap any attempt may have
	// (the cap of --spill-max none, and of a cap of 10,000 MiB or more):
	// recovery inventories at most that many before applying its own cap.
	maxCertificates = 10_000
	// minCertificates is the smallest cap: below 16 MiB of --spill-max
	// durable spill is off (a certificate per MiB of the cap; sixteen is
	// the fewest worth a ledger).
	minCertificates = 16
	// flushInterval is the flusher's shortest cycle: seals and
	// retirements are batched into one directory sync, one ledger append
	// and one ledger sync per cycle at most every 100 ms (contract §7.6,
	// the recommended default).
	flushInterval = 100 * time.Millisecond
	// replayPageKeys is the page size requests_saved_est divides by: the
	// engine's default page (keyrange.PageKeys).
	replayPageKeys = 1000
	// rowCodecVersion is the version of the rows frames' encoding under a
	// codec ID (the schema ID): the disposable spill's plain, delta and
	// zstd frame bodies as spill.go writes them. A durable file whose
	// header names another codec ID or version is not replayable by this
	// build: recovery rejects it (rejected.codec) and lists its interval.
	rowCodecVersion = 1
)

// DurableDir is the durable spill directory, relative to the output.
var DurableDir = filepath.Join(dataset.JournalDir, "spill", "durable")

// ledgerName is the ledger's name inside DurableDir.
const ledgerName = "ledger"

// CertificateCap is the certificate cap for a live spill cap of max bytes
// (0: none): one per MiB, clamped to [16, 10,000]; 0 (durable spill off)
// below 16 MiB.
func CertificateCap(max int64) int {
	switch {
	case max == 0:
		return maxCertificates
	case max < minCertificates<<20:
		return 0
	}
	return int(min(max>>20, maxCertificates))
}

// Cert is one durable file and, once sealed, its certificate (cs, ce].
type Cert struct {
	id   [16]byte
	name string // <hex id>.spill, inside DurableDir
	// The node's logical bounds (the header), and the certificate.
	lo, hi      string
	open        bool
	cs, ce      string
	ceOpen      bool
	rows        int64 // every rows frame's rows (total)
	captured    int64 // rows after C
	first, last string
	bytes       int64  // the file's length once sealed
	crc         uint32 // CRC-32C of the whole file once sealed
	certified   bool   // sealed with a certificate (else live-only)
	slot        bool   // holds a slot and a retirement record's reservation
	reused      bool   // accepted by recovery
	most, owned int64  // recovery: the largest frame's rows and declared storage (replay's decode bounds)
	charged     int64  // bytes charged to the live spill budget (returned at unlink)
	drained     bool   // its reader closed
	retiring    bool   // queued for retirement or unlink
	codec       string // recovery: the header's row codec ID
	codecVer    uint64 // recovery: the header's row codec version
}

// fileName is the durable file's name for id.
func fileName(id [16]byte) string { return hex.EncodeToString(id[:]) + ".spill" }

// rel is the path of name relative to the output root.
func (c *Cache) rel(name string) string { return filepath.Join(DurableDir, name) }

// ledgerRec is one ledger record to append.
type ledgerRec struct {
	cert   *Cert
	retire string // "": sealed; else the retirement reason
}

// Cache is the durable spill of one listing attempt: its files, their
// ledger, the certificate slots and the ledger history, and the flusher
// that owns every ledger append and every durable file's unlink. It is
// owned by the attempt, not by Run, and outlives Run: retirements follow
// part publication, which goes on after the order stage has returned.
//
// Locks: mu (slots, queues, K) is taken under the order stage's mutex
// (an abandon) and under lmu (the flusher takes its queue); lmu, the
// ledger mutex, is taken only by the flusher, recovery and Close, never
// with the order stage's mutex held, and the flusher never takes that
// mutex: spill credit goes back through Run's waker (credit).
type Cache struct {
	root   *os.Root
	verify func() error
	uuid   [16]byte
	ident  string
	codec  string // the run's row codec ID (the schema ID): recovery accepts only files of it
	k      string // K, the published prefix (mu)
	hasK   bool

	mu        sync.Mutex
	cap       int // certificate slots
	slots     int // slots held: capturing, sealed and reused certificates
	hist      int // ledger records in the prefix and written since, plus reservations
	stopping  bool
	failed    bool // the ledger failed: no new slot, records dropped, unlinks continue
	fatal     error
	detached  bool // the K callback is detached (SetK is a no-op)
	queue     []ledgerRec
	unlinks   []*Cert
	drained   []*Cert // drained sealed files waiting for K, oldest first
	reused    []*Cert // recovery's accepted certificates, ascending
	cancel    func(error)
	recovered []ledgerRec // recovery's retirements, written before the flusher starts
	resumed   bool

	lmu     sync.Mutex
	ledger  *os.File
	lend    int64 // the ledger's appended end
	durable int64 // the end known durable (sealed_durable)

	kick    chan struct{}
	stop    chan struct{}
	done    chan struct{}
	started bool
	closed  bool

	credit   atomic.Pointer[func(int64)] // Run's: lowers live spill bytes and wakes waiters
	retained atomic.Int64                // bytes of retained durable files (charged)
	peakRet  atomic.Int64

	st durableStats
}

// durableStats are the durable spill's counters, reported as DurableReport.
type durableStats struct {
	CapturedRanges        atomic.Int64
	CaptureAtStart        atomic.Int64
	CaptureAtFence        atomic.Int64
	CapturePendingDropped atomic.Int64
	NoSlot                atomic.Int64
	HistoryExhausted      atomic.Int64
	AbandonedAwaited      atomic.Int64
	AbandonedRefused      atomic.Int64
	AbandonedUnfenced     atomic.Int64
	AbandonedStopping     atomic.Int64
	AbandonedEmpty        atomic.Int64
	SealedFiles           atomic.Int64
	SealedDurable         atomic.Int64
	FlushNs               atomic.Int64
	FlushCycles           atomic.Int64
	LedgerFailed          atomic.Int64
	RetiredPublished      atomic.Int64
	RetiredEvicted        atomic.Int64
	RetiredRecovery       atomic.Int64
	RetireDeferred        atomic.Int64
	Unlinked              atomic.Int64
	DirsCreated           atomic.Int64
	// Resume.
	LedgerDroppedBytes  atomic.Int64
	Accepted            atomic.Int64
	RejectedMissing     atomic.Int64
	RejectedCorrupt     atomic.Int64
	RejectedForeign     atomic.Int64
	RejectedCodec       atomic.Int64
	RejectedOverlap     atomic.Int64
	RejectedBudget      atomic.Int64
	Orphans             atomic.Int64
	ReplayedRows        atomic.Int64
	ReplaySkippedRows   atomic.Int64
	ReplayedCerts       atomic.Int64
	Gaps                atomic.Int64
	RequestsSavedEst    atomic.Int64
	AcceptedBytes       atomic.Int64
	LedgerTruncatedFrom atomic.Int64
	LedgerCut           atomic.Int64
}

// DurableReport is durableStats for the run report
// (resources.spill.durable), with the caps in effect.
type DurableReport struct {
	Slots                 int              `json:"slots"`       // the certificate cap
	History               int              `json:"history"`     // ledger records retained and reserved at the end
	HistoryCap            int              `json:"history_cap"` // H
	CapturedRanges        int64            `json:"captured_ranges"`
	CaptureAt             map[string]int64 `json:"capture_at"`
	CapturePendingDropped int64            `json:"capture_pending_dropped"`
	NoSlot                int64            `json:"no_slot"`
	HistoryExhausted      int64            `json:"history_exhausted"`
	Abandoned             map[string]int64 `json:"abandoned"`
	SealedFiles           int64            `json:"sealed_files"`
	SealedDurable         int64            `json:"sealed_durable"`
	FlushNs               int64            `json:"flush_ns"`
	FlushCycles           int64            `json:"flush_cycles"`
	LedgerFailed          bool             `json:"ledger_failed"`
	RetainedBytes         int64            `json:"retained_bytes"`
	PeakRetainedBytes     int64            `json:"peak_retained_bytes"`
	Retired               map[string]int64 `json:"retired"`
	RetireDeferred        int64            `json:"retire_deferred"`
	Resume                *DurableResume   `json:"resume,omitempty"` // nil on a fresh run
}

// DurableResume is a resume's account of the durable spill it found.
type DurableResume struct {
	LedgerDroppedBytes int64            `json:"ledger_dropped_bytes"`
	LedgerCut          bool             `json:"ledger_cut"` // the prefix was cut at H records or 10,000 live certificates
	Accepted           int64            `json:"accepted"`
	AcceptedBytes      int64            `json:"accepted_bytes"`
	Rejected           map[string]int64 `json:"rejected"`
	Orphans            int64            `json:"orphans"`
	ReplayedRows       int64            `json:"replayed_rows"`
	ReplaySkippedRows  int64            `json:"replay_skipped_rows"`
	Gaps               int64            `json:"gaps"`
	RequestsSavedEst   int64            `json:"requests_saved_est"`
}

// Report is the cache's report; nil reads as nil.
func (c *Cache) Report() *DurableReport {
	if c == nil {
		return nil
	}
	s := &c.st
	c.mu.Lock()
	slots, hist, failed := c.cap, c.hist, c.failed
	c.mu.Unlock()
	r := &DurableReport{Slots: slots, History: hist, HistoryCap: historyCap,
		CapturedRanges:        s.CapturedRanges.Load(),
		CaptureAt:             map[string]int64{"range_start": s.CaptureAtStart.Load(), "fence": s.CaptureAtFence.Load()},
		CapturePendingDropped: s.CapturePendingDropped.Load(), NoSlot: s.NoSlot.Load(), HistoryExhausted: s.HistoryExhausted.Load(),
		Abandoned: map[string]int64{"awaited": s.AbandonedAwaited.Load(), "refused": s.AbandonedRefused.Load(), "unfenced": s.AbandonedUnfenced.Load(),
			"stopping": s.AbandonedStopping.Load(), "empty": s.AbandonedEmpty.Load()},
		SealedFiles: s.SealedFiles.Load(), SealedDurable: s.SealedDurable.Load(), FlushNs: s.FlushNs.Load(), FlushCycles: s.FlushCycles.Load(),
		LedgerFailed: failed, RetainedBytes: c.retained.Load(), PeakRetainedBytes: c.peakRet.Load(),
		Retired:        map[string]int64{"published": s.RetiredPublished.Load(), "evicted": s.RetiredEvicted.Load(), "recovery": s.RetiredRecovery.Load()},
		RetireDeferred: s.RetireDeferred.Load()}
	if c.resumed {
		r.Resume = &DurableResume{LedgerDroppedBytes: s.LedgerDroppedBytes.Load(), LedgerCut: s.LedgerCut.Load() > 0, Accepted: s.Accepted.Load(), AcceptedBytes: s.AcceptedBytes.Load(),
			Rejected: map[string]int64{"missing": s.RejectedMissing.Load(), "corrupt": s.RejectedCorrupt.Load(), "foreign": s.RejectedForeign.Load(),
				"codec": s.RejectedCodec.Load(), "overlap": s.RejectedOverlap.Load(), "budget": s.RejectedBudget.Load()},
			Orphans: s.Orphans.Load(), ReplayedRows: s.ReplayedRows.Load(), ReplaySkippedRows: s.ReplaySkippedRows.Load(),
			Gaps: s.Gaps.Load(), RequestsSavedEst: s.RequestsSavedEst.Load()}
	}
	return r
}

// Retire reasons (the ledger's retired records, DurableReport.Retired).
const (
	retirePublished = "published"
	retireEvicted   = "evicted"
	retireRecovery  = "recovery"
)

var retireCode = map[string]byte{retirePublished: 1, retireEvicted: 2, retireRecovery: 3}

// newCache is a cache over root's durable directory, before its ledger is
// opened.
func newCache(root *os.Root, verify func() error, uuid, ident, codec string) (*Cache, error) {
	c := &Cache{root: root, verify: verify, ident: ident, codec: codec, kick: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{})}
	if c.verify == nil {
		c.verify = func() error { return nil }
	}
	b, err := hex.DecodeString(uuid)
	if err != nil || len(b) != 16 {
		return nil, fmt.Errorf("durable spill: run UUID %q", uuid)
	}
	copy(c.uuid[:], b)
	return c, nil
}

// hookEvent sees the durable spill's lifecycle events, in order (tests:
// SetDurableEvents).
var hookEvent atomic.Pointer[func(string)]

func event(e string) {
	if f := hookEvent.Load(); f != nil {
		(*f)(e)
	}
}

// SetDurableEvents installs f to see the durable spill's lifecycle events
// ("sealed", "setk", "setk-detached", "detach", "close", "cycle", "cycle-records", "flusher-joined",
// "ledger-closed", and Run's "waker-stopped"), for the shutdown-order
// tests; nil removes it. It returns a function restoring the previous one.
func SetDurableEvents(f func(string)) (restore func()) {
	var p *func(string)
	if f != nil {
		p = &f
	}
	old := hookEvent.Swap(p)
	return func() { hookEvent.Store(old) }
}

// SetK records K, the dataset's published prefix, after a part record:
// drained certificates at or below it are queued for retirement (never a
// wait). After DetachK it does nothing.
func (c *Cache) SetK(k string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.detached {
		event("setk-detached")
		return
	}
	event("setk")
	c.k, c.hasK = k, true
	kept := c.drained[:0]
	for _, d := range c.drained {
		if c.coveredLocked(d) {
			c.retireLocked(d, retirePublished)
			continue
		}
		kept = append(kept, d)
	}
	clear(c.drained[len(kept):])
	c.drained = kept
}

// coveredLocked: K covers d's certificate (mu held).
func (c *Cache) coveredLocked(d *Cert) bool { return c.hasK && !d.ceOpen && d.ce <= c.k }

// DetachK detaches the K callback: later SetK calls are no-ops (the
// shutdown's step 4, before Close).
func (c *Cache) DetachK() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.detached = true
	c.mu.Unlock()
	event("detach")
}

// Stop starts the shutdown: no slot or seal starts after it.
func (c *Cache) Stop() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.stopping = true
	c.mu.Unlock()
}

// Err is the flusher's fatal error (ErrOutputReplaced, a failed unlink),
// nil without one.
func (c *Cache) Err() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.fatal
}

// reserve takes a certificate slot and two history records (the seal and
// the retirement), or nothing; it never waits.
func (c *Cache) reserve() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case c.stopping || c.failed || c.fatal != nil || !c.started:
		return false
	case c.slots >= c.cap:
		c.st.NoSlot.Add(1)
		return false
	case c.hist+2 > historyCap:
		c.st.HistoryExhausted.Add(1)
		return false
	}
	c.slots++
	c.hist += 2
	return true
}

// abandon returns an unsealed file's slot and history; the file serves the
// live run only (unlinked once drained).
func (c *Cache) abandon(d *Cert) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if d.slot {
		d.slot = false
		c.slots--
		c.hist -= 2
	}
}

// sealable reports whether a seal may start (not stopping, the ledger
// healthy).
func (c *Cache) sealable() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.stopping && !c.failed && c.fatal == nil
}

// sealed hands a sealed file's record to the flusher (its file and data
// are synced); the node never waits for the ledger.
func (c *Cache) sealed(d *Cert) {
	c.mu.Lock()
	d.certified = true
	c.queue = append(c.queue, ledgerRec{cert: d})
	c.st.SealedFiles.Add(1)
	c.mu.Unlock()
	event("sealed")
	c.wakeFlusher()
}

// charge counts n bytes of a durable file against the retained bytes.
func (c *Cache) charge(d *Cert, n int64) {
	d.charged += n
	r := c.retained.Add(n)
	for p := c.peakRet.Load(); r > p && !c.peakRet.CompareAndSwap(p, r); p = c.peakRet.Load() {
	}
}

// drainedFile is the drainer's: d's reader is closed. A live-only file is
// unlinked; a certificate waits for K (or is retired now if K covers it).
func (c *Cache) drainedFile(d *Cert) {
	c.mu.Lock()
	defer c.mu.Unlock()
	d.drained = true
	switch {
	case !d.certified:
		if d.slot { // capture never closed (a failed range): live-only
			d.slot = false
			c.slots--
			c.hist -= 2
		}
		c.unlinkLocked(d)
	case c.coveredLocked(d):
		c.retireLocked(d, retirePublished)
	default:
		c.st.RetireDeferred.Add(1)
		c.drained = append(c.drained, d)
	}
}

// evict retires the oldest drained certificate (a refused frame with
// drained files retained): its credit returns at its unlink. False when
// none is drained; undrained files are never evicted.
func (c *Cache) evict() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.drained) == 0 {
		return false
	}
	d := c.drained[0]
	c.drained = append(c.drained[:0], c.drained[1:]...)
	c.retireLocked(d, retireEvicted)
	return true
}

// retireLocked queues d's retirement: its record (with a healthy ledger)
// and its unlink, which never waits on the record (mu held).
func (c *Cache) retireLocked(d *Cert, why string) {
	if d.retiring {
		return
	}
	switch why {
	case retirePublished:
		c.st.RetiredPublished.Add(1)
	case retireEvicted:
		c.st.RetiredEvicted.Add(1)
	case retireRecovery:
		c.st.RetiredRecovery.Add(1)
	}
	if d.slot {
		d.slot = false
		c.slots--
	}
	if !c.failed {
		c.queue = append(c.queue, ledgerRec{cert: d, retire: why})
	}
	c.unlinkLocked(d)
}

// unlinkLocked queues d's unlink (mu held).
func (c *Cache) unlinkLocked(d *Cert) {
	if d.retiring {
		return
	}
	d.retiring = true
	c.unlinks = append(c.unlinks, d)
	select {
	case c.kick <- struct{}{}:
	default:
	}
}

func (c *Cache) wakeFlusher() {
	select {
	case c.kick <- struct{}{}:
	default:
	}
}

// attach installs Run's credit callback; detach (nil) drops later credit.
func (c *Cache) attach(f func(int64)) {
	if f == nil {
		c.credit.Store(nil)
		return
	}
	c.credit.Store(&f)
}

// flusher owns the ledger and the unlinks from Start to Close: a cycle at
// most every flushInterval, when something is queued.
func (c *Cache) flusher() {
	defer close(c.done)
	for {
		select {
		case <-c.kick:
		case <-c.stop:
			c.cycle()
			return
		}
		start := time.Now()
		c.cycle()
		if d := flushInterval - time.Since(start); d > 0 {
			t := time.NewTimer(d)
			select {
			case <-t.C:
			case <-c.stop:
				t.Stop()
				c.cycle()
				return
			}
		}
	}
}

// hookLedgerLocked sees every acquisition of the ledger mutex (tests: the
// lock-order assertion).
var hookLedgerLocked func()

func (c *Cache) lockLedger() {
	c.lmu.Lock()
	if hookLedgerLocked != nil {
		hookLedgerLocked()
	}
}

// cycle is one flusher cycle: the directory sync (covering every name
// handed over), the queued records appended, the ledger synced, then the
// queued unlinks, each returning its credit; Lock.Verify precedes each of
// these mutations (owned).
func (c *Cache) cycle() {
	c.lockLedger()
	defer c.lmu.Unlock()
	c.mu.Lock()
	recs, unl, failed, fatal := c.queue, c.unlinks, c.failed, c.fatal
	c.queue, c.unlinks = nil, nil
	c.mu.Unlock()
	if fatal != nil || (len(recs) == 0 && len(unl) == 0) {
		return
	}
	start := time.Now()
	event("cycle")
	if len(recs) > 0 && !failed {
		event("cycle-records") // a cycle that appends records (the gated kill matrix counts them)
	}
	defer func() {
		c.st.FlushNs.Add(int64(time.Since(start)))
		c.st.FlushCycles.Add(1)
	}()
	if len(recs) > 0 && !failed {
		var re *replacedError
		if err := c.appendRecords(recs); errors.As(err, &re) {
			c.fail(re.err) // the output replaced: nothing more is touched
			return
		} else if errors.Is(err, faults.ErrCrash) {
			c.fail(err) // an injected kill: nothing more is written
			return
		} else if err != nil {
			c.ledgerFailed()
		}
	}
	for _, d := range unl {
		if err := c.unlink(d); err != nil {
			var re *replacedError
			if errors.As(err, &re) {
				err = re.err
			}
			c.fail(err)
			return
		}
	}
}

// replacedError is Lock.Verify's failure before a mutation: the output (or
// its lock file) was replaced, which is fatal, never a ledger failure.
type replacedError struct{ err error }

func (e *replacedError) Error() string { return e.err.Error() }
func (e *replacedError) Unwrap() error { return e.err }

// owned is Lock.Verify immediately before one durable-spill mutation
// (directory sync, append, sync, truncate, create, unlink): a replacement
// detected while an earlier step was blocked stops the next one, so a
// replaced output is never touched (contract §2, §4). It is checked per
// mutation, not per batch: a single check before a cycle's many steps
// would let them continue after the output changed hands.
func (c *Cache) owned() error {
	if err := c.verify(); err != nil {
		return &replacedError{err}
	}
	return nil
}

// appendRecords is the ledger step of a cycle: the directory sync, the
// records, the ledger sync (lmu held).
func (c *Cache) appendRecords(recs []ledgerRec) error {
	err := faults.At("spill-ledger-dirsync")
	if err == nil {
		err = c.owned()
	}
	if err == nil {
		err = dataset.SyncDirIn(c.root, DurableDir)
	}
	if err != nil {
		return err
	}
	var buf []byte
	sealed := int64(0)
	for _, r := range recs {
		buf = appendLedgerFrame(buf, r)
		if r.retire == "" {
			sealed++
		}
	}
	if err := faults.At("spill-ledger-append"); err != nil {
		return err
	}
	if err := c.owned(); err != nil {
		return err
	}
	if _, err := faults.Write(c.ledger, buf); err != nil {
		// A partial append leaves a torn tail: the next recovery reads the
		// prefix before it; this ledger takes no more records.
		return err
	}
	c.lend += int64(len(buf))
	if err := faults.At("spill-ledger-sync"); err != nil {
		return err
	}
	if err := c.owned(); err != nil {
		return err
	}
	if err := faults.Sync(c.ledger); err != nil {
		return err
	}
	c.durable = c.lend
	c.st.SealedDurable.Add(sealed)
	return nil
}

// ledgerFailed latches the ledger failed: no new slot, queued records
// dropped (recovery relists), unlinks continue.
func (c *Cache) ledgerFailed() {
	c.mu.Lock()
	c.failed = true
	c.queue = nil
	c.mu.Unlock()
	c.st.LedgerFailed.Add(1)
}

// fail is a fatal flusher error: the flusher touches nothing more and the
// run is cancelled with err.
func (c *Cache) fail(err error) {
	c.mu.Lock()
	if c.fatal == nil {
		c.fatal = err
	}
	cancel := c.cancel
	c.mu.Unlock()
	if cancel != nil {
		cancel(err)
	}
}

// unlink removes d's file through the root and returns its credit; a file
// already gone is no error (lmu held; Verify immediately before it).
func (c *Cache) unlink(d *Cert) error {
	if err := faults.At("spill-unlink"); err != nil {
		return err
	}
	if err := c.owned(); err != nil {
		return err
	}
	if err := c.root.Remove(c.rel(d.name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("durable spill: unlink %s: %w", d.name, err)
	}
	c.st.Unlinked.Add(1)
	if n := d.charged; n > 0 {
		d.charged = 0
		c.retained.Add(-n)
		if f := c.credit.Load(); f != nil {
			(*f)(n)
		}
	}
	return nil
}

// Close ends the cache (the shutdown's step 4): stopping, the queue
// drained by a last cycle, the flusher joined, the ledger closed. Files
// stay: on a failure they are a resume's; on a commit the caller removes
// the directory (RemoveDir).
func (c *Cache) Close() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.stopping = true
	closed, started := c.closed, c.started
	c.closed = true
	c.mu.Unlock()
	if closed {
		return
	}
	event("close")
	if started {
		close(c.stop)
		<-c.done
		event("flusher-joined")
	}
	c.lmu.Lock()
	if c.ledger != nil {
		c.ledger.Close()
		c.ledger = nil
		event("ledger-closed")
	}
	c.lmu.Unlock()
}

// Abort is Close after an injected crash (faults.ErrCrash): the flusher
// touches nothing more, as a killed process would not.
func (c *Cache) Abort(err error) {
	if c == nil {
		return
	}
	c.mu.Lock()
	if c.fatal == nil {
		c.fatal = err
	}
	c.mu.Unlock()
	c.Close()
}

// RemoveDir removes the durable directory through the root, after Verify:
// a committed run's (its manifest installed, the cache closed).
func (c *Cache) RemoveDir() error {
	if c == nil {
		return nil
	}
	if err := c.verify(); err != nil {
		return err
	}
	return c.root.RemoveAll(DurableDir)
}

// Spans are the accepted certificates' intervals, ascending, for the
// provider to list around (source.Reusing).
func (c *Cache) Spans() []Span {
	if c == nil {
		return nil
	}
	out := make([]Span, len(c.reused))
	for i, d := range c.reused {
		out[i] = Span{Lo: d.cs, Hi: d.ce, HiOpen: d.ceOpen}
	}
	return out
}

// Span is a certificate's (Lo, Hi] (source.Span's fields).
type Span struct {
	Lo, Hi string
	HiOpen bool
}

// randomID is a new file ID.
func randomID() [16]byte {
	var id [16]byte
	rand.Read(id[:])
	return id
}

// frame2 is a version-2 frame: length, CRC-32C over the length and the
// payload (type, body).
func frame2(dst []byte, typ byte, body []byte) []byte {
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[:4], uint32(len(body)+1))
	sum := crc32.Update(crc32.Checksum(hdr[:4], spillCRC), spillCRC, []byte{typ})
	sum = crc32.Update(sum, spillCRC, body)
	binary.LittleEndian.PutUint32(hdr[4:], sum)
	dst = append(dst, hdr[:]...)
	dst = append(dst, typ)
	return append(dst, body...)
}

// seal2 sets the length and version-2 CRC of a frame built in place.
func seal2(frame []byte) {
	binary.LittleEndian.PutUint32(frame[:4], uint32(len(frame)-8))
	binary.LittleEndian.PutUint32(frame[4:8], crc32.Update(crc32.Checksum(frame[:4], spillCRC), spillCRC, frame[8:]))
}

// check2 verifies a version-2 frame's CRC.
func check2(hdr [8]byte, payload []byte) bool {
	return crc32.Update(crc32.Checksum(hdr[:4], spillCRC), spillCRC, payload) == binary.LittleEndian.Uint32(hdr[4:])
}

// headerBody is a durable file's H body: magic, format version, run UUID,
// file ID, the journal identity's digest, the row codec ID (the schema ID)
// and version, the writer's largest frame rows and decoded bytes, the
// node's logical bounds.
func (c *Cache) headerBody(d *Cert, codec string) []byte {
	b := append([]byte(durableMagic), durableVersion)
	b = append(b, c.uuid[:]...)
	b = append(b, d.id[:]...)
	b = schema.AppendString(b, c.ident)
	b = schema.AppendString(b, codec)
	b = binary.AppendUvarint(b, rowCodecVersion)
	b = binary.AppendUvarint(b, publishRows)
	b = binary.AppendUvarint(b, maxSpillDecoded)
	b = schema.AppendString(b, d.lo)
	b = schema.AppendString(b, d.hi)
	return append(b, boolByte(d.open))
}

func boolByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

// Trailer flags.
const (
	trailerCertified = 1 << iota
	trailerCeOpen
	trailerCaptured // a C frame precedes it
)

// trailerBody is a durable file's T body: total and captured rows, flags,
// cs, ce, the first and last captured keys, the trailer's offset and the
// CRC-32C of every byte before the trailer.
func trailerBody(d *Cert, captured bool, off int64, prior uint32) []byte {
	b := binary.AppendUvarint(nil, uint64(d.rows))
	b = binary.AppendUvarint(b, uint64(d.captured))
	flags := byte(0)
	if d.certified {
		flags |= trailerCertified
	}
	if d.ceOpen {
		flags |= trailerCeOpen
	}
	if captured {
		flags |= trailerCaptured
	}
	b = append(b, flags)
	b = schema.AppendString(b, d.cs)
	b = schema.AppendString(b, d.ce)
	b = schema.AppendString(b, d.first)
	b = schema.AppendString(b, d.last)
	b = binary.LittleEndian.AppendUint64(b, uint64(off))
	return binary.LittleEndian.AppendUint32(b, prior)
}

// trailer is a parsed T body.
type trailer struct {
	rows, captured           int64
	certified, open, capture bool
	cs, ce, first, last      string
	off                      int64
	prior                    uint32
}

func parseTrailer(body []byte) (trailer, bool) {
	var t trailer
	d := schema.NewDecoder(body)
	t.rows, t.captured = int64(d.Uvarint()), int64(d.Uvarint())
	flags := d.Byte()
	t.certified, t.open, t.capture = flags&trailerCertified != 0, flags&trailerCeOpen != 0, flags&trailerCaptured != 0
	t.cs, t.ce, t.first, t.last = strings.Clone(d.Str()), strings.Clone(d.Str()), strings.Clone(d.Str()), strings.Clone(d.Str())
	fixed := d.Fixed(12)
	if d.Err() != nil || d.Len() != 0 || len(fixed) != 12 || t.rows < 0 || t.captured < 0 || t.captured > t.rows {
		return t, false
	}
	t.off = int64(binary.LittleEndian.Uint64(fixed[:8]))
	t.prior = binary.LittleEndian.Uint32(fixed[8:])
	return t, true
}

// tail is the fixed tail after the trailer frame.
func tail(off int64) []byte {
	b := binary.LittleEndian.AppendUint64(nil, uint64(off))
	return append(b, durableMagic...)
}

// appendLedgerFrame appends r's ledger frame.
func appendLedgerFrame(dst []byte, r ledgerRec) []byte {
	d := r.cert
	if r.retire != "" {
		return frame2(dst, ledgerRetired, append(append([]byte(nil), d.id[:]...), retireCode[r.retire]))
	}
	b := append([]byte(nil), d.id[:]...)
	b = schema.AppendString(b, d.cs)
	b = schema.AppendString(b, d.ce)
	b = append(b, boolByte(d.ceOpen))
	b = binary.AppendUvarint(b, uint64(d.captured))
	b = schema.AppendString(b, d.last)
	b = binary.AppendUvarint(b, uint64(d.bytes))
	b = binary.LittleEndian.AppendUint32(b, d.crc)
	return frame2(dst, ledgerSealed, b)
}

// ledgerHeaderBody is the ledger's first frame: magic, version, run UUID,
// identity digest.
func (c *Cache) ledgerHeaderBody() []byte {
	b := append([]byte(ledgerMagic), ledgerVersion)
	b = append(b, c.uuid[:]...)
	return schema.AppendString(b, c.ident)
}
