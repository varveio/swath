// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/varveio/swath/internal/journal"
	"github.com/varveio/swath/internal/jsontime"
	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/source"
)

// Progress is a run's live progress, read while it runs: by the command's
// watchdog (is the run still making real progress?) and for the progress
// file a supervising process watches. Every phase advances it: listing
// (rows the provider emitted), ordering (rows handed to the router),
// encoding (rows written into parts) and publication (parts published).
// Requests the source's service answered are counted beside it: liveness,
// not progress.
type Progress struct {
	started  atomic.Int64 // unix nanoseconds
	listed   atomic.Pointer[func() int64]
	answered atomic.Pointer[func() int64]
	frontier atomic.Pointer[func() (int64, int64)]
	faults   atomic.Pointer[func() (int64, int64)]
	// contention reads the engine's decision mutex (source.Contended).
	contention atomic.Pointer[func() (time.Duration, time.Duration)]
	// requests reads the source's live request account (source.Requested);
	// tariff its reference rate per 1,000 requests (0: none known).
	requests atomic.Pointer[func() source.Requests]
	tariff   atomic.Uint64 // math.Float64bits
	retained atomic.Int64  // rows kept from the interrupted run (resume)
	ordered  atomic.Int64
	encoded  atomic.Int64
	parts    atomic.Int64
	lastPart atomic.Int64 // unix nanoseconds of the last part published
	verified atomic.Int64 // bytes of retained parts read by a resume's verification
	gauges   atomic.Pointer[order.Gauges]
	state    atomic.Pointer[string]    // a State*; nil: StateListing
	ids      atomic.Pointer[[2]string] // the attempt's and the run's ids

	mu    sync.Mutex    // held across an owned write and across a change of ownership
	owner *journal.Lock // the output's lock while the run holds it, else nil
}

// Owned calls write while the run owns its output directory and its
// pathname still names the directory the run locked (journal.Lock.Verify),
// and reports whether it did. Ownership cannot end during write, so a
// process whose run was refused, or has released the output, never writes
// into it, and a run whose output was moved or replaced does not write
// into what the pathname names now (a replacement between the check and
// the write is not excluded; see Verify).
func (p *Progress) Owned(write func()) bool {
	if p == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.owner == nil || p.owner.Verify() != nil {
		return false
	}
	write()
	return true
}

// own records the output's lock while the run holds it (nil: released).
func (p *Progress) own(l *journal.Lock) {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.owner = l
	p.mu.Unlock()
}

// identify names the attempt and the run p is the progress of.
func (p *Progress) identify(att *Attempt, run *RunInfo) {
	if p == nil {
		return
	}
	var ids [2]string
	if att != nil {
		ids[0] = att.ID
	}
	if run != nil {
		ids[1] = run.ID
	}
	p.ids.Store(&ids)
}

// Started reports whether the run has started listing (a refusal under
// the output's lock never does).
func (p *Progress) Started() bool { return p != nil && p.started.Load() != 0 }

// RunID is the id of the run p is the progress of ("" before its output
// is opened).
func (p *Progress) RunID() string {
	if p == nil {
		return ""
	}
	if ids := p.ids.Load(); ids != nil {
		return ids[1]
	}
	return ""
}

// setState records the run's state (a State*).
func (p *Progress) setState(s string) {
	if p != nil {
		p.state.Store(&s)
	}
}

// End records how the run ended: StateDone without err, else StateFailed.
func (p *Progress) End(err error) {
	if err == nil {
		p.setState(StateDone)
	} else {
		p.setState(StateFailed)
	}
}

// ProgressVersion is the progress file's schema version, its
// progress_version member.
const ProgressVersion = 1

// A run's states, in order (ProgressSnapshot.State). A run that ends is
// done or failed, its last snapshot with its final counts.
const (
	StateListing    = "listing"    // the provider lists (and a resume verifies its retained parts first)
	StateDraining   = "draining"   // the provider finished: the order stage hands on the rows it holds
	StatePublishing = "publishing" // the last part is written: the manifest is being committed
	StateDone       = "done"       // the run finished: its dataset committed, its text written
	StateFailed     = "failed"     // the run ended without its output
)

// ProgressSnapshot is Progress at one moment. Its counts are named as the
// run report's (rows_fetched, rows_emitted, parts_written, http_attempts).
type ProgressSnapshot struct {
	ProgressVersion int    `json:"progress_version"`
	AttemptID       string `json:"attempt_id,omitempty"` // the report's attempt.id
	RunID           string `json:"run_id,omitempty"`     // the report's run.id (absent until the output is opened)
	State           string `json:"state"`                // a State*
	Rows            int64  `json:"rows_fetched"`         // rows listed so far (directories and versions included)
	Ordered         int64  `json:"rows_emitted"`         // rows handed to the encoders
	Encoded         int64  `json:"rows_encoded"`         // rows written into parts
	Parts           int64  `json:"parts_written"`        // parts published
	// Verified is the bytes of retained parts a resume has read to check
	// them against the journal (size and MD5), before it lists.
	Verified int64 `json:"verified_bytes"`
	// Scanned is the bytes of reused certificates a resume's segments have
	// read before their cut (order.Gauges.CertScanned): no row moves then.
	Scanned int64 `json:"scanned_bytes"`
	Advance int64 `json:"advance"` // the sum: it rises in every phase (verification included) while the run progresses
	// Answered counts requests the source's service answered usably (pages
	// without rows included): it shows the run is alive while no row moves.
	Answered int64 `json:"answered"`
	// Frontier counts the source's key-space progress (source.Advancing):
	// pages that held an entry or ended a chain, and empty continuing
	// pages within the first 1,024 of their run; EmptyChain the empty
	// pages past that bound. The watchdog takes Frontier in place of
	// Answered when the source counts it.
	Frontier   int64 `json:"frontier"`
	EmptyChain int64 `json:"empty_chain_pages"`
	advancing  bool  // the source counts Frontier
	// Throttles and Timeouts count the service's throttle answers and the
	// attempts past the request timeout (source.Faulted; 0 otherwise).
	Throttles int64 `json:"throttles"`
	Timeouts  int64 `json:"timeouts"`
	// Retained is the rows a resume kept from the interrupted run (not
	// listed by this attempt, so not in Rows).
	Retained int64 `json:"rows_retained"`
	// Requests (http_attempts), Retries, Inflight and Limit are the source's HTTP account
	// (source.Requested; 0 otherwise): attempts sent (pages, probes,
	// retries, requeues), attempts repeated, attempts in flight now, and
	// the adaptive limiter's current limit (0 without one). CostUSD is
	// Requests at the source's reference tariff (absent without one).
	Requests int64    `json:"http_attempts"`
	Retries  int64    `json:"retries"`
	Inflight int64    `json:"inflight"`
	Limit    int64    `json:"limit"`
	CostUSD  *float64 `json:"cost_usd,omitempty"`
	// CostRatePer1kUSD is the reference rate CostUSD is derived from.
	CostRatePer1kUSD float64 `json:"rate_per_1k_usd,omitempty"`
	// MutexWaitNs and MaxMutexHoldNs are the range engine's decision
	// mutex: the goroutines' total wait for it and its longest hold
	// (source.Contended; 0 otherwise).
	MutexWaitNs    int64         `json:"mutex_wait_ns"`
	MaxMutexHoldNs int64         `json:"max_mutex_hold_ns"`
	StartedAt      jsontime.Time `json:"started_at"`  // zero before the run starts
	AsOf           jsontime.Time `json:"as_of"`       //
	DurationMs     int64         `json:"duration_ms"` // since StartedAt
	// Order is the order stage's live resources with their peaks:
	// outstanding nodes, spill markers, open spill readers, live spill
	// bytes (nil before the stage starts).
	Order *order.GaugeSnapshot `json:"order,omitempty"`
	// PartAgeMs is the age of the recovery point: the time since the last
	// part was published (journaled), or since the start before the first.
	// A crash now loses the work of that long.
	PartAgeMs int64 `json:"part_age_ms"`
}

// Snapshot reads p; a nil Progress reads as zero.
func (p *Progress) Snapshot() ProgressSnapshot {
	now := time.Now()
	s := ProgressSnapshot{ProgressVersion: ProgressVersion, State: StateListing, AsOf: jsontime.Of(now)}
	if p == nil {
		return s
	}
	if ids := p.ids.Load(); ids != nil {
		s.AttemptID, s.RunID = ids[0], ids[1]
	}
	if st := p.state.Load(); st != nil {
		s.State = *st
	}
	if f := p.listed.Load(); f != nil {
		s.Rows = (*f)()
	}
	if f := p.answered.Load(); f != nil {
		s.Answered = (*f)()
	}
	if f := p.frontier.Load(); f != nil {
		s.Frontier, s.EmptyChain = (*f)()
		s.advancing = true
	}
	if f := p.faults.Load(); f != nil {
		s.Throttles, s.Timeouts = (*f)()
	}
	if f := p.contention.Load(); f != nil {
		w, h := (*f)()
		s.MutexWaitNs, s.MaxMutexHoldNs = int64(w), int64(h)
	}
	if f := p.requests.Load(); f != nil {
		r := (*f)()
		s.Requests, s.Retries, s.Inflight, s.Limit = r.Attempts, r.Retries, r.Inflight, r.Limit
		if rate := math.Float64frombits(p.tariff.Load()); rate > 0 {
			usd := float64(r.Attempts) / 1000 * rate
			s.CostUSD, s.CostRatePer1kUSD = &usd, rate
		}
	}
	s.Retained = p.retained.Load()
	s.Ordered, s.Encoded, s.Parts = p.ordered.Load(), p.encoded.Load(), p.parts.Load()
	s.Verified = p.verified.Load()
	if g := p.gauges.Load(); g != nil {
		o := g.Snapshot()
		s.Order, s.Scanned = &o, g.CertScanned()
	}
	s.Advance = s.Rows + s.Ordered + s.Encoded + s.Parts + s.Verified + s.Scanned
	if st := p.started.Load(); st != 0 {
		s.StartedAt = jsontime.Of(time.Unix(0, st))
		s.DurationMs = now.Sub(s.StartedAt.Time).Milliseconds()
		s.PartAgeMs = s.DurationMs
	}
	if lp := p.lastPart.Load(); lp != 0 {
		s.PartAgeMs = now.Sub(time.Unix(0, lp)).Milliseconds()
	}
	return s
}

// Liveness is what the watchdog watches rise: Advance, plus the source's
// key-space progress (Frontier) when it counts it, else the requests its
// service answered (Answered). An answered empty continuing page past the
// first 1,024 of its run is in Answered but not in Frontier: it shows the
// service is up, not that the listing moves.
func (s ProgressSnapshot) Liveness() int64 {
	if s.advancing {
		return s.Advance + s.Frontier
	}
	return s.Advance + s.Answered
}
