// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/varveio/swath/go/internal/journal"
	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/source"
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

// ProgressSnapshot is Progress at one moment.
type ProgressSnapshot struct {
	Rows    int64 `json:"rows"`    // rows listed so far (directories and versions included)
	Ordered int64 `json:"ordered"` // rows handed to the encoders
	Encoded int64 `json:"encoded"` // rows written into parts
	Parts   int64 `json:"parts"`   // parts published
	// Verified is the bytes of retained parts a resume has read to check
	// them against the journal (size and MD5), before it lists.
	Verified int64 `json:"verified_bytes"`
	Advance  int64 `json:"advance"` // the sum: it rises in every phase (verification included) while the run progresses
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
	// Requests, Retries, Inflight and Limit are the source's HTTP account
	// (source.Requested; 0 otherwise): attempts sent (pages, probes,
	// retries, requeues), attempts repeated, attempts in flight now, and
	// the adaptive limiter's current limit (0 without one). CostUSD is
	// Requests at the source's reference tariff (absent without one).
	Requests int64    `json:"requests"`
	Retries  int64    `json:"retries"`
	Inflight int64    `json:"inflight"`
	Limit    int64    `json:"limit"`
	CostUSD  *float64 `json:"cost_usd,omitempty"`
	// CostRatePer1kUSD is the reference rate CostUSD is derived from.
	CostRatePer1kUSD float64 `json:"cost_rate_per_1k_usd,omitempty"`
	// MutexWaitMs and MutexMaxHoldMs are the range engine's decision
	// mutex: the goroutines' total wait for it and its longest hold
	// (source.Contended; 0 otherwise).
	MutexWaitMs    float64   `json:"mutex_wait_ms"`
	MutexMaxHoldMs float64   `json:"mutex_max_hold_ms"`
	StartedAt      time.Time `json:"started_at"`  // zero before the run starts
	AsOf           time.Time `json:"as_of"`       //
	DurationMs     int64     `json:"duration_ms"` // since StartedAt
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
	s := ProgressSnapshot{AsOf: now.UTC()}
	if p == nil {
		return s
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
		s.MutexWaitMs, s.MutexMaxHoldMs = float64(w)/1e6, float64(h)/1e6
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
	s.Advance = s.Rows + s.Ordered + s.Encoded + s.Parts + s.Verified
	if g := p.gauges.Load(); g != nil {
		o := g.Snapshot()
		s.Order = &o
	}
	if st := p.started.Load(); st != 0 {
		s.StartedAt = time.Unix(0, st).UTC()
		s.DurationMs = now.Sub(s.StartedAt).Milliseconds()
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
