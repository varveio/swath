// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/varveio/swath/go/internal/order"
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
	faults   atomic.Pointer[func() (int64, int64)]
	ordered  atomic.Int64
	encoded  atomic.Int64
	parts    atomic.Int64
	lastPart atomic.Int64 // unix nanoseconds of the last part published
	verified atomic.Int64 // bytes of retained parts read by a resume's verification
	gauges   atomic.Pointer[order.Gauges]

	mu    sync.Mutex // held across an owned write and across a change of ownership
	owner bool       // the run holds the output's lock
}

// Owned calls write while the run owns its output directory and reports
// whether it did. Ownership cannot end during write, so a process whose run
// was refused, or has released the output, never writes into it.
func (p *Progress) Owned(write func()) bool {
	if p == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.owner {
		write()
	}
	return p.owner
}

func (p *Progress) own(v bool) {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.owner = v
	p.mu.Unlock()
}

// ProgressSnapshot is Progress at one moment.
type ProgressSnapshot struct {
	Objects int64 `json:"objects"` // rows listed so far
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
	// Throttles and Timeouts count the service's throttle answers and the
	// attempts past the request timeout (source.Faulted; 0 otherwise).
	Throttles  int64     `json:"throttles"`
	Timeouts   int64     `json:"timeouts"`
	StartedAt  time.Time `json:"started_at"`  // zero before the run starts
	AsOf       time.Time `json:"as_of"`       //
	DurationMs int64     `json:"duration_ms"` // since StartedAt
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
		s.Objects = (*f)()
	}
	if f := p.answered.Load(); f != nil {
		s.Answered = (*f)()
	}
	if f := p.faults.Load(); f != nil {
		s.Throttles, s.Timeouts = (*f)()
	}
	s.Ordered, s.Encoded, s.Parts = p.ordered.Load(), p.encoded.Load(), p.parts.Load()
	s.Verified = p.verified.Load()
	s.Advance = s.Objects + s.Ordered + s.Encoded + s.Parts + s.Verified
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
