// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"sync"
	"sync/atomic"
	"time"
)

// Progress is a run's live progress, read while it runs: by the command's
// watchdog (is the run still making real progress?) and for the progress
// file a supervising process watches. Every phase advances it: listing
// (rows the provider emitted), ordering (rows handed to the router),
// encoding (rows written into parts) and publication (parts published).
type Progress struct {
	started atomic.Int64 // unix nanoseconds
	listed  atomic.Pointer[func() int64]
	ordered atomic.Int64
	encoded atomic.Int64
	parts   atomic.Int64

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
	Objects    int64     `json:"objects"`     // rows listed so far
	Ordered    int64     `json:"ordered"`     // rows handed to the encoders
	Encoded    int64     `json:"encoded"`     // rows written into parts
	Parts      int64     `json:"parts"`       // parts published
	Advance    int64     `json:"advance"`     // the sum: it rises in every phase while the run progresses
	StartedAt  time.Time `json:"started_at"`  // zero before the run starts
	AsOf       time.Time `json:"as_of"`       //
	DurationMs int64     `json:"duration_ms"` // since StartedAt
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
	s.Ordered, s.Encoded, s.Parts = p.ordered.Load(), p.encoded.Load(), p.parts.Load()
	s.Advance = s.Objects + s.Ordered + s.Encoded + s.Parts
	if st := p.started.Load(); st != 0 {
		s.StartedAt = time.Unix(0, st).UTC()
		s.DurationMs = now.Sub(s.StartedAt).Milliseconds()
	}
	return s
}
