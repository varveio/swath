// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"sync"
	"time"

	"github.com/varveio/swath/go/internal/order"
)

// HistoryCap bounds the report's progress history: 300 samples (about 30
// KB of JSON) keep a run of any length readable as a curve. HistoryEvery is
// the sampling tick: the free-disk minimum is taken at every tick, and the
// history keeps every tick until it fills, then every second, fourth, ...
// one (History.IntervalMs doubles each time it fills).
const (
	HistoryCap   = 300
	HistoryEvery = time.Second
)

// History is a run's progress over time: the first sample at the start of
// listing, then one every IntervalMs, and the last at the end.
type History struct {
	IntervalMs int64    `json:"interval_ms"`
	Samples    []Sample `json:"samples"`
}

// Sample is the run's progress at one moment: rows fetched (before the
// filters), emitted (handed on in order) and durable (in journaled or
// committed parts, retained ones included); HTTP attempts sent, in flight
// now, and the limiter's limit (0 without one); live spill bytes; the free
// space on the filesystem the run writes to (absent: none); and the
// resident set (absent where it is not measured).
type Sample struct {
	At            time.Time `json:"at"`
	RowsFetched   int64     `json:"rows_fetched"`
	RowsEmitted   int64     `json:"rows_emitted"`
	RowsDurable   int64     `json:"rows_durable"`
	HTTPAttempts  int64     `json:"http_attempts"`
	Inflight      int64     `json:"inflight"`
	Limit         int64     `json:"limit"`
	SpillBytes    int64     `json:"spill_bytes"`
	FreeDiskBytes *int64    `json:"free_disk_bytes,omitempty"`
	RSSBytes      *int64    `json:"rss_bytes,omitempty"`
}

// sampler records a run's History until finish.
type sampler struct {
	read func() Sample
	disk string // the directory whose filesystem's free space is watched ("" for none)
	stop chan struct{}
	done chan struct{}
	once sync.Once

	hist    History
	freeMin *int64
}

// startHistory samples read (and the free space under disk) every
// HistoryEvery until finish.
func startHistory(read func() Sample, disk string) *sampler {
	s := &sampler{read: read, disk: disk, stop: make(chan struct{}), done: make(chan struct{})}
	s.hist.IntervalMs = HistoryEvery.Milliseconds()
	s.hist.Samples = append(s.hist.Samples, s.sample())
	go s.loop()
	return s
}

func (s *sampler) loop() {
	defer close(s.done)
	t := time.NewTicker(HistoryEvery)
	defer t.Stop()
	stride := int64(1) // ticks per kept sample
	for tick := int64(1); ; tick++ {
		select {
		case <-s.stop:
			s.keep(s.sample())
			return
		case <-t.C:
			v := s.sample()
			if tick%stride != 0 {
				continue
			}
			if s.keep(v) {
				stride *= 2
				s.hist.IntervalMs *= 2
			}
		}
	}
}

// keep appends v; when that fills the history, it keeps every other sample
// (the first and, after the last tick, the final one stay) and reports so.
func (s *sampler) keep(v Sample) (halved bool) {
	h := &s.hist
	h.Samples = append(h.Samples, v)
	if len(h.Samples) < HistoryCap {
		return false
	}
	last := h.Samples[len(h.Samples)-1]
	kept := h.Samples[:0]
	for i := 0; i < len(h.Samples); i += 2 {
		kept = append(kept, h.Samples[i])
	}
	if kept[len(kept)-1] != last {
		kept = append(kept, last)
	}
	h.Samples = kept
	return true
}

// sample reads the run now, and the free space under disk.
func (s *sampler) sample() Sample {
	v := s.read()
	v.At = time.Now().UTC()
	if s.disk != "" {
		if f := order.FreeBytes(existing(s.disk)); f != order.Unlimited {
			v.FreeDiskBytes = &f
			if s.freeMin == nil || f < *s.freeMin {
				s.freeMin = &f
			}
		}
	}
	if rss, ok := residentBytes(); ok {
		v.RSSBytes = &rss
	}
	return v
}

// finish stops the sampling, once, after a last sample, and returns the
// history and the least free space seen.
func (s *sampler) finish() (*History, *int64) {
	s.once.Do(func() {
		close(s.stop)
		<-s.done
	})
	h := s.hist
	h.Samples = append([]Sample(nil), h.Samples...)
	return &h, s.freeMin
}
