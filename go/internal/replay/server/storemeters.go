// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"time"

	"github.com/varveio/swath/go/internal/replay/metrics"
	"github.com/varveio/swath/go/internal/replay/store"
	"github.com/varveio/swath/go/sortedparquet"
)

// meteredStore is a store that reports its reads and its descriptor cache
// (store.Sorted). Another store (the in-memory key list) leaves the store
// meters at zero.
type meteredStore interface {
	SetMeters(store.Meters)
	Counters() *store.Counters
	Files() *sortedparquet.FileCache
}

// initStoreMeters creates the fixture and store meters and connects the
// store's measurements to them. They exist from startup, so a scrape tells
// a store that has not read from one that is not metered.
func (s *Server) initStoreMeters() {
	r := s.reg
	index := r.Get(metrics.IndexLoadLatency, "derived")
	entries := r.Get(metrics.IndexEntries)
	if s.cfg.IndexLoad > 0 {
		index.Record(s.cfg.IndexLoad)
		entries.Observe(float64(s.cfg.IndexEntries))
	}
	r.Get(metrics.SortFixtureBuildLatency)
	r.Get(metrics.SortFixtureOutputBytes)
	pageRead := r.Get(metrics.PageReadLatency)
	query := r.Get(metrics.ParquetQueryLatency)
	queryRows := r.Get(metrics.ParquetQueryRows)
	queryErrors := r.Get(metrics.ParquetQueryErrors)
	opens := r.Get(metrics.DelimiterSkipScanRowGroupOpens)
	shortcuts := r.Get(metrics.DelimiterSkipScanWholeGroupShortcuts)
	decoded := r.Get(metrics.DelimiterSkipScanDecodedKeyRows)
	reseeks := r.Get(metrics.DelimiterSkipScanPageReseeks)

	st, ok := s.cfg.Store.(meteredStore)
	if !ok {
		for _, d := range []metrics.Def{metrics.ParquetQueriesInFlight, metrics.ParquetQueriesPeak} {
			r.SetGauge(d, func() float64 { return 0 })
		}
		return
	}
	st.SetMeters(store.Meters{
		SlotHeld: pageRead.Record,
		Query: func(d time.Duration, rows int, failed bool) {
			query.Record(d)
			queryRows.Observe(float64(rows))
			if failed {
				queryErrors.Inc()
			}
		},
		Rollup: func(stats sortedparquet.Stats) {
			opens.Add(stats.GroupsOpened)
			shortcuts.Add(stats.GroupShortcuts)
			decoded.Observe(float64(stats.KeysDecoded))
			reseeks.Observe(float64(stats.Reseeks))
		},
		Refused: func(reason string) { r.Get(metrics.ServingRefused, reason).Inc() },
	})
	c := st.Counters()
	r.SetGauge(metrics.ParquetQueriesInFlight, func() float64 { return float64(c.InFlightReaders.Load()) })
	r.SetGauge(metrics.ParquetQueriesPeak, func() float64 { return float64(c.PeakReaders.Load()) })
	files := st.Files()
	r.SetCounter(metrics.HandleCacheOpens, func() int64 { return int64(files.Stats().Opens) })
	r.SetCounter(metrics.HandleCacheEvictions, func() int64 { return int64(files.Stats().Evictions) })
	r.SetCounter(metrics.HandleCacheWaits, func() int64 { return int64(files.Stats().Waits) })
	r.SetGauge(metrics.HandleCacheOpen, func() float64 { return float64(files.Stats().Open) })
	r.SetGauge(metrics.HandleCachePeak, func() float64 { return float64(files.Stats().Peak) })
	r.SetGauge(metrics.HandleCacheLimit, func() float64 { return float64(files.Max()) })
}
