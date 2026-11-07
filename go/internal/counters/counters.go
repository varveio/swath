// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package counters is the run's tally: a table of counter definitions
// (stable name, kind, unit, meaning) backed by one atomic per entry, read
// out as one immutable Snapshot. The run report is emitted by iterating
// the table, and the documentation table is generated from it, so a new
// engagement counter is one table row plus its increment.
package counters

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
)

// Kind says how a counter is updated and read.
type Kind uint8

const (
	Count Kind = iota // monotonic sum
	Max               // high-water mark
	Flag              // 0 or 1, reported as a boolean
	Gauge             // a value derived when it is copied in (a percentile): neither summed nor a high-water mark
)

func (k Kind) String() string { return [...]string{"count", "max", "flag", "gauge"}[k] }

// ID is a counter's index in its table.
type ID int

// Def defines one counter. Key is its stable name (dotted, flat in the
// report). Feature, if set, names the policy the counter measures: when the
// run turns that policy off, the counter is left out of the snapshot's
// report and the feature reads off. So a report distinguishes a counter this
// build does not have (absent, no feature), a policy that was off (the
// feature is false), and a policy that was on but never fired (present,
// zero).
type Def struct {
	Key     string
	Kind    Kind
	Unit    string
	Help    string
	Feature string
}

// Set holds the values of one table.
type Set struct {
	defs []Def
	v    []atomic.Int64
}

// New returns a zeroed set for defs; the index of a Def is its ID.
func New(defs []Def) *Set { return &Set{defs: defs, v: make([]atomic.Int64, len(defs))} }

// Add adds n to counter id.
func (s *Set) Add(id ID, n int64) { s.v[id].Add(n) }

// Inc adds one to counter id.
func (s *Set) Inc(id ID) { s.v[id].Add(1) }

// Set stores n (flags, or values copied from elsewhere).
func (s *Set) Set(id ID, n int64) { s.v[id].Store(n) }

// Max raises counter id to n.
func (s *Set) Max(id ID, n int64) {
	for {
		cur := s.v[id].Load()
		if n <= cur || s.v[id].CompareAndSwap(cur, n) {
			return
		}
	}
}

// Load reads counter id.
func (s *Set) Load(id ID) int64 { return s.v[id].Load() }

// Snapshot is the set's values at one moment, with the run's feature
// states; it does not change afterwards.
type Snapshot struct {
	defs     []Def
	vals     []int64
	features map[string]bool
}

// Snapshot reads every counter. off names the features this run turned
// off; every other feature named in the table is on.
func (s *Set) Snapshot(off ...string) Snapshot {
	sn := Snapshot{defs: s.defs, vals: make([]int64, len(s.v)), features: map[string]bool{}}
	for i := range s.v {
		sn.vals[i] = s.v[i].Load()
		if f := s.defs[i].Feature; f != "" {
			sn.features[f] = !slices.Contains(off, f)
		}
	}
	return sn
}

// Get is counter id's value.
func (s Snapshot) Get(id ID) int64 {
	if int(id) >= len(s.vals) {
		return 0
	}
	return s.vals[id]
}

// Value is the value of the counter named key (0 if there is none).
func (s Snapshot) Value(key string) int64 {
	for i, d := range s.defs {
		if d.Key == key {
			return s.vals[i]
		}
	}
	return 0
}

// On reports whether feature was on.
func (s Snapshot) On(feature string) bool { return s.features[feature] }

// MarshalJSON writes {"features": {name: on}, "values": {key: value}}:
// flat stable names, flags as booleans, and no counter of a feature that
// was off.
func (s Snapshot) MarshalJSON() ([]byte, error) {
	var b []byte
	b = append(b, `{"features":`...)
	f, err := json.Marshal(s.features)
	if err != nil {
		return nil, err
	}
	b = append(append(b, f...), `,"values":{`...)
	first := true
	for i, d := range s.defs {
		if d.Feature != "" && !s.features[d.Feature] {
			continue
		}
		var v any = s.vals[i]
		if d.Kind == Flag {
			v = s.vals[i] != 0
		}
		key, _ := json.Marshal(d.Key)
		val, _ := json.Marshal(v)
		if !first {
			b = append(b, ',')
		}
		first = false
		b = append(append(append(b, key...), ':'), val...)
	}
	return append(b, "}}"...), nil
}

// Markdown renders defs as a documentation table.
func Markdown(defs []Def) string {
	var b strings.Builder
	b.WriteString("| Key | Kind | Unit | Feature | Meaning |\n|---|---|---|---|---|\n")
	for _, d := range defs {
		fmt.Fprintf(&b, "| `%s` | %s | %s | %s | %s |\n", d.Key, d.Kind, d.Unit, d.Feature, d.Help)
	}
	return b.String()
}
