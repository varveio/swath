// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"math"
	"runtime/debug"
)

// The default row budget is a share of the least memory limit detected
// (physical memory, the cgroup's limit, GOMEMLIMIT; not free memory),
// between MinRowBudget and maxAutoRowBudget; DefaultRowBudget when none
// is detected.
const (
	// rowBudgetShare is the share of memory the default row budget takes
	// (1/16): the budget is one of several allowances beside it (the
	// encoders' allowance equal to it, the tail's segment reservations,
	// pages in flight, codec state, GC headroom: running.md's Memory).
	rowBudgetShare = 16
	// maxAutoRowBudget caps the default row budget at 1 GiB: past it a
	// listing larger than memory spills anyway, and more encoders and tail
	// workers have bought little (measurements.md: 31 vs 8 tail workers).
	maxAutoRowBudget = 1 << 30
)

// Why the row budget is what it is (RowBudget.Reason).
const (
	BudgetExplicit = "explicit"          // --sort-buffer (Limits.RowBudget) was given
	BudgetShare    = "capacity_fraction" // 1/16 of memory
	BudgetCap      = "cap"               // 1/16 of memory was past maxAutoRowBudget
	BudgetMinimum  = "minimum"           // 1/16 of memory was below MinRowBudget
	BudgetUnknown  = "unknown_fallback"  // no memory limit was found: DefaultRowBudget
)

// What bounded the memory the default row budget is a share of
// (RowBudget.BoundBy).
const (
	MemoryPhysical = "physical"        // the host's physical memory
	MemoryCgroup   = "cgroup"          // the process's cgroup limit (Linux)
	MemoryGoLimit  = "go_memory_limit" // GOMEMLIMIT (runtime/debug.SetMemoryLimit)
)

// RowBudget is how invocation.sort_buffer_bytes was chosen, and the memory
// limits found when the run started: physical and cgroup 0 (absent) when
// none was found; the Go limit absent when none is set, and 0 a limit.
// MemoryBytes is absent when it is 0: BoundBy then says whether that is a
// GOMEMLIMIT of 0 or no limit at all.
type RowBudget struct {
	Reason string `json:"reason"`
	// MemoryBytes is the least of the limits below, the memory the default
	// is a share of; BoundBy names it.
	MemoryBytes   int64  `json:"memory_bytes,omitempty"`
	BoundBy       string `json:"bound_by,omitempty"`
	PhysicalBytes int64  `json:"physical_bytes,omitempty"`
	CgroupBytes   int64  `json:"cgroup_bytes,omitempty"`
	// GoMemoryLimitBytes is the Go runtime's memory limit when one is set
	// (GOMEMLIMIT; 0 is a limit, the tightest): absent when there is none.
	GoMemoryLimitBytes *int64 `json:"go_memory_limit_bytes,omitempty"`
}

// memoryLimits are the memory limits the host sets the process: physical
// and cgroup 0 when none is found; goLimit when goLimitSet (0 is a limit).
type memoryLimits struct {
	physical, cgroup, goLimit int64
	goLimitSet                bool
}

// readMemoryLimits finds them; a test replaces it.
var readMemoryLimits = func() memoryLimits {
	m := memoryLimits{physical: physicalMemory(), cgroup: cgroupMemory()}
	if l := debug.SetMemoryLimit(-1); l < math.MaxInt64 { // -1 reads it without a change; MaxInt64 is none
		m.goLimit, m.goLimitSet = l, true
	}
	return m
}

// rowBudgetFor is the row budget for an explicit one (0: none given) and
// the limits found, with how it was chosen.
func rowBudgetFor(explicit int64, m memoryLimits) (int64, RowBudget) {
	why := RowBudget{PhysicalBytes: m.physical, CgroupBytes: m.cgroup}
	if m.goLimitSet {
		why.GoMemoryLimitBytes = &m.goLimit
	}
	for _, l := range []struct {
		n    int64
		set  bool
		name string
	}{{m.physical, m.physical > 0, MemoryPhysical}, {m.cgroup, m.cgroup > 0, MemoryCgroup}, {max(0, m.goLimit), m.goLimitSet, MemoryGoLimit}} {
		if l.set && (why.BoundBy == "" || l.n < why.MemoryBytes) {
			why.MemoryBytes, why.BoundBy = l.n, l.name
		}
	}
	b := why.MemoryBytes / rowBudgetShare
	switch {
	case explicit > 0:
		why.Reason, b = BudgetExplicit, explicit
	case why.BoundBy == "":
		why.Reason, b = BudgetUnknown, DefaultRowBudget
	case b > maxAutoRowBudget:
		why.Reason, b = BudgetCap, maxAutoRowBudget
	case b < MinRowBudget:
		why.Reason, b = BudgetMinimum, MinRowBudget
	default:
		why.Reason = BudgetShare
	}
	return b, why
}

// clampMemory is n bytes as an int64, saturating.
func clampMemory(n uint64) int64 { return int64(min(n, math.MaxInt64)) }
