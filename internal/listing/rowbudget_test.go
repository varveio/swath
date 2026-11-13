// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"math"
	"testing"
)

const (
	mib = int64(1) << 20
	gib = int64(1) << 30
)

func TestRowBudgetFor(t *testing.T) {
	for _, c := range []struct {
		name     string
		explicit int64
		m        memoryLimits
		budget   int64
		reason   string
		memory   int64
		boundBy  string
	}{
		{"unknown memory", 0, memoryLimits{}, DefaultRowBudget, BudgetUnknown, 0, ""},
		{"2 GiB host", 0, memoryLimits{physical: 2 * gib}, 128 * mib, BudgetShare, 2 * gib, MemoryPhysical},
		{"8 GiB host", 0, memoryLimits{physical: 8 * gib}, 512 * mib, BudgetShare, 8 * gib, MemoryPhysical},
		{"16 GiB host is the cap", 0, memoryLimits{physical: 16 * gib}, gib, BudgetShare, 16 * gib, MemoryPhysical},
		{"64 GiB host capped", 0, memoryLimits{physical: 64 * gib}, gib, BudgetCap, 64 * gib, MemoryPhysical},
		{"cgroup below physical", 0, memoryLimits{physical: 64 * gib, cgroup: gib}, 64 * mib, BudgetShare, gib, MemoryCgroup},
		{"cgroup above physical", 0, memoryLimits{physical: 2 * gib, cgroup: 8 * gib}, 128 * mib, BudgetShare, 2 * gib, MemoryPhysical},
		{"GOMEMLIMIT least", 0, memoryLimits{physical: 64 * gib, cgroup: 8 * gib, goLimit: 512 * mib, goLimitSet: true}, 32 * mib, BudgetShare, 512 * mib, MemoryGoLimit},
		{"GOMEMLIMIT above physical", 0, memoryLimits{physical: 2 * gib, goLimit: 64 * gib, goLimitSet: true}, 128 * mib, BudgetShare, 2 * gib, MemoryPhysical},
		{"GOMEMLIMIT=0 is a limit, the tightest", 0, memoryLimits{physical: 32 * gib, goLimit: 0, goLimitSet: true}, MinRowBudget, BudgetMinimum, 0, MemoryGoLimit},
		{"GOMEMLIMIT alone", 0, memoryLimits{goLimit: 4 * gib, goLimitSet: true}, 256 * mib, BudgetShare, 4 * gib, MemoryGoLimit},
		{"cgroup alone (physical unread)", 0, memoryLimits{cgroup: 4 * gib}, 256 * mib, BudgetShare, 4 * gib, MemoryCgroup},
		{"tiny cgroup takes the minimum", 0, memoryLimits{physical: 64 * gib, cgroup: 1 * mib}, MinRowBudget, BudgetMinimum, mib, MemoryCgroup},
		{"explicit wins, limits still recorded", 300 * mib, memoryLimits{physical: 64 * gib}, 300 * mib, BudgetExplicit, 64 * gib, MemoryPhysical},
		{"explicit with memory unknown", 300 * mib, memoryLimits{}, 300 * mib, BudgetExplicit, 0, ""},
		{"saturated physical", 0, memoryLimits{physical: math.MaxInt64}, gib, BudgetCap, math.MaxInt64, MemoryPhysical},
	} {
		t.Run(c.name, func(t *testing.T) {
			b, why := rowBudgetFor(c.explicit, c.m)
			if b != c.budget || why.Reason != c.reason || why.MemoryBytes != c.memory || why.BoundBy != c.boundBy {
				t.Fatalf("rowBudgetFor(%d, %+v) = %d, %+v; want %d, reason %s, memory %d bound by %q", c.explicit, c.m, b, why, c.budget, c.reason, c.memory, c.boundBy)
			}
			if why.PhysicalBytes != c.m.physical || why.CgroupBytes != c.m.cgroup {
				t.Fatalf("limits recorded %+v, found %+v", why, c.m)
			}
			if g := why.GoMemoryLimitBytes; (g != nil) != c.m.goLimitSet || g != nil && *g != c.m.goLimit {
				t.Fatalf("Go limit recorded %v, found %+v", g, c.m)
			}
		})
	}
}

func TestClampMemory(t *testing.T) {
	if got := clampMemory(math.MaxUint64); got != math.MaxInt64 {
		t.Fatalf("clampMemory(MaxUint64) = %d", got)
	}
	if got := clampMemory(5); got != 5 {
		t.Fatalf("clampMemory(5) = %d", got)
	}
}

// TestResolveRowBudget: resolve takes the default from the memory found,
// keeps an explicit budget, and records how either was chosen.
func TestResolveRowBudget(t *testing.T) {
	old := readMemoryLimits
	t.Cleanup(func() { readMemoryLimits = old })
	readMemoryLimits = func() memoryLimits { return memoryLimits{physical: 8 * gib, cgroup: 4 * gib} }

	c := Config{Source: SourceConfig{URI: "/tmp"}}
	if err := c.resolve(); err != nil {
		t.Fatal(err)
	}
	if l := c.Limits; l.RowBudget != 256*mib || l.rowBudgetWhy.Reason != BudgetShare || l.rowBudgetWhy.BoundBy != MemoryCgroup {
		t.Fatalf("default: %d %+v", l.RowBudget, l.rowBudgetWhy)
	}
	c = Config{Source: SourceConfig{URI: "/tmp"}, Limits: Limits{RowBudget: 2 * mib}}
	if err := c.resolve(); err != nil {
		t.Fatal(err)
	}
	if l := c.Limits; l.RowBudget != 2*mib || l.rowBudgetWhy.Reason != BudgetExplicit {
		t.Fatalf("explicit: %d %+v", l.RowBudget, l.rowBudgetWhy)
	}
	c = Config{Source: SourceConfig{URI: "/tmp"}, Limits: Limits{RowBudget: MinRowBudget - 1}}
	if err := c.resolve(); err == nil {
		t.Fatal("an explicit budget below the minimum was accepted")
	}
}

// TestReadMemoryLimitsHost: on the platforms that read it, the host's
// physical memory is found and the default budget is a share of it.
func TestReadMemoryLimitsHost(t *testing.T) {
	m := readMemoryLimits()
	if physicalMemory() == 0 {
		t.Skip("physical memory is not read on this platform")
	}
	b, why := rowBudgetFor(0, m)
	if why.Reason == BudgetUnknown || b < MinRowBudget || b > maxAutoRowBudget {
		t.Fatalf("host %+v: budget %d %+v", m, b, why)
	}
}
