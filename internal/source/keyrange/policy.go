// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import "github.com/varveio/swath/internal/source/objhttp"

// Policy is the engine's policy configuration, the same for every provider
// on the engine: one flag group (the command's cli.EngineGroup), one value
// a provider passes on.
type Policy struct {
	Sequential bool      // one continuation-token chain, no ranges (the oracle)
	Seed       bool      // parallel: tile the scope from delimiter listings first (the command's default)
	Structure  Structure // pivot on folder boundaries from structure probes sent alongside pages
	MaxKeys    int64     // stop after this many objects (sequential only; 0 = all)
	SlowSplit  bool      // split latency outliers without an idle worker (off by default)
	PageSize   int       // the page size asked of the service (0: PageKeys); diagnostic
	TilePages  int       // the pages of a counted prior's tile (0: TilePages); diagnostic
	WindowRows int64     // the frontier window's rows in a planned run (0: the default rule); diagnostic
}

// Common is what every provider on the engine shares, the value of the
// command's engine flag group (cli.EngineGroup): the engine's policy and
// its HTTP client's admission (objhttp.Admission), kept apart.
type Common struct {
	Policy    Policy
	Admission objhttp.Admission
	// AllVersions lists every version of every key (--all-versions): S3
	// versions and delete markers, GCS generations, Azure Blob versions,
	// snapshots and soft-deleted blobs, in the providers' versions schemas.
	AllVersions bool
	// Trace is the run trace's file (swath diag --trace): the command
	// checks, opens and closes it (cli.TraceFlag) and hands the trace to
	// the engine (TraceTo); a provider does not read it.
	Trace string
}

// Options are the engine options this policy sets.
func (p Policy) Options() Options {
	return Options{Sequential: p.Sequential, Seed: p.Seed, SeedFirstPage: p.Seed, Structure: p.Structure,
		MaxKeys: p.MaxKeys, SlowSplit: p.SlowSplit, PageSize: p.PageSize, TilePages: p.TilePages, WindowRows: p.WindowRows}
}

// MaxTilePages is the most pages Policy.TilePages may ask of a counted
// prior's tile (--seed-from-tile-pages).
const MaxTilePages = maxTilePages
