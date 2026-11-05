// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"runtime/debug"
	"time"

	"github.com/varveio/swath/go/internal/source/s3"
)

// costPer1k is the reference LIST price swath reports cost with.
const costPer1k = 0.005

// RunSummary is swath's run summary (schema_version 2), as Java swath
// writes _swath_summary.json, filled with what this tool measures. Readers
// such as Varve check schema_version, completed, config.target and
// config.filters for whole-bucket coverage; the rest is for people and
// tabulation. Fields this tool has no counterpart for are left out rather
// than written as zeros, except steals, which is 0 by construction (splits
// are owner-side). Go is this tool's own report.
type RunSummary struct {
	SchemaVersion     int              `json:"schema_version"`
	Producer          string           `json:"producer"`
	Argv              []string         `json:"argv"`
	Strategy          string           `json:"strategy"`
	Completed         bool             `json:"completed"`
	ExitCode          int              `json:"exit_code"`
	StopReason        string           `json:"stop_reason"`
	StartedAt         time.Time        `json:"started_at"`
	AsOf              time.Time        `json:"as_of"`
	DurationMs        int64            `json:"duration_ms"`
	ListingDurationMs int64            `json:"listing_duration_ms"`
	Objects           int64            `json:"objects"`
	RecoveredObjects  int64            `json:"recovered_objects"`
	Config            RunConfig        `json:"config"`
	EngineFlags       map[string]bool  `json:"engine_flags"`
	Output            RunOutput        `json:"output"`
	Cost              RunCost          `json:"cost"`
	Efficiency        RunEfficiency    `json:"efficiency"`
	Engine            RunEngine        `json:"engine"`
	RecoveredErrors   map[string]int64 `json:"recovered_errors"`
	Seed              RunSeed          `json:"seed"`
	Sort              RunSort          `json:"sort"`
	Shape             RunShape         `json:"shape"`
	Go                *Summary         `json:"go"`
}

// RunConfig is the run's target and settings.
type RunConfig struct {
	Target              string   `json:"target"`
	Region              string   `json:"region,omitempty"`
	Format              string   `json:"format"`
	MaxParallelListings int      `json:"max_parallel_listings"`
	NoSignRequest       bool     `json:"no_sign_request"`
	RequestPayer        bool     `json:"request_payer"`
	Filters             []string `json:"filters"`
}

// RunOutput describes what was written.
type RunOutput struct {
	Format              string `json:"format"`
	Files               int    `json:"files"`
	CompressedSizeBytes int64  `json:"compressed_size_bytes"`
}

// RunCost is the LIST cost at the reference rate.
type RunCost struct {
	APICalls int64   `json:"api_calls"`
	CostUSD  float64 `json:"cost_usd"`
	Basis    struct {
		RatePer1kUSD float64 `json:"rate_per_1k_usd"`
		Source       string  `json:"source"`
	} `json:"basis"`
}

// RunEfficiency is throughput and resource use.
type RunEfficiency struct {
	KeysPerSec           float64 `json:"keys_per_sec"`
	APICallsPer1kObjects float64 `json:"api_calls_per_1k_objects"`
	PeakRSSBytes         int64   `json:"peak_rss_bytes"`
	CPUSeconds           float64 `json:"cpu_seconds"`
}

// RunEngine is the engine's activity.
type RunEngine struct {
	Pages        int64   `json:"pages"`
	PeakInFlight int     `json:"peak_in_flight"`
	AvgInFlight  float64 `json:"avg_in_flight"`
	Steals       int64   `json:"steals"`
	Splits       int64   `json:"splits"`
	Errors       int64   `json:"errors"`
}

// RunSeed describes the seed.
type RunSeed struct {
	Mode      string `json:"mode"` // "shallow" (one delimiter page) or "none"
	CutPoints int64  `json:"cut_points"`
	Ranges    int64  `json:"ranges"`
}

// RunSort says the dataset is globally sorted and how.
type RunSort struct {
	Enabled bool   `json:"enabled"`
	Arm     string `json:"arm"`
}

// RunShape carries the binary's fingerprint.
type RunShape struct {
	Fingerprint struct {
		GitSHA     string    `json:"git_sha"`
		StartedAt  time.Time `json:"started_at"`
		FinishedAt time.Time `json:"finished_at"`
	} `json:"fingerprint"`
}

// swathSummary builds the S3 run summary from this tool's summary.
func swathSummary(cfg Config, sum *Summary, target string) RunSummary {
	st := sum.S3
	if st == nil {
		st = &s3.Stats{}
	}
	now := time.Now()
	r := RunSummary{
		SchemaVersion: 2, Producer: "swath-go", Argv: cfg.Argv, Strategy: "ordered_ranges",
		Completed: !st.Truncated, StopReason: "completed",
		StartedAt: sum.Started.UTC(), AsOf: now.UTC(),
		DurationMs: now.Sub(sum.Started).Milliseconds(), ListingDurationMs: int64(sum.ListingSeconds * 1000),
		Objects: st.Objects, Go: sum,
	}
	if st.Truncated {
		r.StopReason = "max_keys"
	}
	if r.Argv == nil {
		r.Argv = []string{}
	}
	r.Config = RunConfig{Target: target, Region: st.Region, Format: "parquet", MaxParallelListings: cfg.Workers,
		NoSignRequest: st.Anonymous, Filters: []string{}}
	r.EngineFlags = map[string]bool{"owner_split": !cfg.S3.Sequential, "density_extrapolation": !cfg.S3.Sequential,
		"seed": cfg.S3.Seed, "sequential": cfg.S3.Sequential}
	r.Output = RunOutput{Format: "parquet", Files: sum.Parts, CompressedSizeBytes: sum.OutBytes}
	r.Cost.APICalls = st.Requests
	r.Cost.CostUSD = float64(st.Requests) * costPer1k / 1000
	r.Cost.Basis.RatePer1kUSD, r.Cost.Basis.Source = costPer1k, "aws-list-reference-rate"
	if sum.ListingSeconds > 0 {
		r.Efficiency.KeysPerSec = float64(st.Objects) / sum.ListingSeconds
	}
	if st.Objects > 0 {
		r.Efficiency.APICallsPer1kObjects = float64(st.Requests) * 1000 / float64(st.Objects)
	}
	r.Efficiency.PeakRSSBytes, r.Efficiency.CPUSeconds = sum.PeakRSS, sum.UserSeconds+sum.SysSeconds
	r.Engine = RunEngine{Pages: st.Pages, PeakInFlight: st.MaxActive, AvgInFlight: st.MeanActive,
		Splits: st.Split.AcceptedBounded + st.Split.AcceptedFrontier}
	r.RecoveredErrors = map[string]int64{"retries": st.Retries, "throttle_events": st.Throttles, "attempt_timeout_events": st.Timeouts, "parse_failures": st.ParseFailures}
	r.Seed.Mode = "none"
	if cfg.S3.Seed {
		r.Seed = RunSeed{Mode: "shallow", CutPoints: st.Seed.Cuts, Ranges: st.Seed.Cuts + 1}
		if st.Seed.Sentinel {
			r.Seed.Ranges++
		}
	}
	r.Sort = RunSort{Enabled: true, Arm: "ordered_emission"}
	r.Shape.Fingerprint.GitSHA = revision()
	r.Shape.Fingerprint.StartedAt, r.Shape.Fingerprint.FinishedAt = r.StartedAt, r.AsOf
	return r
}

// revision is the VCS revision the binary was built from, if recorded.
func revision() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	rev, dirty := "", false
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.modified":
			dirty = s.Value == "true"
		}
	}
	if rev == "" {
		return info.Main.Version
	}
	if dirty {
		rev += "-dirty"
	}
	return rev
}
