// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package latency is the replay server's injected latency: a profile of
// delays by request shape (--inject-latency), deterministic jitter
// (--latency-jitter), compressed time (--latency-scale), and the request
// classification every listing route shares.
//
// The delay is a deadline, not a surcharge: it is the request's total
// target time, so a client observes max(server cost, profile), and a
// request whose own cost exceeds its profile is an overrun the server
// counts. The routes apply that rule; this package computes the target.
package latency

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/varveio/swath/internal/replay/javanum"
)

// Shape is a request's class.
type Shape int

// The shapes, in meter order.
const (
	WorkerPage Shape = iota
	PivotProbe
	StructureProbe
)

// Shapes lists every shape.
var Shapes = [...]Shape{WorkerPage, PivotProbe, StructureProbe}

// String is the shape's tag value: worker_page, pivot_probe,
// structure_probe.
func (s Shape) String() string {
	switch s {
	case PivotProbe:
		return "pivot_probe"
	case StructureProbe:
		return "structure_probe"
	}
	return "worker_page"
}

// Upper is the shape in the slow-request log's spelling: WORKER_PAGE.
func (s Shape) Upper() string { return strings.ToUpper(s.String()) }

// Classify is the provider-neutral classification: a request with a
// delimiter is a structure probe, a delimiter-free request asking for at
// most one result is a pivot probe, anything else a worker page. A route
// passes the page size as the client asked for it (S3 max-keys, before
// clamping; zero counts as a probe).
func Classify(delimiter bool, maxResults int64) Shape {
	if delimiter {
		return StructureProbe
	}
	if maxResults <= 1 {
		return PivotProbe
	}
	return WorkerPage
}

// Delay is one shape's delay: a base, plus, for structure probes, a term
// per common prefix returned (which a network proxy cannot infer).
type Delay struct {
	Base            time.Duration
	PerCommonPrefix time.Duration
}

// ProdCommonCrawl is the built-in reference profile. Its structure-probe
// delay reproduces a 6,865 ms reference at 121 common prefixes as one
// worker page's base plus 55 ms per prefix (223 + 55 × 121 ≈ 6,878 ms):
// the reference does not separate a floor from a slope, so the base is
// pinned to the worker-page value and the slope carries the rest.
var ProdCommonCrawl = [...]Delay{
	WorkerPage:     {Base: 223 * time.Millisecond},
	PivotProbe:     {Base: 121 * time.Millisecond},
	StructureProbe: {Base: 223 * time.Millisecond, PerCommonPrefix: 55 * time.Millisecond},
}

// Profile is a parsed --inject-latency profile. A nil *Profile injects
// nothing.
type Profile struct {
	delays [3]Delay
	jitter float64
}

// New returns a profile with the given delays (an unlisted shape gets
// none) and jitter fraction in [0, 1).
func New(delays map[Shape]Delay, jitter float64) (*Profile, error) {
	if !(jitter >= 0 && jitter < 1) { // the negated form also refuses NaN
		return nil, fmt.Errorf("latency jitter must be in [0, 1), got %v", jitter)
	}
	p := &Profile{jitter: jitter}
	for s, d := range delays {
		p.delays[s] = d
	}
	return p, nil
}

// Delay is the target time of a request of shape s that returned
// commonPrefixes prefixes; key identifies the request for the jitter, so
// the same request always gets the same delay.
func (p *Profile) Delay(s Shape, commonPrefixes int64, key int64) time.Duration {
	if p == nil {
		return 0
	}
	d := p.delays[s]
	base := d.Base
	if d.PerCommonPrefix != 0 && commonPrefixes > 0 {
		base = addSaturating(base, mulSaturating(d.PerCommonPrefix, commonPrefixes))
	}
	if base == 0 || p.jitter == 0 {
		return base
	}
	// Jitter in [-jitter, +jitter] of the base. The key is avalanched
	// first: successive pages of one walk differ only in nearby tokens,
	// and dropping the low bits of a plain hash would give them all the
	// same draw.
	h := uint64(mix64(key))
	signed := float64(h>>11)*0x1p-53*2 - 1
	scaled := javanum.Saturate(float64(base.Nanoseconds()) * (1 + p.jitter*signed))
	return time.Duration(max(0, scaled))
}

// The delay arithmetic saturates at the largest time.Duration (about 292
// years) instead of wrapping: a wrapped sum or product is negative, and
// the route reads a non-positive delay as no injection, so an overflowing
// profile would silently answer at once (TestHugeDelaysSaturate).

func addSaturating(a, b time.Duration) time.Duration {
	if a > math.MaxInt64-b {
		return math.MaxInt64
	}
	return a + b
}

func mulSaturating(d time.Duration, n int64) time.Duration {
	if d != 0 && n > math.MaxInt64/int64(d) {
		return math.MaxInt64
	}
	return d * time.Duration(n)
}

// mix64 is SplitMix64's finalizer.
func mix64(z int64) int64 {
	u := uint64(z)
	u = (u ^ (u >> 30)) * 0xbf58476d1ce4e5b9
	u = (u ^ (u >> 27)) * 0x94d049bb133111eb
	return int64(u ^ (u >> 31))
}

// Parse reads a profile spec: "prod-commoncrawl", or a comma-separated
// list of shape=delay where shape is worker_page (page), pivot_probe
// (pivot) or structure_probe (structure), and delay is 6865ms, 6.9s or a
// bare millisecond count, with an optional +55ms/cp term for
// structure_probe only. scale divides every delay (1 leaves the profile
// exactly as written). A blank spec returns nil (no injection), and then
// a scale other than 1 is an error: there is nothing to compress.
func Parse(spec string, jitter, scale float64) (*Profile, error) {
	if !(scale > 0 && !math.IsInf(scale, 1)) {
		return nil, fmt.Errorf("latency scale must be finite and > 0, got %v", scale)
	}
	if strings.TrimSpace(spec) == "" {
		if scale != 1 {
			return nil, errors.New("a latency scale requires a latency profile to scale")
		}
		if !(jitter >= 0 && jitter < 1) {
			return nil, fmt.Errorf("latency jitter must be in [0, 1), got %v", jitter)
		}
		return nil, nil
	}
	delays := map[Shape]Delay{}
	if strings.ToLower(strings.TrimSpace(spec)) == "prod-commoncrawl" {
		for s, d := range ProdCommonCrawl {
			delays[Shape(s)] = d
		}
	} else {
		for _, pair := range strings.Split(spec, ",") {
			pair = strings.TrimSpace(pair)
			if pair == "" {
				continue
			}
			name, value, ok := strings.Cut(pair, "=")
			if !ok {
				return nil, fmt.Errorf("latency spec entry must be shape=delay, got: %s", pair)
			}
			s, err := parseShape(strings.TrimSpace(name))
			if err != nil {
				return nil, err
			}
			d, err := parseDelay(s, strings.TrimSpace(value))
			if err != nil {
				return nil, err
			}
			delays[s] = d
		}
	}
	if scale != 1 {
		for s, d := range delays {
			delays[s] = Delay{Base: divide(d.Base, scale), PerCommonPrefix: divide(d.PerCommonPrefix, scale)}
		}
	}
	return New(delays, jitter)
}

// divide scales a delay down, truncating toward zero and saturating (a
// tiny scale on a long delay overflows the quotient).
func divide(d time.Duration, scale float64) time.Duration {
	return time.Duration(javanum.Saturate(float64(d.Nanoseconds()) / scale))
}

func parseShape(s string) (Shape, error) {
	switch strings.ToLower(s) {
	case "worker_page", "page":
		return WorkerPage, nil
	case "pivot_probe", "pivot":
		return PivotProbe, nil
	case "structure_probe", "structure":
		return StructureProbe, nil
	}
	return 0, fmt.Errorf("unknown latency shape '%s' (worker_page|pivot_probe|structure_probe)", s)
}

func parseDelay(s Shape, v string) (Delay, error) {
	base, perCP, hasCP := strings.Cut(v, "+")
	if !hasCP {
		d, err := parseDuration(v)
		return Delay{Base: d}, err
	}
	perCP = strings.TrimSpace(perCP)
	if !strings.HasSuffix(strings.ToLower(perCP), "/cp") {
		return Delay{}, fmt.Errorf("per-entry latency term must end in /cp (e.g. 55ms/cp), got: %s", perCP)
	}
	if s != StructureProbe {
		return Delay{}, errors.New("a /cp term only applies to structure_probe (only a delimiter'd response has a fanout)")
	}
	b, err := parseDuration(strings.TrimSpace(base))
	if err != nil {
		return Delay{}, err
	}
	c, err := parseDuration(strings.TrimSpace(perCP[:len(perCP)-3]))
	return Delay{Base: b, PerCommonPrefix: c}, err
}

// parseDuration reads one delay: a number then ms or s (the number in the
// floating-point grammar of javanum.ParseDouble: decimal or hexadecimal,
// an optional d/f suffix; 0x1p0s is one second), or a bare integer count
// of milliseconds (javanum.ParseInt). The text is lower-cased first, so
// NaN and Infinity are not spellings of a delay, and a value too large for
// a time.Duration saturates.
func parseDuration(s string) (time.Duration, error) {
	v := strings.ToLower(s)
	bad := fmt.Errorf("bad latency duration '%s' (e.g. 6865ms, 6.9s, 250)", s)
	var d time.Duration
	switch {
	case strings.HasSuffix(v, "ms"), strings.HasSuffix(v, "s"):
		unit, cut := 1e9, 1
		if strings.HasSuffix(v, "ms") {
			unit, cut = 1e6, 2
		}
		f, ok := javanum.ParseDouble(v[:len(v)-cut])
		if !ok {
			return 0, bad
		}
		d = time.Duration(javanum.Saturate(f * unit))
	default:
		ms, ok := javanum.ParseInt(v, 64)
		if !ok {
			return 0, bad
		}
		if ms < 0 {
			d = -1
		} else {
			d = mulSaturating(time.Millisecond, ms)
		}
	}
	if d < 0 {
		return 0, fmt.Errorf("latency duration must be >= 0, got '%s'", s)
	}
	return d, nil
}
