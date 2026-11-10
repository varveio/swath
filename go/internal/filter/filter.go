// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package filter is the row filters: --include and --exclude (globs,
// glob.go), --include-regex and --exclude-regex (a regular expression
// found anywhere in the key), --min-size and --max-size (inclusive),
// --modified-since and --modified-until (inclusive, UTC without an offset,
// or a duration before the run's start) and --storage-class (any of a
// list). A row is kept when there is no include rule or one of them
// (glob or regex) matches, no exclude matches, and the other filters pass;
// the order rules are given in does not matter. Filters reduce the rows
// emitted, not the requests made, except that includes sharing a literal
// prefix narrow the listed prefix (NarrowPrefix). A filtered dataset says
// so in its manifest (coverage.filters), and a reader that needs a whole
// source refuses it.
//
// Regular expressions are Go's (RE2), not Java's: the syntax is largely
// shared, but backreferences, lookaround, possessive quantifiers and
// atomic groups do not exist (a pattern with them is refused), and \Z,
// \z and character-class details differ; RE2 matches in linear time.
package filter

import (
	"errors"
	"fmt"
	"math/big"
	"regexp"
	"regexp/syntax"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source"
)

// Spec is a run's filters as given (their text is what the manifest and
// the identity record).
type Spec struct {
	Include, Exclude           []string // globs
	IncludeRegex, ExcludeRegex []string
	MinSize, MaxSize           string // size text (ParseSize): 1K, 256MB, 1.5GiB
	// Since and Until are 2026-01-01, 2026-01-01T12:00:00 or
	// 2026-01-01T12:00:00Z, or a duration before the run's start (7d),
	// which Resolve replaces by the time it names.
	Since, Until   string
	StorageClasses []string
	// Base is the globs' root: the key prefix the source names, up to its
	// last '/' ("" for a local tree, whose keys are relative to its root).
	Base string
	// incGlob and excGlob match the key relative to Base, incRe and excRe
	// the key: each the alternation of its rules, nil for none.
	incGlob, excGlob, incRe, excRe *regexp.Regexp
	minSize, maxSize               int64
	since, until                   int64 // epoch microseconds
	classes                        map[string]bool
	hasSize, hasMtime              bool
	compiled                       bool
}

// Empty reports whether s filters nothing.
func (s *Spec) Empty() bool {
	return s == nil || len(s.Include)+len(s.Exclude)+len(s.IncludeRegex)+len(s.ExcludeRegex) == 0 && s.MinSize == "" && s.MaxSize == "" && s.Since == "" && s.Until == "" &&
		len(s.StorageClasses) == 0
}

// Validate parses the filters; an error names the flag.
func (s *Spec) Validate() error {
	if s.Empty() {
		return nil
	}
	var err error
	for _, r := range []struct {
		flag  string
		pats  []string
		re    **regexp.Regexp
		globs bool
	}{{"include", s.Include, &s.incGlob, true}, {"exclude", s.Exclude, &s.excGlob, true},
		{"include-regex", s.IncludeRegex, &s.incRe, false}, {"exclude-regex", s.ExcludeRegex, &s.excRe, false}} {
		if *r.re, err = anyOf(r.flag, r.pats, r.globs); err != nil {
			return err
		}
	}
	s.minSize, s.maxSize = 0, 1<<63-1
	if s.MinSize != "" {
		if s.minSize, err = ParseSize(s.MinSize); err != nil {
			return fmt.Errorf("--min-size: %w", err)
		}
	}
	if s.MaxSize != "" {
		if s.maxSize, err = ParseSize(s.MaxSize); err != nil {
			return fmt.Errorf("--max-size: %w", err)
		}
	}
	if s.minSize > s.maxSize {
		return fmt.Errorf("--min-size must be <= --max-size (got %s > %s)", s.MinSize, s.MaxSize)
	}
	s.hasSize = s.MinSize != "" || s.MaxSize != ""
	s.since, s.until = -1<<63, 1<<63-1
	if s.Since != "" {
		if s.since, err = ParseTime(s.Since); err != nil {
			return fmt.Errorf("--modified-since: %w", err)
		}
	}
	if s.Until != "" {
		if s.until, err = ParseTime(s.Until); err != nil {
			return fmt.Errorf("--modified-until: %w", err)
		}
	}
	if s.since > s.until {
		return fmt.Errorf("--modified-since must be <= --modified-until (got %s > %s)", s.Since, s.Until)
	}
	s.hasMtime = s.Since != "" || s.Until != ""
	if len(s.StorageClasses) > 0 {
		s.classes = map[string]bool{}
		for _, c := range s.StorageClasses {
			if c = strings.ToUpper(strings.TrimSpace(c)); c != "" {
				s.classes[c] = true
			}
		}
		if len(s.classes) == 0 {
			return errors.New("--storage-class: no class given")
		}
	}
	s.compiled = true
	return nil
}

// Reads reports which of the columns beyond the key the filters read: a
// source that cannot fill one refuses them before it lists
// (registry.Factory.CheckFilter).
func (s *Spec) Reads() (size, mtime, class bool) {
	if s == nil {
		return false, false, false
	}
	return s.MinSize != "" || s.MaxSize != "", s.Since != "" || s.Until != "", len(s.StorageClasses) > 0
}

// anyOf compiles flag's patterns (globs, or regular expressions) to one
// expression matching where any of them does; nil for none.
func anyOf(flag string, pats []string, globs bool) (*regexp.Regexp, error) {
	if len(pats) == 0 {
		return nil, nil
	}
	alt := make([]string, len(pats))
	for i, p := range pats {
		if p == "" {
			return nil, fmt.Errorf("--%s: empty pattern", flag)
		}
		alt[i] = p
		if globs {
			re, err := globRegexp(p)
			if err != nil {
				return nil, fmt.Errorf("--%s %q: %w", flag, p, err)
			}
			alt[i] = re
		} else if _, err := regexp.Compile(p); err != nil {
			return nil, fmt.Errorf("invalid --%s %q (Go RE2 syntax): %w", flag, p, err)
		}
	}
	return regexp.Compile("(?:" + strings.Join(alt, ")|(?:") + ")")
}

// Descriptions are the filters' descriptions: the manifest's
// coverage.filters and part of the run's identity. Each pattern is one,
// sorted and once each within its flag, so the same rules in another order
// are the same filters.
func (s *Spec) Descriptions() []string {
	if s.Empty() {
		return nil
	}
	var d []string
	add := func(k, v string) {
		if v != "" {
			d = append(d, k+"="+v)
		}
	}
	for _, r := range []struct {
		k    string
		pats []string
	}{{"include", s.Include}, {"include_regex", s.IncludeRegex}, {"exclude", s.Exclude}, {"exclude_regex", s.ExcludeRegex}} {
		pats := slices.Clone(r.pats)
		slices.Sort(pats)
		for _, p := range slices.Compact(pats) {
			add(r.k, p)
		}
	}
	add("min_size", s.MinSize)
	add("max_size", s.MaxSize)
	add("modified_after", s.Since)
	add("modified_before", s.Until)
	if len(s.StorageClasses) > 0 {
		add("storage_class", strings.Join(s.StorageClasses, ","))
	}
	return d
}

// Compile binds the filters to a schema's columns (size and last_modified
// by name, the storage class as the schema declares it). The predicate is called on the listing workers
// for every row: it allocates nothing.
func Compile[P any](s *Spec, sc *schema.Schema[P]) (func(*schema.Row[P]) bool, error) {
	if s.Empty() {
		return nil, nil
	}
	if !s.compiled {
		if err := s.Validate(); err != nil {
			return nil, err
		}
	}
	var size, mtime func(*P) (int64, bool)
	var class func(*P) (string, bool)
	for _, c := range sc.Columns {
		switch c.Name {
		case "size":
			size = c.Int
		case "last_modified":
			mtime = c.Int
		}
		if c.Name == sc.StorageClass && sc.StorageClass != "" {
			class = c.Str
		}
	}
	switch {
	case s.hasSize && size == nil:
		return nil, fmt.Errorf("--min-size and --max-size need a size column; schema %s has none", sc.ID)
	case s.hasMtime && mtime == nil:
		return nil, fmt.Errorf("--modified-since and --modified-until need a last_modified column; schema %s has none", sc.ID)
	case s.classes != nil && class == nil:
		return nil, fmt.Errorf("--storage-class needs a storage class column; schema %s has none", sc.ID)
	}
	incGlob, excGlob, incRe, excRe, base := s.incGlob, s.excGlob, s.incRe, s.excRe, s.Base
	minS, maxS, since, until, classes := s.minSize, s.maxSize, s.since, s.until, s.classes
	hasSize, hasMtime := s.hasSize, s.hasMtime
	return func(r *schema.Row[P]) bool {
		rel, _ := strings.CutPrefix(r.Key, base)
		if (incGlob != nil || incRe != nil) && !(incGlob != nil && incGlob.MatchString(rel) || incRe != nil && incRe.MatchString(r.Key)) {
			return false
		}
		if excGlob != nil && excGlob.MatchString(rel) || excRe != nil && excRe.MatchString(r.Key) {
			return false
		}
		if hasSize {
			// An absent size (a delete marker's) is no size: it is
			// neither below nor within a bound, so it never matches.
			if v, ok := size(&r.Data); !ok || v < minS || v > maxS {
				return false
			}
		}
		if hasMtime {
			// Java's: a row without a modification time is kept.
			if v, ok := mtime(&r.Data); ok && v != 0 && (v < since || v > until) {
				return false
			}
		}
		if classes != nil {
			v, _ := class(&r.Data)
			if !classes[v] && !classes[strings.ToUpper(v)] {
				return false
			}
		}
		return true
	}, nil
}

// The engagement of NarrowPrefix, for the run report: why the listed
// prefix was or was not narrowed by the includes.
const (
	// Narrowed: the includes share a literal prefix longer than the
	// source's, which is listed in its place.
	Narrowed = "narrowed"
	// NarrowUnanchored: an include has no literal prefix (a glob without
	// a '/', which matches at any depth, or a regex not starting ^literal).
	NarrowUnanchored = "include_without_prefix"
	// NarrowNoLonger: the includes' common literal prefix does not extend
	// the source's.
	NarrowNoLonger = "no_longer_prefix"
	// NarrowUnsupported: the provider lists only the prefix its root names
	// (registry.Factory.WithPrefix).
	NarrowUnsupported = "provider_lists_root"
)

// NarrowPrefix is the listed prefix the includes imply inside scope (the
// prefix the source names), and its engagement (Narrowed, or why not; ""
// without includes): the longest literal prefix common to every include,
// glob (after Base) and regex (^literal) alike, when it extends scope.
// Every key an include matches starts with it, so the rows are the same
// and the requests fewer. Excludes never change it. s is valid
// (Validate).
func (s *Spec) NarrowPrefix(scope string) (string, string) {
	if s == nil || len(s.Include)+len(s.IncludeRegex) == 0 {
		return scope, ""
	}
	var lits []string
	for _, p := range s.Include {
		lit, ok := globLiteral(p)
		if !ok {
			return scope, NarrowUnanchored
		}
		lits = append(lits, s.Base+lit)
	}
	for _, p := range s.IncludeRegex {
		lit := regexLiteral(p)
		if lit == "" {
			return scope, NarrowUnanchored
		}
		lits = append(lits, lit)
	}
	common := lits[0]
	for _, l := range lits[1:] {
		n := 0
		for n < len(common) && n < len(l) && common[n] == l[n] {
			n++
		}
		common = common[:n]
	}
	// A cut inside a character (é and è share a byte) is taken back to its
	// start: the listed prefix stays text.
	for len(common) > 0 && len(common) < len(lits[0]) && !utf8.RuneStart(lits[0][len(common)]) {
		common = common[:len(common)-1]
	}
	if len(common) <= len(scope) || !strings.HasPrefix(common, scope) {
		return scope, NarrowNoLonger
	}
	return common, Narrowed
}

// regexLiteral is the literal a regex anchored at the key's start begins
// with (^logs/2026), "" for none.
func regexLiteral(p string) string {
	re, err := syntax.Parse(p, syntax.Perl)
	if err != nil {
		return ""
	}
	if re.Op == syntax.OpConcat && len(re.Sub) >= 2 && re.Sub[0].Op == syntax.OpBeginText &&
		re.Sub[1].Op == syntax.OpLiteral && re.Sub[1].Flags&syntax.FoldCase == 0 {
		return string(re.Sub[1].Rune)
	}
	return ""
}

// ParseSize is swath's one size syntax (the filters and the resource and
// part sizes): a non-negative decimal number and an optional unit, in any
// case: B or none bytes; K, M, G, T and KiB, MiB, GiB, TiB binary; KB, MB,
// GB, TB decimal. It must come to a whole number of bytes.
func ParseSize(text string) (int64, error) {
	s := strings.ToLower(strings.TrimSpace(text))
	i := 0
	for i < len(s) && (s[i] >= '0' && s[i] <= '9' || s[i] == '.') {
		i++
	}
	num, unit := s[:i], strings.TrimSpace(s[i:])
	if num == "" {
		return 0, fmt.Errorf("invalid size: %s", text)
	}
	mult := map[string]int64{"": 1, "b": 1, "k": 1 << 10, "kib": 1 << 10, "kb": 1e3, "m": 1 << 20, "mib": 1 << 20, "mb": 1e6,
		"g": 1 << 30, "gib": 1 << 30, "gb": 1e9, "t": 1 << 40, "tib": 1 << 40, "tb": 1e12}
	m, ok := mult[unit]
	if !ok {
		return 0, fmt.Errorf("unknown size unit '%s' in: %s", unit, text)
	}
	r, ok := new(big.Rat).SetString(num)
	if !ok {
		return 0, fmt.Errorf("invalid size number: %s", text)
	}
	r.Mul(r, new(big.Rat).SetInt64(m))
	if !r.IsInt() || !r.Num().IsInt64() {
		return 0, fmt.Errorf("size is out of range or not an exact byte count: %s", text)
	}
	return r.Num().Int64(), nil
}

// Resolve replaces a --modified-since or --modified-until given as a
// duration (7d) by the time that long before now, the run's start, so that
// the journal, coverage.filters and the report record the time and a
// resume keeps the same bound.
func (s *Spec) Resolve(now time.Time) {
	for _, t := range []*string{&s.Since, &s.Until} {
		if *t == "" {
			continue
		}
		if _, err := ParseTime(*t); err == nil {
			continue
		}
		if d, err := source.ParseDuration(strings.TrimSpace(*t)); err == nil && d >= 0 {
			*t = now.Add(-d).UTC().Truncate(time.Second).Format(time.RFC3339)
		}
	}
}

// ParseTime is Java's TimeParser: an instant with Z or an offset, a local
// date-time taken as UTC, or a date (its start, UTC); epoch microseconds.
func ParseTime(text string) (int64, error) {
	s := strings.TrimSpace(text)
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05.999999999", "2006-01-02T15:04", "2006-01-02"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UnixMicro(), nil
		}
	}
	return 0, fmt.Errorf("invalid date/time: %s (use 2026-01-01, 2026-01-01T12:00:00, 2026-01-01T12:00:00Z, or a duration before now such as 7d)", text)
}

// Equal reports whether two description lists are the same filters.
func Equal(a, b []string) bool { return slices.Equal(a, b) }
