// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package filter is Java swath's seven row filters: --include and
// --exclude (a regular expression found anywhere in the key), --min-size
// and --max-size (inclusive), --modified-since and --modified-until
// (inclusive, UTC without an offset) and --storage-class (any of a list).
// Filters reduce the rows emitted, not the requests made (except that an
// --include anchored at the start with a literal prefix narrows the listed
// prefix, NarrowPrefix). A filtered dataset says so in its manifest
// (coverage.filters), and a reader that needs a whole source refuses it.
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

	"github.com/varveio/swath/go/internal/schema"
)

// Spec is a run's filters as given (their text is what the manifest and
// the identity record).
type Spec struct {
	Include, Exclude  string
	MinSize, MaxSize  string // Java size text: 1k, 256mb, 1.5GiB (binary units)
	Since, Until      string // 2026-01-01, 2026-01-01T12:00:00, 2026-01-01T12:00:00Z
	StorageClasses    []string
	include, exclude  *regexp.Regexp
	minSize, maxSize  int64
	since, until      int64 // epoch microseconds
	classes           map[string]bool
	hasSize, hasMtime bool
	compiled          bool
}

// Empty reports whether s filters nothing.
func (s *Spec) Empty() bool {
	return s == nil || s.Include == "" && s.Exclude == "" && s.MinSize == "" && s.MaxSize == "" && s.Since == "" && s.Until == "" &&
		len(s.StorageClasses) == 0
}

// Validate parses the filters; an error names the flag.
func (s *Spec) Validate() error {
	if s.Empty() {
		return nil
	}
	var err error
	if s.Include != "" {
		if s.include, err = regexp.Compile(s.Include); err != nil {
			return fmt.Errorf("invalid --include regex (Go RE2 syntax): %w", err)
		}
	}
	if s.Exclude != "" {
		if s.exclude, err = regexp.Compile(s.Exclude); err != nil {
			return fmt.Errorf("invalid --exclude regex (Go RE2 syntax): %w", err)
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

// Descriptions are Java's filter descriptions, in Java's order: the
// manifest's coverage.filters and part of the run's identity.
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
	add("include", s.Include)
	add("exclude", s.Exclude)
	add("min_size", s.MinSize)
	add("max_size", s.MaxSize)
	add("modified_after", s.Since)
	add("modified_before", s.Until)
	if len(s.StorageClasses) > 0 {
		add("storage_class", strings.Join(s.StorageClasses, ","))
	}
	return d
}

// Compile binds the filters to a schema's columns (size, last_modified,
// storage_class by name). The predicate is called on the listing workers
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
		case "storage_class":
			class = c.Str
		}
	}
	switch {
	case s.hasSize && size == nil:
		return nil, fmt.Errorf("--min-size and --max-size need a size column; schema %s has none", sc.ID)
	case s.hasMtime && mtime == nil:
		return nil, fmt.Errorf("--modified-since and --modified-until need a last_modified column; schema %s has none", sc.ID)
	case s.classes != nil && class == nil:
		return nil, fmt.Errorf("--storage-class needs a storage_class column; schema %s has none", sc.ID)
	}
	inc, exc := s.include, s.exclude
	minS, maxS, since, until, classes := s.minSize, s.maxSize, s.since, s.until, s.classes
	hasSize, hasMtime := s.hasSize, s.hasMtime
	return func(r *schema.Row[P]) bool {
		if inc != nil && !inc.MatchString(r.Key) {
			return false
		}
		if exc != nil && exc.MatchString(r.Key) {
			return false
		}
		if hasSize {
			if v, _ := size(&r.Data); v < minS || v > maxS {
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

// NarrowPrefix is the listed prefix an --include anchored at the key's
// start with a literal (^logs/2026) implies, inside scope (the prefix the
// source names): the longer of the two when one extends the other, else
// scope unchanged. The rows are the same; the requests fewer.
func (s *Spec) NarrowPrefix(scope string) string {
	if s == nil || s.Include == "" {
		return scope
	}
	re, err := syntax.Parse(s.Include, syntax.Perl)
	if err != nil {
		return scope
	}
	lit := ""
	if re.Op == syntax.OpConcat && len(re.Sub) >= 2 && re.Sub[0].Op == syntax.OpBeginText &&
		re.Sub[1].Op == syntax.OpLiteral && re.Sub[1].Flags&syntax.FoldCase == 0 {
		lit = string(re.Sub[1].Rune)
	}
	if strings.HasPrefix(lit, scope) {
		return lit
	}
	return scope
}

// ParseSize is Java's SizeParser: a non-negative decimal number and an
// optional binary unit (b; k, kb, kib; m, mb, mib; g, gb, gib; t, tb,
// tib; case-insensitive) that must come to a whole number of bytes.
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
	mult := map[string]int64{"": 1, "b": 1, "k": 1 << 10, "kb": 1 << 10, "kib": 1 << 10, "m": 1 << 20, "mb": 1 << 20, "mib": 1 << 20,
		"g": 1 << 30, "gb": 1 << 30, "gib": 1 << 30, "t": 1 << 40, "tb": 1 << 40, "tib": 1 << 40}
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

// ParseTime is Java's TimeParser: an instant with Z or an offset, a local
// date-time taken as UTC, or a date (its start, UTC); epoch microseconds.
func ParseTime(text string) (int64, error) {
	s := strings.TrimSpace(text)
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05.999999999", "2006-01-02T15:04", "2006-01-02"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UnixMicro(), nil
		}
	}
	return 0, fmt.Errorf("invalid date/time: %s (use 2026-01-01, 2026-01-01T12:00:00, or 2026-01-01T12:00:00Z)", text)
}

// Equal reports whether two description lists are the same filters.
func Equal(a, b []string) bool { return slices.Equal(a, b) }
