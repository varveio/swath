// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package filter

import (
	"regexp"
	"strings"
)

// Pruning describes which exclude rules can prevent local child traversal.
// Counts describe configured rules, not descendants that were never visited.
type Pruning struct {
	Reason         string `json:"reason"`
	RecursiveGlobs int    `json:"recursive_globs"`
	RowOnlyGlobs   int    `json:"row_only_globs"`
	RegexRules     int    `json:"regex_rules"`
}

// PruneDirectories returns an immutable predicate on directory keys (with a
// trailing slash). True proves that an exclude covers every descendant; it
// says nothing about retaining the directory's own row. Only terminal /**
// globs supply that proof. Other rules remain authoritative row filters.
func (s *Spec) PruneDirectories() (func(string) bool, Pruning, error) {
	plan := Pruning{Reason: "no_excludes"}
	if s.Empty() {
		return nil, plan, nil
	}
	if !s.compiled {
		if err := s.Validate(); err != nil {
			return nil, plan, err
		}
	}
	plan.RegexRules = len(s.ExcludeRegex)
	var alternatives []string
	for _, pattern := range s.Exclude {
		_, descendants, err := globRegexps(pattern)
		if err != nil {
			return nil, plan, err
		}
		if descendants == "" {
			plan.RowOnlyGlobs++
			continue
		}
		plan.RecursiveGlobs++
		alternatives = append(alternatives, "(?:"+descendants+")")
	}
	if len(alternatives) == 0 {
		if plan.RowOnlyGlobs+plan.RegexRules > 0 {
			plan.Reason = "row_filters_only"
		}
		return nil, plan, nil
	}
	plan.Reason = "recursive_excludes"
	re, err := regexp.Compile(strings.Join(alternatives, "|"))
	if err != nil {
		return nil, plan, err
	}
	base := s.Base
	return func(key string) bool {
		if !strings.HasSuffix(key, "/") {
			return false
		}
		rel, _ := strings.CutPrefix(key, base)
		return re.MatchString(rel)
	}, plan, nil
}
