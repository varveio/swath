// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package filter

import (
	"slices"
	"testing"
)

func TestPruneDirectories(t *testing.T) {
	for _, tc := range []struct {
		pattern string
		yes, no []string
	}{
		{"dir/**", []string{"dir/"}, []string{"dir", "directory/", "sub/dir/"}},
		{"/dir/**", []string{"dir/"}, []string{"/dir/", "sub/dir/"}},
		{"**/node_modules/**", []string{"node_modules/", "a/node_modules/", "a\n/b/node_modules/"}, []string{"node_modules-old/", "a/not_node_modules/"}},
		{"{raw,cache}/**", []string{"raw/", "cache/"}, []string{"cachex/", "a/raw/"}},
		{"a[0-9]/**", []string{"a1/"}, []string{"a/", "a10/", "sub/a1/"}},
		{"a?/*/**", []string{"ab/c/", "a\n/z/"}, []string{"ab/", "ab/c/d/"}},
		{"literal\\*/**", []string{"literal*/"}, []string{"literalx/", "a/literal*/"}},
		{"**/日本/**", []string{"日本/", "é/日本/"}, []string{"日本語/"}},
		{"a/**/**", []string{"a/", "a/sub/"}, []string{"ab/"}},
		{"dir/", nil, []string{"dir/", "dir/sub/"}},
		{"*.tmp", nil, []string{"a.tmp/"}},
		{"**", nil, []string{"dir/"}},
		{"dir/**/", nil, []string{"dir/", "dir/sub/"}},
		{"dir/\\*\\*", nil, []string{"dir/", "dir/**/"}},
	} {
		t.Run(tc.pattern, func(t *testing.T) {
			spec := &Spec{Exclude: []string{tc.pattern}}
			prune, plan, err := spec.PruneDirectories()
			if err != nil {
				t.Fatal(err)
			}
			wantRecursive := 0
			if len(tc.yes) > 0 {
				wantRecursive = 1
			}
			if plan.RecursiveGlobs != wantRecursive {
				t.Fatalf("plan %+v", plan)
			}
			for _, key := range append(slices.Clone(tc.yes), tc.no...) {
				got := prune != nil && prune(key)
				if got != slices.Contains(tc.yes, key) {
					t.Errorf("prune(%q)=%v", key, got)
				}
			}
		})
	}
}

func TestPrunePlan(t *testing.T) {
	for _, tc := range []struct {
		spec *Spec
		want Pruning
	}{
		{nil, Pruning{Reason: "no_excludes"}},
		{&Spec{Include: []string{"*.go"}}, Pruning{Reason: "no_excludes"}},
		{&Spec{Exclude: []string{"*.tmp"}, ExcludeRegex: []string{"^cache/"}}, Pruning{Reason: "row_filters_only", RowOnlyGlobs: 1, RegexRules: 1}},
		{&Spec{Exclude: []string{"dir/**", "dir/**", "dir/"}, ExcludeRegex: []string{".*"}}, Pruning{Reason: "recursive_excludes", RecursiveGlobs: 2, RowOnlyGlobs: 1, RegexRules: 1}},
	} {
		_, got, err := tc.spec.PruneDirectories()
		if err != nil {
			t.Fatal(err)
		}
		if got != tc.want {
			t.Errorf("got %+v want %+v", got, tc.want)
		}
	}
	for _, spec := range []*Spec{{Exclude: []string{"bad/**x"}}, {ExcludeRegex: []string{"["}}, {MinSize: "oops"}} {
		if _, _, err := spec.PruneDirectories(); err == nil {
			t.Errorf("accepted invalid filter %+v", spec)
		}
	}
	spec := &Spec{Base: "prefix/", Exclude: []string{"dir/**"}}
	prune, _, err := spec.PruneDirectories()
	if err != nil || prune == nil || !prune("prefix/dir/") || prune("prefix/dirx/") {
		t.Fatalf("base predicate: %v", err)
	}
}

// Every proven directory prefix must have all its descendants rejected by the
// original row matcher. Inputs include escaped terminal
// stars and valid patterns that match directory rows but not every child.
func FuzzPruneDirectoriesSound(f *testing.F) {
	for _, pattern := range []string{"dir/**", "**/node_modules/**", "{a,b}/**", "a/**/", "a/\\*\\*", "a/[!z]/**"} {
		f.Add(pattern, "dir/", "child\n/leaf")
	}
	f.Fuzz(func(t *testing.T, pattern, dir, descendant string) {
		spec := &Spec{Exclude: []string{pattern}}
		prune, _, err := spec.PruneDirectories()
		if err != nil || prune == nil || !prune(dir) || descendant == "" {
			return
		}
		re, err := anyOf("exclude", []string{pattern}, true)
		if err != nil {
			t.Fatal(err)
		}
		if !re.MatchString(dir + descendant) {
			t.Fatalf("%q prunes %q but keeps descendant %q", pattern, dir, dir+descendant)
		}
	})
}
