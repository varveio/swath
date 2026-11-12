// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package docs holds no code: its test checks the Markdown under the
// module's root.
package docs_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"unicode"
)

var (
	fence    = regexp.MustCompile("(?ms)^\\s*```.*?^\\s*```[^\\n]*$")
	codeSpan = regexp.MustCompile("`[^`\\n]*`")
	link     = regexp.MustCompile(`\]\(([^)\s]+)(?:\s+"[^"]*")?\)`)
	heading  = regexp.MustCompile(`(?m)^#{1,6}\s+(.+?)\s*#*\s*$`)
	htmlID   = regexp.MustCompile(`<a\s+(?:id|name)="([^"]+)"`)
	mdLink   = regexp.MustCompile(`\[([^\]]*)\]\([^)]*\)`)
)

// slug is a heading's anchor as GitHub renders it: lower case, links
// reduced to their text, every character but letters, digits, spaces,
// hyphens and underscores dropped, spaces turned into hyphens.
func slug(h string) string {
	h = mdLink.ReplaceAllString(h, "$1")
	var b strings.Builder
	for _, r := range strings.ToLower(h) {
		switch {
		case r == ' ':
			b.WriteByte('-')
		case r == '-' || r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r):
			b.WriteRune(r)
		}
	}
	return b.String()
}

// anchors are a Markdown file's heading anchors, a repeated one numbered
// as GitHub does (name, name-1, ...), and its HTML anchors.
func anchors(text string) map[string]bool {
	text = fence.ReplaceAllString(text, "")
	a := map[string]bool{}
	seen := map[string]int{}
	for _, m := range heading.FindAllStringSubmatch(text, -1) {
		s := slug(m[1])
		if n := seen[s]; n > 0 {
			a[s+"-"+itoa(n)] = true
		} else {
			a[s] = true
		}
		seen[s]++
	}
	for _, m := range htmlID.FindAllStringSubmatch(text, -1) {
		a[m[1]] = true
	}
	return a
}

func itoa(n int) string {
	if n < 10 {
		return string(rune('0' + n))
	}
	return itoa(n/10) + itoa(n%10)
}

// TestMarkdownLinks: every relative link in the module's Markdown names a
// file or directory that exists, and a fragment into a Markdown file names
// one of its headings or anchors. External URLs are not fetched.
func TestMarkdownLinks(t *testing.T) {
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	cache := map[string]map[string]bool{}
	anchorsOf := func(path string) map[string]bool {
		if a, ok := cache[path]; ok {
			return a
		}
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		cache[path] = anchors(string(b))
		return cache[path]
	}
	checked := 0
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (d.Name() == "testdata" || strings.HasPrefix(d.Name(), ".")) && path != root {
			return filepath.SkipDir
		}
		if d.IsDir() || !strings.HasSuffix(path, ".md") {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := codeSpan.ReplaceAllString(fence.ReplaceAllString(string(b), ""), "")
		rel, _ := filepath.Rel(root, path)
		for _, m := range link.FindAllStringSubmatch(text, -1) {
			target := m[1]
			if strings.Contains(target, "://") || strings.HasPrefix(target, "mailto:") {
				continue
			}
			file, frag, _ := strings.Cut(target, "#")
			dest := path
			if file != "" {
				dest = filepath.Join(filepath.Dir(path), filepath.FromSlash(file))
				if _, err := os.Stat(dest); err != nil {
					t.Errorf("%s: link %s: no such file", rel, target)
					continue
				}
			}
			checked++
			if frag != "" && strings.HasSuffix(dest, ".md") && !anchorsOf(dest)[frag] {
				t.Errorf("%s: link %s: no heading or anchor %q there", rel, target, frag)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if checked == 0 {
		t.Fatal("no links checked")
	}
}
