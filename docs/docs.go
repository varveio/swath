// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package docs is the documentation's Go side: Path hands a test a page
// under docs/, and the package's tests check the Markdown under the
// module's root. Only _test.go files import it (docs/readers_test.go
// enforces that): a helper in a regular file would hide the tests that
// read pages through it from ci.yml go-docs, and would link testing into
// a binary.
package docs

import (
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestPrefix begins the name of every test that reads a page under docs/.
// ci.yml go-docs runs exactly these tests on a pull request that changes
// only documentation; a test that read docs/ under another name would not
// run there.
const TestPrefix = "TestDocs"

// Unread are the names under docs/ no test may read, a directory by its
// trailing slash: scripts/ci/go-ci-policy.py selects no Go test for a
// change to them (LIGHT_DOCS, which scripts/ci/test-ci-policy.py holds
// equal to this list).
var Unread = []string{"assets/", "ops/", "style.md"}

// Path is the path of name, slash-separated and relative to docs/, for t
// to read or, under UPDATE=1, rewrite. It fails t unless t's top-level
// test is named TestPrefix... and name is not under Unread.
func Path(t testing.TB, name string) string {
	t.Helper()
	if top, _, _ := strings.Cut(t.Name(), "/"); !strings.HasPrefix(top, TestPrefix) {
		t.Fatalf("%s reads docs/%s: name it %s... so that ci.yml go-docs runs it", top, name, TestPrefix)
	}
	for _, u := range Unread {
		if name == u || strings.HasSuffix(u, "/") && strings.HasPrefix(name, u) {
			t.Fatalf("docs/%s is under docs/%s, which no Go test reads: a change to it selects no Go test", name, u)
		}
	}
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("docs.Path: no caller")
	}
	return filepath.Join(filepath.Dir(file), filepath.FromSlash(name))
}
