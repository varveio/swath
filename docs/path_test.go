// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package docs_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/varveio/swath/docs"
)

// recorder is a testing.TB that records Path's failures instead of
// ending the test.
type recorder struct {
	testing.TB
	name   string
	failed []string
}

func (r *recorder) Name() string { return r.name }
func (r *recorder) Helper()      {}
func (r *recorder) Fatal(a ...any) {
	r.failed = append(r.failed, fmt.Sprint(a...))
}
func (r *recorder) Fatalf(format string, a ...any) {
	r.failed = append(r.failed, fmt.Sprintf(format, a...))
}

// TestPath: Path names the page under docs/ for a TestDocs test and its
// subtests, and fails any other test and any page under docs.Unread.
func TestPath(t *testing.T) {
	for _, c := range []struct {
		test, name string
		ok         bool
	}{
		{"TestDocsCLIFlags", "commands/swath.md", true},
		{"TestDocsCLIFlags/list", "commands/swath-list.md", true},
		{"TestDocsStyle", "styles.md", true},
		{"TestCLIFlags", "commands/swath.md", false},
		{"TestCLIFlags/TestDocs", "commands/swath.md", false},
		{"TestDocsA", "assets/swath-demo-v0.2.1.gif", false},
		{"TestDocsA", "ops/dev/CI.md", false},
		{"TestDocsA", "style.md", false},
	} {
		r := &recorder{TB: t, name: c.test}
		path := docs.Path(r, c.name)
		if (len(r.failed) == 0) != c.ok {
			t.Errorf("%s reading %s: failures %q, want ok %v", c.test, c.name, r.failed, c.ok)
		}
		if c.ok && c.name != "styles.md" {
			if _, err := os.Stat(path); err != nil {
				t.Errorf("%s: %v", c.name, err)
			}
		}
	}
}
