// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"os/exec"
	"strings"
	"testing"
)

func TestKindString(t *testing.T) {
	want := map[Kind]string{
		KindObject:    "object",
		KindPrefix:    "prefix",
		KindFile:      "file",
		KindDir:       "dir",
		KindSymlink:   "symlink",
		KindOther:     "other",
		0:             "Kind(0)",
		KindOther + 1: "Kind(7)",
	}
	for k, name := range want {
		if got := k.String(); got != name {
			t.Errorf("Kind %d: String() = %q, want %q", uint8(k), got, name)
		}
	}
}

// TestZeroLookupStateIsUnknown: a state never set is unknown, never
// absent.
func TestZeroLookupStateIsUnknown(t *testing.T) {
	var r LookupResult
	if r.State != LookupUnknown || r.State.String() != "unknown" {
		t.Errorf("zero state = %v", r.State)
	}
	if LookupPresent.String() != "present" || LookupAbsent.String() != "absent" || LookupState(3).String() != "LookupState(3)" {
		t.Errorf("names: %v, %v, %v", LookupPresent, LookupAbsent, LookupState(3))
	}
}

// TestStandardLibraryOnly: the package imports nothing beyond the
// standard library, so naming the contract links no lister, SDK or
// Parquet code.
func TestStandardLibraryOnly(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", ".").Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	const self = "github.com/varveio/swath/reader/listing"
	listed := false
	for line := range strings.Lines(string(out)) {
		switch dep := strings.TrimSpace(line); dep {
		case self:
			listed = true
		case "":
		default:
			t.Errorf("imports %s", dep)
		}
	}
	if !listed {
		t.Errorf("go list did not list %s: %q", self, out)
	}
}
