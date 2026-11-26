// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

// TestNoCloudSDK: neither the packages this module builds (its tests
// included) nor its module graph name a cloud SDK, and none of them is the
// lister's module. The go commands run with the workspace off, so the
// graph is this module's, not the workspace's three modules'.
func TestNoCloudSDK(t *testing.T) {
	for _, args := range [][]string{
		{"list", "-deps", "-test", "-f", "{{with .Module}}{{.Path}}{{end}}", "./..."},
		{"list", "-m", "all"},
	} {
		cmd := exec.Command("go", args...)
		cmd.Env = append(os.Environ(), "GOWORK=off")
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("go %s: %v", strings.Join(args, " "), err)
		}
		for line := range strings.Lines(string(out)) {
			m := strings.ToLower(strings.TrimSpace(line))
			if strings.Contains(m, "aws") || strings.Contains(m, "cloud.google.com") || strings.Contains(m, "azure") || m == "github.com/varveio/swath" {
				t.Errorf("go %s: %s", strings.Join(args, " "), strings.TrimSpace(line))
			}
		}
	}
}

// TestNoParquetGoImport: the package's own code (its tests aside) does not
// import parquet-go, nor does the inventory view's: they read values and
// schemas through SeekParquet's own types, so parquet-go is no part of
// this module's API.
func TestNoParquetGoImport(t *testing.T) {
	out, err := exec.Command("go", "list", "-f", `{{join .Imports "\n"}}`, ".", "./view").Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	for line := range strings.Lines(string(out)) {
		if strings.HasPrefix(line, "github.com/parquet-go/") {
			t.Errorf("imports %s", strings.TrimSpace(line))
		}
	}
}

// TestDuImportsListingOnly: package du, its tests included, imports
// nothing but package listing and the standard library, so a program that
// sums with it links no reader, SeekParquet or Parquet code.
func TestDuImportsListingOnly(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", "-test", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", "./du").Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	for line := range strings.Lines(string(out)) {
		path, _, _ := strings.Cut(strings.TrimSpace(line), " ") // "du [du.test]" is du built for its tests
		switch path {
		case "", "github.com/varveio/swath/reader/du", "github.com/varveio/swath/reader/du.test", "github.com/varveio/swath/reader/listing":
		default:
			t.Errorf("du links %s", path)
		}
	}
}

// TestTreeImports: package tree, its tests included, imports nothing but
// golang.org/x/text's width tables and the standard library, so a program
// that draws the view links no reader, SeekParquet or Parquet code.
func TestTreeImports(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", "-test", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", "./du/tree").Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	for line := range strings.Lines(string(out)) {
		path, _, _ := strings.Cut(strings.TrimSpace(line), " ") // "tree [tree.test]" is tree built for its tests
		switch path {
		case "", "github.com/varveio/swath/reader/du/tree", "github.com/varveio/swath/reader/du/tree_test", "github.com/varveio/swath/reader/du/tree.test",
			"golang.org/x/text/width", "golang.org/x/text/transform":
		default:
			t.Errorf("tree links %s", path)
		}
	}
}
