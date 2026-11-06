// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package source_test

import (
	"go/build"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/source"
)

const mod = "github.com/varveio/swath/go"

// moduleDir is the module root (this file is in internal/source).
func moduleDir(t *testing.T) string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("no caller")
	}
	return filepath.Join(filepath.Dir(file), "..", "..")
}

// deps returns the module packages pkg imports, transitively (non-test
// files, this platform's build).
func deps(t *testing.T, root, pkg string) map[string]bool {
	seen := map[string]bool{}
	var walk func(string)
	walk = func(p string) {
		bp, err := build.Import(p, root, 0)
		if err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		for _, imp := range bp.Imports {
			if strings.HasPrefix(imp, mod+"/") && !seen[imp] {
				seen[imp] = true
				walk(imp)
			}
		}
	}
	walk(pkg)
	return seen
}

// TestImportBoundary: the pipeline sees providers only through packages
// source and registry. listing, order, parquetout, dataset, schema, journal
// and registry import nothing under internal/source/ (directly or not),
// internal/source imports no provider, the command imports providers only
// to register them, and never test support (scalefake).
func TestImportBoundary(t *testing.T) {
	root := moduleDir(t)
	providers := mod + "/internal/source/"
	for _, p := range []string{"listing", "order", "parquetout", "dataset", "schema", "journal", "registry", "source"} {
		for d := range deps(t, root, mod+"/internal/"+p) {
			if strings.HasPrefix(d, providers) {
				t.Errorf("internal/%s depends on provider package %s", p, strings.TrimPrefix(d, mod+"/"))
			}
		}
	}
	// The walk sees providers where they are imported (not vacuous).
	cmd := deps(t, root, mod+"/cmd/swath")
	if !cmd[providers+"s3"] {
		t.Fatal("the dependency walk does not see cmd/swath's providers")
	}
	// Test and bench support stays out of the binary.
	for _, p := range []string{"scalefake"} {
		if cmd[providers+p] {
			t.Errorf("cmd/swath depends on test support %s", p)
		}
	}
	fset := token.NewFileSet()
	files, err := filepath.Glob(filepath.Join(root, "cmd", "swath", "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		af, err := parser.ParseFile(fset, f, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range af.Imports {
			path := strings.Trim(imp.Path.Value, `"`)
			if strings.HasPrefix(path, providers) && (imp.Name == nil || imp.Name.Name != "_") {
				t.Errorf("%s imports provider %s for more than registration", filepath.Base(f), path)
			}
		}
	}
}

// TestRangeEngineApplicability: the range engine applies only with no rank
// oracle, a fabricable seek and byte-ordered pages.
func TestRangeEngineApplicability(t *testing.T) {
	for _, c := range []struct {
		caps source.Capabilities
		want bool
	}{
		{source.Capabilities{Seek: source.SeekFabricable, Order: source.OrderBytes}, true},
		{source.Capabilities{Seek: source.SeekFabricable, Order: source.OrderBytes, RankOracle: true}, false},
		{source.Capabilities{Seek: source.SeekFabricable, Order: source.OrderOther}, false}, // UTF-16 order
		{source.Capabilities{Seek: source.SeekFabricable, Order: source.OrderNone}, false},
		{source.Capabilities{Seek: source.SeekFabricable}, false},
		{source.Capabilities{Seek: source.SeekTokenOnly}, false},
		{source.Capabilities{Seek: source.SeekNone}, false},
		{source.Capabilities{}, false},
	} {
		if got := c.caps.RangeEngine(); got != c.want {
			t.Errorf("%+v: %v, want %v", c.caps, got, c.want)
		}
	}
}
