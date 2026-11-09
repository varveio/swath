// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package source_test

import (
	"flag"
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/source"
	_ "github.com/varveio/swath/go/internal/source/azblob"
	_ "github.com/varveio/swath/go/internal/source/gcs"
	_ "github.com/varveio/swath/go/internal/source/localfs"
	_ "github.com/varveio/swath/go/internal/source/s3"
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
// to register them, and never test support (scalefake, shapes).
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
	for _, p := range []string{providers + "scalefake", mod + "/internal/shapes"} {
		if cmd[p] {
			t.Errorf("cmd/swath depends on test support %s", strings.TrimPrefix(p, mod+"/"))
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

// TestCommandNamesNoProvider: the command knows no provider by name. No
// string in its non-test code is a provider's scheme or root form, or one
// of a provider's own flags (the shared groups' flags are every object
// store's, and the command may name them): what the command needs of a
// provider, it reads from the registered factories (registry.Factory).
func TestCommandNamesNoProvider(t *testing.T) {
	var schemes []string
	own := map[string]bool{}
	for _, f := range registry.Factories() {
		if f.Scheme != "" {
			schemes = append(schemes, f.Scheme)
		}
		all, groups := flag.NewFlagSet("", flag.ContinueOnError), flag.NewFlagSet("", flag.ContinueOnError)
		f.RegisterFlags(all, true)
		for _, g := range f.Shared {
			g.Flags(groups, true)
		}
		all.VisitAll(func(fl *flag.Flag) {
			if groups.Lookup(fl.Name) == nil {
				own[fl.Name] = true
			}
		})
	}
	// Not vacuous: the providers registered, with their prefixed flags.
	for _, want := range []string{"s3", "gs", "az"} {
		if !strings.Contains(strings.Join(schemes, " "), want) {
			t.Fatalf("provider %s:// not registered (schemes %q)", want, schemes)
		}
	}
	for _, want := range []string{"profile", "gcs-credentials", "azblob-sas"} {
		if !own[want] {
			t.Fatalf("provider flag --%s not collected", want)
		}
	}
	var names []string
	for n := range own {
		names = append(names, regexp.QuoteMeta(n))
	}
	// A provider flag as a whole string ("profile") or typed in one
	// ("--profile"); a scheme as a whole string ("s3") or in a root
	// ("s3://").
	flagRe := regexp.MustCompile(`(^|[^\w-])-{1,2}(` + strings.Join(names, "|") + `)($|[^\w-])`)
	files, err := filepath.Glob(filepath.Join(moduleDir(t), "cmd", "swath", "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		af, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(af, func(n ast.Node) bool {
			if _, ok := n.(*ast.ImportSpec); ok {
				return false // the blank imports that register providers
			}
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			v, err := strconv.Unquote(lit.Value)
			if err != nil {
				t.Fatal(err)
			}
			where := fset.Position(lit.Pos())
			if own[v] || flagRe.MatchString(v) {
				t.Errorf("%s:%d: %q names a provider's flag", filepath.Base(where.Filename), where.Line, v)
			}
			for _, sc := range schemes {
				if v == sc || strings.Contains(v, sc+"://") {
					t.Errorf("%s:%d: %q names provider %s://", filepath.Base(where.Filename), where.Line, v, sc)
				}
			}
			return true
		})
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
