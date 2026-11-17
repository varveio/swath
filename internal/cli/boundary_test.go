// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"go/parser"
	"go/token"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const mod = "github.com/varveio/swath"

// composition is what the command composes providers with, which no
// provider may import, directly or not: the flag package, the scheme
// registry and sink selection, the writers, and this package.
var composition = []string{
	"github.com/spf13/pflag",
	mod + "/internal/registry",
	mod + "/internal/parquetout",
	mod + "/internal/textout",
	mod + "/internal/cli",
}

// pending are the provider packages whose register.go still registers the
// provider and defines its flags, importing the composition: they are
// checked without that file. Moving it into this package removes the
// file, and the entry with it (the test fails while an entry outlives its
// file).
var pending = []string{"s3", "gcs", "azblob", "localfs"}

// pkg is one package of go list -deps: its import path, its module's path
// and its imports.
type pkg struct {
	path, module string
	imports      []string
}

// golist runs go list with args and returns its output's lines.
func golist(t *testing.T, args ...string) []string {
	t.Helper()
	out, err := exec.Command("go", append([]string{"list"}, args...)...).Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			t.Fatalf("go list %s: %v\n%s", strings.Join(args, " "), err, ee.Stderr)
		}
		t.Fatalf("go list %s: %v", strings.Join(args, " "), err)
	}
	return strings.Fields(string(out))
}

// closure is roots and every package they import, transitively (non-test
// files, this platform's build).
func closure(t *testing.T, roots []string) []pkg {
	t.Helper()
	var out []pkg
	for _, line := range golist(t, append([]string{"-deps", "-f", "{{.ImportPath}}|{{with .Module}}{{.Path}}{{end}}|{{join .Imports \",\"}}"}, roots...)...) {
		f := strings.SplitN(line, "|", 3)
		p := pkg{path: f[0], module: f[1]}
		if f[2] != "" {
			p.imports = strings.Split(f[2], ",")
		}
		out = append(out, p)
	}
	return out
}

// violations names what of the composition the closure holds, and the
// packages of swath's own module in it that import parquet-go: Parquet
// written by swath's code. parquet-go also arrives under the reader module,
// through SeekParquet (schema → reader), an edge the schema package's own
// split owns; it is not asserted here.
func violations(deps []pkg) []string {
	var v []string
	for _, p := range deps {
		if slices.Contains(composition, p.path) {
			v = append(v, p.path)
		}
		if p.module != mod {
			continue
		}
		for _, imp := range p.imports {
			if strings.HasPrefix(imp, "github.com/parquet-go/") {
				v = append(v, p.path+" imports "+imp)
			}
		}
	}
	return v
}

// importsBut is the imports of the non-test files of package path, other
// than the file named except.
func importsBut(t *testing.T, path, except string) []string {
	t.Helper()
	f := golist(t, "-f", "{{.Dir}}|{{join .GoFiles \",\"}}", path)
	dir, files, _ := strings.Cut(f[0], "|")
	if !slices.Contains(strings.Split(files, ","), except) {
		t.Fatalf("%s has no %s: drop it from pending", path, except)
	}
	var imports []string
	fset := token.NewFileSet()
	for _, name := range strings.Split(files, ",") {
		if name == except {
			continue
		}
		af, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range af.Imports {
			if p := strings.Trim(imp.Path.Value, `"`); !slices.Contains(imports, p) {
				imports = append(imports, p)
			}
		}
	}
	return imports
}

// TestProviderImportBoundary: the packages providers are built from (the
// provider contract, the range engine, the HTTP client plumbing, the token
// sources) and each provider's own code import none of the command's
// composition, directly or not, and reach no Parquet writing of swath's.
// A provider still pending is checked without its register.go.
func TestProviderImportBoundary(t *testing.T) {
	for _, p := range []string{"source", "source/keyrange", "source/objhttp", "source/tokensource"} {
		if v := violations(closure(t, []string{mod + "/internal/" + p})); len(v) > 0 {
			t.Errorf("internal/%s reaches the composition: %q", p, v)
		}
	}
	for _, p := range pending {
		path := mod + "/internal/source/" + p
		if v := violations(closure(t, importsBut(t, path, "register.go"))); len(v) > 0 {
			t.Errorf("internal/source/%s, register.go aside, reaches the composition: %q", p, v)
		}
	}
	// The check sees the composition where it is imported (not vacuous).
	v := violations(closure(t, []string{mod + "/cmd/swath"}))
	for _, want := range append(composition, mod+"/internal/parquetout imports github.com/parquet-go/parquet-go") {
		if !slices.Contains(v, want) {
			t.Errorf("the check does not see %s in cmd/swath (found %q)", want, v)
		}
	}
}
