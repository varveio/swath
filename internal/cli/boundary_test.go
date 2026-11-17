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

// providers are the provider packages under internal/source, each checked
// whole once it is no longer pending.
var providers = []string{"s3", "gcs", "azblob", "localfs"}

// pending are the providers whose register.go still registers the
// provider and defines its flags, importing the composition: they are
// checked without that file. Moving it into this package removes the
// file, and the entry with it (the test fails while an entry outlives its
// file).
var pending = []string{"s3", "gcs", "azblob", "localfs"}

// readerEdge are the packages that may import parquet-go in a provider's
// closure: SeekParquet's reader, reached through schema → reader, an edge
// the schema package's own split removes. Parquet-go imported by any other
// package is a violation; once no closure reaches the reader the test
// fails until this list is emptied.
var readerEdge = []string{
	"github.com/varveio/seekparquet/go",
	"github.com/varveio/seekparquet/go/internal/thriftcheck",
}

// pkg is one package of go list -deps: its import path and its imports.
type pkg struct {
	path    string
	imports []string
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
	for _, line := range golist(t, append([]string{"-deps", "-f", "{{.ImportPath}}|{{join .Imports \",\"}}"}, roots...)...) {
		path, imports, _ := strings.Cut(line, "|")
		p := pkg{path: path}
		if imports != "" {
			p.imports = strings.Split(imports, ",")
		}
		out = append(out, p)
	}
	return out
}

// violations names what of the composition the closure holds, and the
// packages in it that import parquet-go, parquet-go's own and the reader
// edge aside; it sets *reader when the closure reaches parquet-go through
// that edge.
func violations(deps []pkg, reader *bool) []string {
	const parquetGo = "github.com/parquet-go/"
	var v []string
	for _, p := range deps {
		if slices.Contains(composition, p.path) {
			v = append(v, p.path)
		}
		if strings.HasPrefix(p.path, parquetGo) {
			continue
		}
		for _, imp := range p.imports {
			if !strings.HasPrefix(imp, parquetGo) {
				continue
			}
			if slices.Contains(readerEdge, p.path) {
				*reader = true
				continue
			}
			v = append(v, p.path+" imports "+imp)
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
// sources) and each provider import none of the command's composition,
// directly or not, and reach parquet-go only through the reader edge. A
// provider still pending is checked without its register.go, every other
// one whole.
func TestProviderImportBoundary(t *testing.T) {
	var reader bool
	for _, p := range []string{"source", "source/keyrange", "source/objhttp", "source/tokensource"} {
		if v := violations(closure(t, []string{mod + "/internal/" + p}), &reader); len(v) > 0 {
			t.Errorf("internal/%s reaches the composition: %q", p, v)
		}
	}
	for _, p := range providers {
		path := mod + "/internal/source/" + p
		name := "internal/source/" + p
		roots := []string{path}
		if slices.Contains(pending, p) {
			name += ", register.go aside,"
			roots = importsBut(t, path, "register.go")
		}
		if v := violations(closure(t, roots), &reader); len(v) > 0 {
			t.Errorf("%s reaches the composition: %q", name, v)
		}
	}
	if !reader && len(readerEdge) > 0 {
		t.Error("no provider reaches parquet-go through the reader edge any more: empty readerEdge")
	}

	// The check sees the composition where it is imported (not vacuous).
	var ignored bool
	v := violations(closure(t, []string{mod + "/cmd/swath"}), &ignored)
	for _, want := range append(composition, mod+"/internal/parquetout imports github.com/parquet-go/parquet-go") {
		if !slices.Contains(v, want) {
			t.Errorf("the check does not see %s in cmd/swath (found %q)", want, v)
		}
	}
}
