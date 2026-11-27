// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"os/exec"
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

// providers are the provider packages under internal/source. Each is
// registered here, by this package's init, and imports none of it.
var providers = []string{"s3", "gcs", "azblob", "localfs", "vercel"}

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

// TestProviderImportBoundary: the packages providers are built from (the
// provider contract, the range engine, the HTTP client plumbing, the token
// sources) and each provider import none of the command's composition,
// directly or not, and reach parquet-go only through the reader edge. A
// provider importing none of it cannot register itself either: this
// package registers it.
func TestProviderImportBoundary(t *testing.T) {
	var reader bool
	for _, p := range []string{"source", "source/keyrange", "source/objhttp", "source/tokensource"} {
		if v := violations(closure(t, []string{mod + "/internal/" + p}), &reader); len(v) > 0 {
			t.Errorf("internal/%s reaches the composition: %q", p, v)
		}
	}
	for _, p := range providers {
		if v := violations(closure(t, []string{mod + "/internal/source/" + p}), &reader); len(v) > 0 {
			t.Errorf("internal/source/%s reaches the composition: %q", p, v)
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
