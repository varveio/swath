// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package docs_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// docsImport is the docs package, which only _test.go files import.
const docsImport = "github.com/varveio/swath/docs"

// evidence is the provider evidence under docs/ (docs/replay-provider-*):
// data that the fixture tier's tests read, selected with them by
// scripts/ci/go-ci-policy.py, not pages for the docs tier.
const evidence = "replay-provider-"

// namesDocs reports whether a string literal is a path under docs/: one
// word (no space, so a message about a page is not one) that starts with
// docs/, has a docs/ element or ends in one, and is not a URL or the
// provider evidence.
func namesDocs(s string) bool {
	if strings.ContainsAny(s, " \t\n") || strings.Contains(s, "://") || strings.Contains(s, "docs/"+evidence) {
		return false
	}
	return strings.HasPrefix(s, "docs/") || strings.Contains(s, "/docs/") || strings.HasSuffix(s, "/docs")
}

// violations is what the Go file at rel (slash-separated, from the root)
// does that would keep a test that reads docs/ out of ci.yml go-docs: a
// path under docs/ in a string literal or a path element "docs" passed to
// a call (the test would read it without docs.Path), or, outside a
// _test.go file, an import of the docs package (a helper that calls
// docs.Path would hide its callers from go-docs' discovery).
func violations(rel string, src []byte) ([]string, error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, rel, src, parser.SkipObjectResolution)
	if err != nil {
		return nil, err
	}
	var out []string
	at := func(p token.Pos) string { return rel + ":" + strconv.Itoa(fset.Position(p).Line) }
	str := func(e ast.Expr) (string, bool) {
		lit, ok := e.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return "", false
		}
		s, err := strconv.Unquote(lit.Value)
		return s, err == nil
	}
	for _, imp := range f.Imports {
		if p, _ := strconv.Unquote(imp.Path.Value); p == docsImport && !strings.HasSuffix(rel, "_test.go") {
			out = append(out, at(imp.Pos())+": imports the docs package outside a _test.go file")
		}
	}
	ast.Inspect(f, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.ImportSpec:
			return false
		case *ast.BasicLit:
			if s, ok := str(n); ok && namesDocs(s) {
				out = append(out, at(n.Pos())+": "+strconv.Quote(s)+" names a path under docs/")
			}
		case *ast.CallExpr:
			for i, arg := range n.Args {
				if s, ok := str(arg); !ok || s != "docs" {
					continue
				}
				if i+1 < len(n.Args) {
					if next, ok := str(n.Args[i+1]); ok && strings.HasPrefix(next, evidence) {
						continue
					}
				}
				out = append(out, at(arg.Pos())+": passes the path element \"docs\"")
			}
		}
		return true
	})
	return out, nil
}

// TestEveryDocsReaderUsesPath: no Go file outside this directory names a
// path under docs/ or imports the docs package from a regular file, so
// every test that reads a page goes through docs.Path, which requires the
// TestDocs name ci.yml go-docs selects by and refuses the pages no Go test
// may read (docs.Unread). Such a test would pass the full suite and be
// skipped on a documentation change.
func TestEveryDocsReaderUsesPath(t *testing.T) {
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	here, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}
	files := 0
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && path != root && (path == here || d.Name() == "testdata" || d.Name() == "vendor" ||
			d.Name() == "node_modules" || strings.HasPrefix(d.Name(), ".") || strings.HasPrefix(d.Name(), "_")) {
			return filepath.SkipDir
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		files++
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		found, err := violations(filepath.ToSlash(rel), src)
		if err != nil {
			return err
		}
		for _, v := range found {
			t.Errorf("%s; read pages through docs.Path from a TestDocs test", v)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if files == 0 {
		t.Fatal("no Go files checked")
	}
}

// TestViolations: the guard above on snippets it must accept and reject.
func TestViolations(t *testing.T) {
	for _, c := range []struct {
		name, rel, body string
		bad             bool
	}{
		{"docs.Path", "cmd/x/a_test.go", `func TestDocsA(t *testing.T) { os.ReadFile(docs.Path(t, "commands/swath.md")) }`, false},
		{"a message about a page", "cmd/x/a_test.go", `func f(t *testing.T) { t.Errorf("docs/commands/swath.md is stale") }`, false},
		{"a format naming a page", "cmd/x/a_test.go", `func f(t *testing.T, n string) { t.Fatalf("docs/%s lacks the markers", n) }`, false},
		{"a URL", "cmd/x/a.go", `const u = "https://github.com/varveio/swath/blob/main/docs/install.md"`, false},
		{"evidence as one path", "internal/x/a_test.go", `const a = "../../../docs/replay-provider-evidence.tar.gz"`, false},
		{"evidence joined", "internal/x/a_test.go", `func f() { filepath.Join(repo, "docs", "replay-provider-expectations.json") }`, false},
		{"evidence from the root", "bench/x/main.go", `func f() { filepath.Join("docs", "replay-provider-evidence.tar.gz") }`, false},
		{"another directory", "internal/x/a_test.go", `func f() { os.ReadFile("../../bench/x/script.sh") }`, false},
		{"a word containing docs", "internal/x/a_test.go", `var a = []string{"mydocs/x", "docsy", "docs"}`, false},
		{"up to a page", "cmd/x/a_test.go", `func f() { os.ReadFile("../../docs/troubleshooting.md") }`, true},
		{"up to the directory", "cmd/x/a_test.go", `func f() { os.ReadFile("../../docs" + "/x.md") }`, true},
		{"joined with a page path", "cmd/x/a_test.go", `func f() { os.ReadFile(filepath.Join("..", "docs/reference/page.md")) }`, true},
		{"joined with ..", "cmd/x/a_test.go", `func f() { filepath.Join("..", "..", "docs", "commands") }`, true},
		{"joined from a root", "cmd/x/a_test.go", `func f() { filepath.Join(root, "docs") }`, true},
		{"from the root", "cmd/x/a_test.go", `var a = map[string]string{"docs/readme.md": "x"}`, true},
		{"an assets file", "cmd/x/a_test.go", `func f() { os.ReadFile("../../docs/assets/demo.gif") }`, true},
		{"an ops page", "cmd/x/a_test.go", `func f() { filepath.Join("..", "..", "docs/ops/dev/CI.md") }`, true},
		{"a regular file", "cmd/x/a.go", `func f() { os.ReadFile("../../docs/x.md") }`, true},
		{"a helper importing docs", "internal/x/helper.go", `import "github.com/varveio/swath/docs"; var _ = docs.Path`, true},
		{"a test importing docs", "internal/x/a_test.go", `import "github.com/varveio/swath/docs"; var _ = docs.Path`, false},
	} {
		got, err := violations(c.rel, []byte("package x\n"+c.body+"\n"))
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if (len(got) > 0) != c.bad {
			t.Errorf("%s: violations %q, want any: %v", c.name, got, c.bad)
		}
	}
}
