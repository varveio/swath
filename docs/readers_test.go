// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package docs_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// upToDocs is a relative path into docs/ from below the module's root.
var upToDocs = regexp.MustCompile(`^(\.\./)+docs(/|$)`)

// evidence is the provider evidence under docs/ (docs/replay-provider-*):
// data that the fixture tier's tests read, selected with them by
// scripts/ci/go-ci-policy.py, not pages for the docs tier.
const evidence = "replay-provider-"

// TestEveryDocsReaderUsesPath: no test file outside this directory names
// docs/ itself, as "../../docs/x.md" or a filepath.Join argument "docs",
// so every test that reads a page goes through docs.Path, which requires
// the TestDocs name ci.yml go-docs selects by. A test that read docs/
// directly would pass the full suite and be skipped on a documentation
// change.
func TestEveryDocsReaderUsesPath(t *testing.T) {
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	here, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}
	str := func(e ast.Expr) (string, bool) {
		lit, ok := e.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return "", false
		}
		s, err := strconv.Unquote(lit.Value)
		return s, err == nil
	}
	files := 0
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && path != root && (path == here || d.Name() == "testdata" || d.Name() == "vendor" ||
			strings.HasPrefix(d.Name(), ".") || strings.HasPrefix(d.Name(), "_")) {
			return filepath.SkipDir
		}
		if d.IsDir() || !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		files++
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		at := func(p token.Pos) string { return rel + ":" + strconv.Itoa(fset.Position(p).Line) }
		ast.Inspect(f, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.BasicLit:
				if s, ok := str(n); ok && upToDocs.MatchString(s) && !strings.Contains(s, "docs/"+evidence) {
					t.Errorf("%s: %q reads docs/ directly; use docs.Path from a TestDocs test", at(n.Pos()), s)
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
					t.Errorf("%s: joins \"docs\"; use docs.Path from a TestDocs test", at(arg.Pos()))
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if files == 0 {
		t.Fatal("no test files checked")
	}
}
