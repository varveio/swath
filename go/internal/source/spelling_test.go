// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package source_test

import (
	"flag"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/registry"
	_ "github.com/varveio/swath/go/internal/source/azblob"
	_ "github.com/varveio/swath/go/internal/source/gcs"
	_ "github.com/varveio/swath/go/internal/source/localfs"
	_ "github.com/varveio/swath/go/internal/source/s3"
)

// spellings are what the command must not spell: the registered providers'
// schemes, their own flags (not their shared groups', which are every
// object store's) and the prefixes that are a provider's flag namespace.
type spellings struct {
	schemes  []string
	flags    map[string]bool
	prefixes []string
}

// providerSpellings collects the spellings from the registered factories.
// A prefix (the name up to its first '-', "gcs-") is a provider's
// namespace when two or more of its own flags start with it and no shared
// group's flag, other provider's flag or command flag (command) does.
func providerSpellings(command []string) spellings {
	sp := spellings{flags: map[string]bool{}}
	byProvider := map[string][]string{}
	groupFlags := map[string]bool{}
	for _, f := range registry.Factories() {
		if f.Scheme != "" {
			sp.schemes = append(sp.schemes, f.Scheme)
		}
		all, groups := flag.NewFlagSet("", flag.ContinueOnError), flag.NewFlagSet("", flag.ContinueOnError)
		f.RegisterFlags(all, true)
		for _, g := range f.Shared {
			g.Flags(groups, true)
		}
		groups.VisitAll(func(fl *flag.Flag) { groupFlags[fl.Name] = true })
		all.VisitAll(func(fl *flag.Flag) {
			if groups.Lookup(fl.Name) == nil {
				sp.flags[fl.Name] = true
				byProvider[f.Scheme] = append(byProvider[f.Scheme], fl.Name)
			}
		})
	}
	owners := map[string]map[string]int{} // prefix: provider: flags
	note := func(name, owner string) {
		if p, _, ok := strings.Cut(name, "-"); ok {
			if owners[p+"-"] == nil {
				owners[p+"-"] = map[string]int{}
			}
			owners[p+"-"][owner]++
		}
	}
	for scheme, names := range byProvider {
		for _, n := range names {
			note(n, scheme)
		}
	}
	for n := range groupFlags {
		note(n, "shared group")
	}
	for _, n := range command {
		note(n, "shared group")
	}
	for p, by := range owners {
		if len(by) == 1 {
			for owner, n := range by {
				if owner != "shared group" && n >= 2 {
					sp.prefixes = append(sp.prefixes, p)
				}
			}
		}
	}
	slices.Sort(sp.prefixes)
	return sp
}

// commandFlags are the flag names the files register: the name argument
// of flag.FlagSet's definition methods (f.BoolVar(&v, "name", ...),
// f.Func("name", ...)) when it is a string literal.
func commandFlags(files []*ast.File) []string {
	var names []string
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			c, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := c.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			m := sel.Sel.Name
			i := 0
			switch {
			case strings.HasSuffix(m, "Var"):
				i = 1
			case m == "Func" || m == "BoolFunc" || m == "String" || m == "Bool" || m == "Int" || m == "Int64" || m == "Duration" || m == "Float64" || m == "Uint":
			default:
				return true
			}
			if i < len(c.Args) {
				if lit, ok := c.Args[i].(*ast.BasicLit); ok && lit.Kind == token.STRING {
					if v, err := strconv.Unquote(lit.Value); err == nil {
						names = append(names, v)
					}
				}
			}
			return true
		})
	}
	return names
}

// scan reports, as "file:line: why", every string the files spell that
// names a provider. Each maximal constant string expression is folded
// first: a '+' chain of literals and of the files' string constants. It
// counts when it is a provider's scheme, or holds one as "scheme:" (a root
// or a prefix test); holds a provider's flag typed with dashes ("--profile");
// holds a word starting with a provider's flag namespace ("gcs-"); or is a
// provider's flag name exactly ("profile"), except as an argument of a
// call into os, path or path/filepath, where it is a file name.
func scan(fset *token.FileSet, files []*ast.File, sp spellings) []string {
	var names []string
	for n := range sp.flags {
		names = append(names, regexp.QuoteMeta(n))
	}
	slices.Sort(names)
	quote := func(ss []string) string {
		q := make([]string, len(ss))
		for i, s := range ss {
			q[i] = regexp.QuoteMeta(s)
		}
		return strings.Join(q, "|")
	}
	schemeRe := regexp.MustCompile(`(^|[^A-Za-z0-9])(` + quote(sp.schemes) + `):`)
	flagRe := regexp.MustCompile(`(^|[^\w-])--?(` + strings.Join(names, "|") + `)($|[^\w-])`)
	prefixRe := regexp.MustCompile(`(^|[^\w-])-{0,2}(` + quote(sp.prefixes) + `)`)

	consts := map[string]ast.Expr{}
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			if d, ok := n.(*ast.GenDecl); ok && d.Tok == token.CONST {
				for _, spec := range d.Specs {
					vs := spec.(*ast.ValueSpec)
					for i, id := range vs.Names {
						if i < len(vs.Values) {
							consts[id.Name] = vs.Values[i]
						}
					}
				}
			}
			return true
		})
	}
	var fold func(e ast.Expr, depth int) (string, bool)
	fold = func(e ast.Expr, depth int) (string, bool) {
		if depth > 16 {
			return "", false
		}
		switch e := e.(type) {
		case *ast.BasicLit:
			if e.Kind != token.STRING {
				return "", false
			}
			v, err := strconv.Unquote(e.Value)
			return v, err == nil
		case *ast.ParenExpr:
			return fold(e.X, depth+1)
		case *ast.Ident:
			if c, ok := consts[e.Name]; ok {
				return fold(c, depth+1)
			}
		case *ast.BinaryExpr:
			if e.Op == token.ADD {
				x, okx := fold(e.X, depth+1)
				y, oky := fold(e.Y, depth+1)
				return x + y, okx && oky
			}
		}
		return "", false
	}
	var out []string
	for _, f := range files {
		fileNames := map[ast.Expr]bool{} // arguments of os, path and path/filepath calls
		pathPkgs := map[string]bool{}
		for _, imp := range f.Imports {
			p, _ := strconv.Unquote(imp.Path.Value)
			if p == "os" || p == "path" || p == "path/filepath" {
				name := p[strings.LastIndex(p, "/")+1:]
				if imp.Name != nil {
					name = imp.Name.Name
				}
				pathPkgs[name] = true
			}
		}
		ast.Inspect(f, func(n ast.Node) bool {
			if c, ok := n.(*ast.CallExpr); ok {
				if sel, ok := c.Fun.(*ast.SelectorExpr); ok {
					if id, ok := sel.X.(*ast.Ident); ok && pathPkgs[id.Name] {
						for _, a := range c.Args {
							fileNames[a] = true
						}
					}
				}
			}
			return true
		})
		ast.Inspect(f, func(n ast.Node) bool {
			if _, ok := n.(*ast.ImportSpec); ok {
				return false // the blank imports that register providers
			}
			e, ok := n.(ast.Expr)
			if !ok {
				return true
			}
			if _, ok := e.(*ast.Ident); ok {
				return true // a constant is checked where it is declared
			}
			v, ok := fold(e, 0)
			if !ok {
				return true
			}
			at := fset.Position(e.Pos())
			report := func(why string) {
				out = append(out, filepath.Base(at.Filename)+":"+strconv.Itoa(at.Line)+": "+strconv.Quote(v)+" "+why)
			}
			switch {
			case slices.Contains(sp.schemes, v) || schemeRe.MatchString(v):
				report("names a provider's scheme")
			case flagRe.MatchString(v):
				report("names a provider's flag")
			case prefixRe.MatchString(v):
				report("spells a provider's flag namespace")
			case sp.flags[v] && !fileNames[e]:
				report("is a provider's flag name (if it is not the flag, name it through a constant of the package that owns it)")
			}
			return false // the maximal expression, folded, is checked once
		})
	}
	return out
}

// TestCommandNamesNoProvider is a tripwire on spellings, not a proof: it
// fails when cmd/swath's non-test source spells a registered provider's
// scheme, own flag or flag namespace (scan), in a string literal or a
// constant string expression. Values computed at run time, or spelled
// some other way, pass it. What the command needs of a provider, it reads
// from the registered factories (registry.Factory).
func TestCommandNamesNoProvider(t *testing.T) {
	files, err := filepath.Glob(filepath.Join(moduleDir(t), "cmd", "swath", "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	var parsed []*ast.File
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		af, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		parsed = append(parsed, af)
	}
	command := commandFlags(parsed)
	if !slices.Contains(command, "no-spill") || !slices.Contains(command, "workers") {
		t.Fatalf("the command's flags not collected (%q)", command)
	}
	sp := providerSpellings(command)
	for _, want := range []string{"s3", "gs", "az"} {
		if !slices.Contains(sp.schemes, want) {
			t.Fatalf("provider %s:// not registered (schemes %q)", want, sp.schemes)
		}
	}
	for _, want := range []string{"profile", "gcs-credentials", "azblob-sas"} {
		if !sp.flags[want] {
			t.Fatalf("provider flag --%s not collected", want)
		}
	}
	for _, want := range []string{"azblob-", "gcs-"} {
		if !slices.Contains(sp.prefixes, want) {
			t.Fatalf("flag namespace %s not derived (%q)", want, sp.prefixes)
		}
	}
	for _, f := range scan(fset, parsed, sp) {
		t.Error(f)
	}
}

// TestScanSpellings: the tripwire on adversarial spellings, folded
// constants among them; a provider's single-word flag name as a file name
// passes, any other bare use does not.
func TestScanSpellings(t *testing.T) {
	sp := providerSpellings([]string{"no-spill"})
	for _, c := range []struct {
		src     string
		flagged bool
	}{
		{`const scheme = "s" + "3"`, true},
		{`const a, b = "s", "3"; const scheme = a + b`, true},
		{`var _ = strings.HasPrefix(root, "s3:")`, true},
		{`var _ = "gs://" + bucket`, true},
		{`var _ = "--" + "gcs-" + name`, true},
		{`var _ = fs.Lookup("gcs-" + "credentials")`, true},
		{`const ns = "azblob-"`, true},
		{`var _ = errors.New("give --profile")`, true},
		{`var _ = []string{"profile"}`, true},
		{`var _ = filepath.Join(dir, "metadata")`, false},
		{`var _ = os.Remove("metadata")`, false},
		{`var _ = m["metadata"]`, true},
		{`var _ = errors.New("flags: bad args: x")`, false},
		{`var _ = "--endpoint"`, false}, // a shared group's flag
		{`var _ = "--request-timeout"`, false},
		{`var _ = "one region"`, false},
	} {
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, "x.go", "package main\nimport (\"os\"; \"path/filepath\")\nvar _, _ = os.Args, filepath.Base\n"+c.src+"\n", 0)
		if err != nil {
			t.Fatalf("%s: %v", c.src, err)
		}
		got := scan(fset, []*ast.File{f}, sp)
		if (len(got) > 0) != c.flagged {
			t.Errorf("%s: findings %q, want flagged %v", c.src, got, c.flagged)
		}
	}
}
