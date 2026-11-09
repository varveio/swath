// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package source_test

import (
	"flag"
	"go/ast"
	"go/constant"
	"go/parser"
	"go/token"
	"go/types"
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

// scan reports, as "file:line: why", every constant string expression in
// files that the type checker evaluates and that names a provider. Files
// are one package, type-checked with its imports as empty placeholders and
// its errors ignored: the package's own constants evaluate with Go's
// scoping (shadowing, iota, implicitly repeated specs, other files), and
// so do conversions of constant numbers or runes to strings; nothing built
// at run time does (a variable, fmt.Sprintf's result, a function's return).
// Each maximal constant string expression is checked once, on its value:
// it counts when it is a provider's scheme, or holds one as "scheme:" (a
// root or a prefix test); holds a provider's flag typed with dashes
// ("--profile"); holds a word starting with a provider's flag namespace
// ("gcs-"); or is a provider's flag name exactly ("profile"). The last two
// do not count for a path element passed directly to filepath.Join or
// path.Join (a file named metadata, a directory gcs-cache).
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

	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Uses: map[*ast.Ident]types.Object{}}
	conf := types.Config{Importer: placeholders{}, Error: func(error) {}}
	conf.Check(files[0].Name.Name, fset, files, info) // errors are the placeholders' missing members
	str := func(e ast.Expr) (string, bool) {
		tv, ok := info.Types[e]
		if !ok || tv.Value == nil || tv.Value.Kind() != constant.String {
			return "", false
		}
		return constant.StringVal(tv.Value), true
	}
	var out []string
	for _, f := range files {
		pathElements := map[ast.Expr]bool{} // direct arguments of filepath.Join and path.Join
		ast.Inspect(f, func(n ast.Node) bool {
			c, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if sel, ok := c.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Join" {
				if id, ok := sel.X.(*ast.Ident); ok {
					if pn, ok := info.Uses[id].(*types.PkgName); ok && (pn.Imported().Path() == "path/filepath" || pn.Imported().Path() == "path") {
						for _, a := range c.Args {
							pathElements[a] = true
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
			v, ok := str(e)
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
			case pathElements[e]:
			case prefixRe.MatchString(v):
				report("spells a provider's flag namespace")
			case sp.flags[v]:
				report("is a provider's flag name (if it is not the flag, name it through a constant of the package that owns it)")
			}
			return false // the maximal expression, evaluated, is checked once
		})
	}
	return out
}

// placeholders imports every path as an empty, complete package: the
// files' own constants evaluate without loading their dependencies.
type placeholders struct{}

func (placeholders) Import(path string) (*types.Package, error) {
	p := types.NewPackage(path, path[strings.LastIndex(path, "/")+1:])
	p.MarkComplete()
	return p, nil
}

// TestCommandNamesNoProvider is a tripwire on spellings in the command's
// source, not a proof: it fails when a constant string expression the type
// checker evaluates in cmd/swath's non-test files spells a registered
// provider's scheme, own flag or flag namespace (scan). A value built at
// run time (a variable, fmt.Sprintf's result) passes it. What the command
// needs of a provider, it reads from the registered factories
// (registry.Factory).
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
	if len(sp.prefixes) == 0 {
		t.Fatal("no provider flag namespace derived: a namespace needs two flags of one provider sharing a prefix that no command, group or other provider flag starts with; if no provider has one any more, drop this check")
	}
	for _, f := range scan(fset, parsed, sp) {
		t.Error(f)
	}
}

// TestScanSpellings pins scan on adversarial spellings: constants folded
// with Go's scoping, across files and through implicit specs; a parameter
// is no constant; a provider flag name or namespace passes only as a path
// element given directly to filepath.Join or path.Join.
func TestScanSpellings(t *testing.T) {
	sp := providerSpellings([]string{"no-spill"})
	for _, c := range []struct {
		files   []string
		flagged bool
	}{
		{[]string{`const scheme = "s" + "3"`}, true},
		{[]string{`const a, b = "s", "3"; const scheme = a + b`}, true},
		{[]string{`const p = "s"; var scheme = p + "3"; func f() { const p = "x"; _ = p }`}, true}, // a local constant shadows only locally
		{[]string{`const p = "s"; func f(p string) string { return p + "3" }`}, false},             // a parameter is no constant
		{[]string{`const a = "s"`, `var scheme = a + "3"`}, true},                                  // across files
		{[]string{`const (a = "s"; b); var scheme = b + "3"`}, true},                               // an implicitly repeated spec
		{[]string{`const (x = "s" + string(rune('3' + iota)))`}, true},                             // iota, a rune conversion
		{[]string{`var _ = strings.HasPrefix(root, "s3:")`}, true},
		{[]string{`var _ = "gs://" + bucket`}, true},
		{[]string{`var _ = "--" + "gcs-" + name`}, true},
		{[]string{`var _ = fs.Lookup("gcs-" + "credentials")`}, true},
		{[]string{`const ns = "azblob-"`}, true},
		{[]string{`var _ = errors.New("give --profile")`}, true},
		{[]string{`var _ = []string{"profile"}`}, true},
		{[]string{`var _ = m["metadata"]`}, true},
		{[]string{`var _ = fs.Lookup(filepath.Base("profile"))`}, true},
		{[]string{`var _ = os.Getenv("profile")`}, true},
		{[]string{`var _ = os.Remove("metadata")`}, true},
		{[]string{`var _ = filepath.Join(dir, "--profile")`}, true},
		{[]string{`var _ = filepath.Join(dir, "metadata")`}, false},
		{[]string{`var _ = path.Join("a", "metadata")`}, false},
		{[]string{`var _ = filepath.Join(dir, "gcs-cache")`}, false},
		{[]string{`var _ = fmt.Sprintf("s%s", "3")`}, false}, // run time: out of reach
		{[]string{`var _ = errors.New("flags: bad args: x")`}, false},
		{[]string{`var _ = "--endpoint"`}, false}, // a shared group's flag
		{[]string{`var _ = "--request-timeout"`}, false},
		{[]string{`var _ = "one region"`}, false},
	} {
		fset := token.NewFileSet()
		var files []*ast.File
		for i, src := range c.files {
			f, err := parser.ParseFile(fset, "x"+strconv.Itoa(i)+".go", "package main\nimport (\"fmt\"; \"os\"; \"path\"; \"path/filepath\")\nvar _, _, _, _ = fmt.Sprint, os.Args, path.Base, filepath.Base\n"+src+"\n", 0)
			if err != nil {
				t.Fatalf("%s: %v", src, err)
			}
			files = append(files, f)
		}
		got := scan(fset, files, sp)
		if (len(got) > 0) != c.flagged {
			t.Errorf("%q: findings %q, want flagged %v", c.files, got, c.flagged)
		}
	}
}
