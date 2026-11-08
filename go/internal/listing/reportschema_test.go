// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// reportPaths is every JSON path of the run report's own schema, from its
// Go types: one line per field, "a.b.c type" ("[]" marks an array's
// elements). The provider's statistics (source.stats) are the provider's,
// documented with its counters (docs/internals/counters.md).
func reportPaths() []string {
	var out []string
	var walk func(prefix string, t reflect.Type)
	walk = func(prefix string, t reflect.Type) {
		for t.Kind() == reflect.Pointer {
			t = t.Elem()
		}
		switch {
		case t == reflect.TypeOf(time.Time{}):
			out = append(out, prefix+" time")
			return
		case t.Kind() == reflect.Slice:
			if t.Elem().Kind() == reflect.Struct {
				walk(prefix+"[]", t.Elem())
				return
			}
			out = append(out, prefix+" []"+t.Elem().Kind().String())
			return
		case t.Kind() == reflect.Map:
			out = append(out, prefix+" map")
			return
		case t.Kind() == reflect.Interface:
			out = append(out, prefix+" any")
			return
		case t.Kind() != reflect.Struct:
			out = append(out, prefix+" "+t.Kind().String())
			return
		}
		for i := range t.NumField() {
			f := t.Field(i)
			name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
			if !f.IsExported() || name == "-" || name == "" {
				continue
			}
			p := name
			if prefix != "" {
				p = prefix + "." + name
			}
			walk(p, f.Type)
		}
	}
	walk("", reflect.TypeOf(Report{}))
	slices.Sort(out)
	return out
}

// TestReportSchemaGolden: the run report's schema is testdata's golden
// (report_version 3); a change to it shows in review as a diff of that
// file, with ReportVersion and go/docs/upgrade.md to change beside it. Run
// with UPDATE=1 to rewrite it.
func TestReportSchemaGolden(t *testing.T) {
	const golden = "testdata/report-schema.golden"
	got := "report_version " + strconv.Itoa(ReportVersion) + "\n" + strings.Join(reportPaths(), "\n") + "\n"
	if os.Getenv("UPDATE") != "" {
		if err := os.WriteFile(golden, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatal(err)
	}
	if string(want) != got {
		t.Fatalf("the run report's schema changed: bump ReportVersion, list the change in go/docs/upgrade.md and run UPDATE=1 go test ./internal/listing -run TestReportSchemaGolden\ngot:\n%s", got)
	}
}

// TestReportSchemaDoc: docs/cli.md's run report section names every field
// of the report's schema (by its last path element).
func TestReportSchemaDoc(t *testing.T) {
	b, err := os.ReadFile("../../docs/cli.md")
	if err != nil {
		t.Fatal(err)
	}
	doc := string(b)
	i := strings.Index(doc, "<!-- report:begin -->")
	j := strings.Index(doc, "<!-- report:end -->")
	if i < 0 || j < i {
		t.Fatal("docs/cli.md lacks the report markers")
	}
	section := doc[i:j]
	for _, p := range reportPaths() {
		path, _, _ := strings.Cut(p, " ")
		parts := strings.Split(strings.ReplaceAll(path, "[]", ""), ".")
		if leaf := parts[len(parts)-1]; !strings.Contains(section, "`"+leaf+"`") {
			t.Errorf("report field %s is not in docs/cli.md's run report section", path)
		}
	}
}
