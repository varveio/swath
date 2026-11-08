// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
)

// campaignCase is a case as the campaign's matrix dumps it.
type campaignCase struct {
	ID      string      `json:"id"`
	Kind    string      `json:"kind"`
	Query   string      `json:"query"`
	Headers [][2]string `json:"headers"`
	Token   *struct {
		CaseID   *string `json:"case_id"`
		Page     int     `json:"page"`
		Mutation *string `json:"mutation"`
	} `json:"token"`
}

const testRunPrefix = "probe/r20260101t000000z-00000000/"

// campaignCases runs the campaign's own matrix code (python3, the
// standard library only) and returns its cases for provider.
func campaignCases(t *testing.T, module, class string) []campaignCase {
	t.Helper()
	py, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not found: the campaign's matrix cannot be read")
	}
	_, file, _, _ := runtime.Caller(0)
	dir := filepath.Join(filepath.Dir(file), "..", "..", "..", "scripts", "provider-conformance")
	script := fmt.Sprintf(`import json, sys
sys.path.insert(0, %q)
import matrix, %s
p = %s.%s.__new__(%s.%s)
print(json.dumps([matrix.case_record(c) for c in p.cases(%q, "full")]))`, dir, module, module, class, module, class, testRunPrefix)
	out, err := exec.Command(py, "-I", "-c", script).Output()
	if err != nil {
		var stderr []byte
		if ee, ok := err.(*exec.ExitError); ok {
			stderr = ee.Stderr
		}
		t.Fatalf("reading the campaign matrix: %v\n%s", err, stderr)
	}
	var cases []campaignCase
	if err := json.Unmarshal(out, &cases); err != nil {
		t.Fatal(err)
	}
	return cases
}

// TestProviderMatrixMatchesTheCampaign: bound to the campaign corpus,
// every case renders the campaign's query, kind, headers and token
// source exactly, and the two matrices hold the same cases.
func TestProviderMatrixMatchesTheCampaign(t *testing.T) {
	for _, m := range []struct {
		module, class string
		cases         []providerCase
	}{{"s3_live", "S3Provider", s3ProviderCases()}, {"gcs_live", "GcsProvider", gcsProviderCases()}} {
		want := campaignCases(t, m.module, m.class)
		var ids, wantIDs []string
		for _, c := range m.cases {
			ids = append(ids, c.ID)
		}
		for _, c := range want {
			wantIDs = append(wantIDs, c.ID)
		}
		if !slices.Equal(slices.Sorted(slices.Values(ids)), slices.Sorted(slices.Values(wantIDs))) || len(ids) != len(wantIDs) {
			t.Errorf("%s: cases differ:\n go %v\n campaign %v", m.module, ids, wantIDs)
		}
		byID := map[string]providerCase{}
		for _, c := range m.cases {
			byID[c.ID] = c
		}
		for _, w := range want {
			c, ok := byID[w.ID]
			if !ok {
				continue
			}
			q, missing := c.render(corpusBinding(testRunPrefix))
			if q != w.Query || len(missing) > 0 || c.Kind != w.Kind {
				t.Errorf("%s: %s %q, campaign %s %q", w.ID, c.Kind, q, w.Kind, w.Query)
			}
			from, mutation, page := "", "", 0
			if w.Token != nil {
				page = w.Token.Page
				if w.Token.CaseID != nil {
					from = *w.Token.CaseID
				}
				if w.Token.Mutation != nil {
					mutation = *w.Token.Mutation
				}
			}
			if c.TokenFrom != from || c.Mutation != mutation || c.Page != page {
				t.Errorf("%s: token %s/%d/%s, campaign %s/%d/%s", w.ID, c.TokenFrom, c.Page, c.Mutation, from, page, mutation)
			}
			if fmt.Sprint(c.Header) != fmt.Sprint(w.Headers) && !(len(c.Header) == 0 && len(w.Headers) == 0) {
				t.Errorf("%s: headers %v, campaign %v", w.ID, c.Header, w.Headers)
			}
		}
	}
}

// TestFixtureRolesKeepTheirParts: over a tree shaped like a public
// bucket, every role is bound to names that keep the corpus name's part
// in its questions, and the owned cases stay unsent.
func TestFixtureRolesKeepTheirParts(t *testing.T) {
	var keys []string
	for i := range 8 {
		keys = append(keys, fmt.Sprintf("data/small/f%02d.csv", i))
	}
	for i := range 1200 {
		keys = append(keys, fmt.Sprintf("data/big/%04d.bin", i))
	}
	keys = append(keys, "data/tree/", "data/tree/a/1.txt", "data/tree/a/2.txt", "data/tree/a/3.txt", "data/tree/b/1.txt",
		"data/tree/b/2.txt", "data/tree/c.txt", "data/tree/readme.md", "data/x.csv", "data/x.csv.gz", "data/x.csv.gz.md5")
	slices.Sort(keys)
	f := deriveRoles(keys, "data/")
	n := f.names
	want := map[string]string{"r/": "data/small/", "r/k01": "data/small/f00.csv", "r/k04": "data/small/f02.csv ",
		"r/k05": "data/small/f03.csv", "s/": "data/tree/a/", "x/a": "data/x.csv", "x/ab": "data/x.csv.gz",
		"x/abc": "data/x.csv.gz.md5", "x/": "data/", "d/": "data/tree/", "d/a/": "data/tree/a/", "d/a": "data/tree/a",
		"d/a/1": "data/tree/a/1.txt", "d/a/m": "data/tree/a/2.txt", "d/b/3": "data/tree/b/2.txt~", "d/a0": "data/tree/b/1.txt",
		"b/": "data/big/", "=/1": "/1", "none/": "data/none/"}
	for k, v := range want {
		if n[k] != v {
			t.Errorf("%s = %q, want %q", k, n[k], v)
		}
	}
	if _, ok := n["b/0"]; ok || f.missing["b/0"] == "" {
		t.Errorf("b/0 bound without a directory of exactly 1,000 names: %q", n["b/0"])
	}
	if len(f.reduced) != 0 {
		t.Errorf("reduced roles on a full shape: %v", f.reduced)
	}

	// Without a directory of six objects, one of three stands in, and the
	// cases over it say so.
	small := deriveRoles([]string{"p/a/1", "p/a/2", "p/a/3", "p/b/x"}, "p/")
	if small.names["r/"] != "p/a/" || small.names["r/k07"] != "p/a/3" || small.names["r/k04"] != "p/a/3 " ||
		small.reducedShape(&providerCase{Tmpl: "prefix={r/}&start-after={r/k02}"}) == "" ||
		small.reducedShape(&providerCase{Tmpl: "prefix={d/}"}) != "" {
		t.Errorf("reduced r/: %v %v", small.names, small.reduced)
	}
	sent, owned, missing := 0, 0, 0
	for _, c := range append(s3ProviderCases(), gcsProviderCases()...) {
		q, why := f.instantiate(&c)
		switch {
		case q != "":
			sent++
			if strings.Contains(q, "probe/") {
				t.Errorf("%s names the corpus: %s", c.ID, q)
			}
		case strings.HasPrefix(why, "needs an owned bucket"):
			owned++
		default:
			missing++
			if !strings.Contains(why, "b/0") {
				t.Errorf("%s: %s", c.ID, why)
			}
		}
	}
	if missing != 2 || owned < 40 || sent < 140 {
		t.Errorf("%d sent, %d owned, %d not instantiable", sent, owned, missing)
	}
}
