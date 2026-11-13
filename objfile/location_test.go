// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objfile_test

import (
	"strings"
	"testing"

	"github.com/varveio/swath/objfile"
)

func TestParseLocation(t *testing.T) {
	for raw, want := range map[string]objfile.Location{
		"s3://bkt":                  {Scheme: "s3", Container: "bkt"},
		"s3://bkt/":                 {Scheme: "s3", Container: "bkt"},
		"s3://my.bkt-1/datasets/ds": {Scheme: "s3", Container: "my.bkt-1", Prefix: "datasets/ds/"},
		"gs://bkt_x/ds/":            {Scheme: "gs", Container: "bkt_x", Prefix: "ds/"},
		"gs://bkt/a%20b/c d/":       {Scheme: "gs", Container: "bkt", Prefix: "a%20b/c d/"},
		"az://acct/ctr":             {Scheme: "az", Account: "acct", Container: "ctr"},
		"az://acct/ctr/root/ds/":    {Scheme: "az", Account: "acct", Container: "ctr", Prefix: "root/ds/"},
	} {
		got, err := objfile.ParseLocation(raw)
		if err != nil || got != want {
			t.Errorf("ParseLocation(%q) = %+v, %v; want %+v", raw, got, err, want)
			continue
		}
		again, err := objfile.ParseLocation(got.String())
		if err != nil || again != got {
			t.Errorf("ParseLocation(%q) = %+v, %v; want %+v", got.String(), again, err, got)
		}
	}
	for _, raw := range []string{
		"", "bkt/ds", "S3://bkt", "s4://bkt", "file:///tmp", "s3:/bkt",
		"s3://", "s3:///ds", "s3://Bkt", "s3://user@bkt/ds", "s3://bkt:9000/ds",
		"s3://bkt/ds?versionId=1", "s3://bkt/ds#frag", "az://acct/ctr/ds?sig=x",
		"az://acct/../ds", "az://acct/./ds", "az://../ctr/ds", "az://./ctr", "s3://../ds", "s3://./ds", "gs://..",
		"s3://bkt/../ds", "s3://bkt/ds/..", "s3://bkt/a/./b", "s3://bkt//ds", "s3://bkt/ds//", "s3://bkt/a\\b",
		"s3://bkt/a\x00b", "az://acct", "az://acct/", "az:///ctr", "az://acct/Ctr",
	} {
		if got, err := objfile.ParseLocation(raw); err == nil {
			t.Errorf("ParseLocation(%q) = %+v, want an error", raw, got)
		}
	}
	if got := (objfile.Location{Scheme: "az", Account: "acct", Container: "ctr", Prefix: "ds/"}).String(); got != "az://acct/ctr/ds/" {
		t.Errorf("String = %q", got)
	}
}

func TestParseLocationSecrecy(t *testing.T) {
	for _, root := range []string{"s3://bkt/ds", "gs://bkt/ds", "az://acct/ctr/ds"} {
		scheme, rest, _ := strings.Cut(root, "://")
		for _, raw := range []string{root + "?sig=SECRET", root + "#SECRET", scheme + "://user:SECRET@" + rest} {
			if _, err := objfile.ParseLocation(raw); err == nil || strings.Contains(err.Error(), "SECRET") {
				t.Errorf("%q: error %v; want a refusal without credentials", raw, err)
			}
		}
	}
}

func TestParseLocationDiagnostic(t *testing.T) {
	for _, c := range []struct{ raw, shown string }{
		{"gs://b:SECRET/p", "[redacted]"},
		{"az://user:SECRET@acct/c", "[redacted]"},
		{"https://host/p?sig=SECRET", "[redacted]"},
		{"s3://UPPER/p", "s3://UPPER/p"},
		{"s3://UPPER/team@home", "s3://UPPER/team@home"},
		{"s3:/user:SECRET@bkt", "[redacted]"},
		{"s3:/bkt/p", "s3:/bkt/p"},
		{"https://host/p", "https://host/p"},
	} {
		_, err := objfile.ParseLocation(c.raw)
		if err == nil || !strings.Contains(err.Error(), `location "`+c.shown+`":`) || strings.Contains(err.Error(), "SECRET") {
			t.Errorf("%q: %v; want location %q without credentials", c.raw, err, c.shown)
		}
	}
}
