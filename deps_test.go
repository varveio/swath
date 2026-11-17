// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package swath

import (
	"os/exec"
	"slices"
	"strings"
	"testing"
)

const mod = "github.com/varveio/swath"

// command is what swath's command is built from and no library package may
// link: the flag package, the scheme registry and the command's
// composition, the run pipeline, and the writers, SeekParquet's Parquet
// writer among them.
var command = []string{
	"github.com/spf13/pflag",
	mod + "/internal/cli",
	mod + "/internal/registry",
	mod + "/internal/inventory",
	mod + "/internal/parquetout",
	mod + "/internal/textout",
	"github.com/varveio/seekparquet/go/write",
}

// readerEdge are the packages outside parquet-go that may import it: the
// reader SeekParquet reads datasets with, which the providers' schemas
// reach (internal/schema → reader).
var readerEdge = []string{
	"github.com/varveio/seekparquet/go",
	"github.com/varveio/seekparquet/go/internal/thriftcheck",
}

// sdks are the cloud SDKs' import path prefixes.
var sdks = []string{"github.com/aws/", "cloud.google.com/", "github.com/Azure/", "google.golang.org/api/"}

// deps is pkg and every package it imports, transitively (non-test files,
// this platform's build), each with its own imports.
func deps(t *testing.T, pkg string) map[string][]string {
	t.Helper()
	out, err := exec.Command("go", "list", "-deps", "-f", `{{.ImportPath}}|{{join .Imports ","}}`, pkg).Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			t.Fatalf("go list %s: %v\n%s", pkg, err, ee.Stderr)
		}
		t.Fatalf("go list %s: %v", pkg, err)
	}
	m := map[string][]string{}
	for _, line := range strings.Fields(string(out)) {
		path, imports, _ := strings.Cut(line, "|")
		m[path] = nil
		if imports != "" {
			m[path] = strings.Split(imports, ",")
		}
	}
	return m
}

// TestLibraryImports: the library's packages (this one and the provider
// packages) link none of the command: no flags, no registry, no run
// pipeline, no writer, and parquet-go only through SeekParquet's reader.
// This package links no cloud SDK and no provider; a provider package
// links its own SDK.
func TestLibraryImports(t *testing.T) {
	for _, pkg := range []string{mod, mod + "/s3"} {
		d := deps(t, pkg)
		for _, c := range command {
			if _, ok := d[c]; ok {
				t.Errorf("%s links %s", pkg, c)
			}
		}
		for path, imports := range d {
			if strings.HasPrefix(path, "github.com/parquet-go/") || slices.Contains(readerEdge, path) {
				continue
			}
			for _, imp := range imports {
				if strings.HasPrefix(imp, "github.com/parquet-go/") {
					t.Errorf("%s links %s through %s, not through SeekParquet's reader", pkg, imp, path)
				}
			}
		}
	}

	core := deps(t, mod)
	for path := range core {
		if strings.HasPrefix(path, mod+"/internal/source/") || slices.ContainsFunc(sdks, func(sdk string) bool { return strings.HasPrefix(path, sdk) }) {
			t.Errorf("%s links %s", mod, path)
		}
	}
	// The walk sees what it looks for where it is linked (not vacuous).
	if _, ok := deps(t, mod+"/s3")["github.com/aws/aws-sdk-go-v2/service/s3"]; !ok {
		t.Error("the walk does not see the S3 SDK in the s3 package")
	}
	swath := deps(t, mod+"/cmd/swath")
	for _, c := range command {
		if _, ok := swath[c]; !ok {
			t.Errorf("the walk does not see %s in cmd/swath", c)
		}
	}
}

// TestAzblobImports: the azblob package links none of the command, its
// parquet-go only through SeekParquet's reader, and of the cloud SDKs only
// Azure's; the s3 package links no Azure SDK.
func TestAzblobImports(t *testing.T) {
	const azure = "github.com/Azure/"
	d := deps(t, mod+"/azblob")
	for _, c := range command {
		if _, ok := d[c]; ok {
			t.Errorf("%s/azblob links %s", mod, c)
		}
	}
	for path, imports := range d {
		if !strings.HasPrefix(path, azure) && slices.ContainsFunc(sdks, func(sdk string) bool { return strings.HasPrefix(path, sdk) }) {
			t.Errorf("%s/azblob links %s", mod, path)
		}
		if strings.HasPrefix(path, "github.com/parquet-go/") || slices.Contains(readerEdge, path) {
			continue
		}
		for _, imp := range imports {
			if strings.HasPrefix(imp, "github.com/parquet-go/") {
				t.Errorf("%s/azblob links %s through %s, not through SeekParquet's reader", mod, imp, path)
			}
		}
	}
	if _, ok := d["github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"]; !ok {
		t.Error("the walk does not see the Azure SDK in the azblob package")
	}
	for path := range deps(t, mod+"/s3") {
		if strings.HasPrefix(path, azure) || strings.HasPrefix(path, mod+"/internal/source/azblob") {
			t.Errorf("%s/s3 links %s", mod, path)
		}
	}
}
