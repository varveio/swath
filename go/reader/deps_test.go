// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"os/exec"
	"strings"
	"testing"
)

// TestNoCloudSDK: neither the packages this module builds (its tests
// included) nor its module graph name a cloud SDK.
func TestNoCloudSDK(t *testing.T) {
	for _, args := range [][]string{
		{"list", "-deps", "-test", "-f", "{{with .Module}}{{.Path}}{{end}}", "./..."},
		{"list", "-m", "all"},
	} {
		out, err := exec.Command("go", args...).Output()
		if err != nil {
			t.Fatalf("go %s: %v", strings.Join(args, " "), err)
		}
		for line := range strings.Lines(string(out)) {
			if m := strings.ToLower(line); strings.Contains(m, "aws") || strings.Contains(m, "cloud.google.com") || strings.Contains(m, "azure") {
				t.Errorf("go %s: %s", strings.Join(args, " "), strings.TrimSpace(line))
			}
		}
	}
}

// TestNoParquetGoImport: the package's own code (its tests aside) does not
// import parquet-go: it reads values and schemas through SeekParquet's own
// types, so parquet-go is no part of this module's API.
func TestNoParquetGoImport(t *testing.T) {
	out, err := exec.Command("go", "list", "-f", `{{join .Imports "\n"}}`, ".").Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	for line := range strings.Lines(string(out)) {
		if strings.HasPrefix(line, "github.com/parquet-go/") {
			t.Errorf("imports %s", strings.TrimSpace(line))
		}
	}
}
