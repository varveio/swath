// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

// TestNoCloudSDK: neither the packages this module builds (its tests
// included) nor its module graph name a cloud SDK, and none of them is the
// lister's module. The go commands run with the workspace off, so the
// graph is this module's, not the workspace's three modules'.
func TestNoCloudSDK(t *testing.T) {
	for _, args := range [][]string{
		{"list", "-deps", "-test", "-f", "{{with .Module}}{{.Path}}{{end}}", "./..."},
		{"list", "-m", "all"},
	} {
		cmd := exec.Command("go", args...)
		cmd.Env = append(os.Environ(), "GOWORK=off")
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("go %s: %v", strings.Join(args, " "), err)
		}
		for line := range strings.Lines(string(out)) {
			m := strings.ToLower(strings.TrimSpace(line))
			if strings.Contains(m, "aws") || strings.Contains(m, "cloud.google.com") || strings.Contains(m, "azure") || m == "github.com/varveio/swath" {
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
