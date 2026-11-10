// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"os"
	"reflect"
	"strings"
	"testing"
)

// TestProgressSchemaDoc: docs/reference/run-report.md's progress-file table, the one
// schema of progress.json, names every field ProgressSnapshot writes.
func TestProgressSchemaDoc(t *testing.T) {
	b, err := os.ReadFile("../../docs/reference/run-report.md")
	if err != nil {
		t.Fatal(err)
	}
	doc := string(b)
	ty := reflect.TypeOf(ProgressSnapshot{})
	for i := 0; i < ty.NumField(); i++ {
		name, _, _ := strings.Cut(ty.Field(i).Tag.Get("json"), ",")
		if name != "" && name != "order" && !strings.Contains(doc, "`"+name+"`") {
			t.Errorf("progress.json field %q is not in docs/reference/run-report.md", name)
		}
	}
}
