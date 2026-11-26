// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package command_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/varveio/swath/command"
)

// Run swath list on a directory in this process, its keys as TSV rows on
// stdout, and branch on how the run ended without reading its messages.
func ExampleList() {
	dir, err := os.MkdirTemp("", "photos")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	for _, name := range []string{"2026/a.jpg", "2026/b.jpg", "-c.jpg"} {
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			log.Fatal(err)
		}
		if err := os.WriteFile(p, nil, 0o644); err != nil {
			log.Fatal(err)
		}
	}

	r, err := command.List(context.Background(), command.ListOptions{
		Source:   dir,
		Format:   "tsv",
		Columns:  []string{"key"},
		NoHeader: true,
		Stdout:   os.Stdout,
		Stderr:   os.Stderr,
	})
	switch {
	case errors.Is(err, command.ErrSourceNotFound):
		fmt.Println("no such directory")
	case errors.Is(err, command.ErrOutputInUse):
		fmt.Println("another run owns the output")
	case errors.Is(err, context.Canceled):
		fmt.Println("canceled")
	case err != nil:
		fmt.Println(r.Class, r.ExitCode, err)
	default:
		fmt.Println(r.Status, r.ExitCode)
	}
	// Output:
	// -c.jpg
	// 2026/
	// 2026/a.jpg
	// 2026/b.jpg
	// completed 0
}
