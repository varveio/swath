// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/varveio/swath/internal/journal"
)

// TestTroubleshootingMessages keeps the refusals docs/troubleshooting.md
// quotes equal to what swath prints for them: each case runs the command
// line the page describes, requires exit 2, and requires the page to hold
// the first line swath prints on stderr verbatim.
func TestTroubleshootingMessages(t *testing.T) {
	// Profiling side paths would be checked, and written, by the runs here.
	t.Setenv("SWATH_CPUPROFILE", "")
	t.Setenv("SWATH_MEMPROFILE", "")
	t.Setenv("SWATH_MEMPROFILERATE", "")
	doc, err := os.ReadFile("../../docs/troubleshooting.md")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	t.Chdir(dir)
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "a"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	// "inventory" holds a dataset, "other" a stranger's file.
	if err := run(context.Background(), []string{src, "-o", "inventory", "-q"}, io.Discard, false); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir("other", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join("other", "f"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	// "tree/inventory" holds a dataset inside the tree it lists.
	if err := os.Mkdir("tree", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join("tree", "a"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run(context.Background(), []string{"tree", "-o", filepath.Join("tree", "inventory"), "-q"}, io.Discard, false); err != nil {
		t.Fatal(err)
	}
	// "stopped" holds an interrupted run: a journal and no manifest.
	j, err := journal.Create("stopped", journal.Identity{Tool: "swath"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	j.Close()
	for _, c := range []struct {
		name string
		args []string
		// rename maps the case's own names to the page's.
		rename [2]string
	}{
		{"unknown flag", []string{"list", src, "--region", "x"}, [2]string{}},
		{"provider option", []string{"list", "gs://bucket", "--s3-profile", "p"}, [2]string{}},
		{"output not empty", []string{"list", src, "-o", "inventory"}, [2]string{}},
		{"overwrite a stranger", []string{"list", src, "-o", "other", "--overwrite"}, [2]string{"other", "inventory"}},
		{"output inside the tree", []string{"list", "tree", "-o", filepath.Join("tree", "inventory")}, [2]string{filepath.Join("tree", "inventory"), "tree/inventory"}},
		{"csv output", []string{"list", src, "-o", "x.csv"}, [2]string{}},
		{"interrupted run", []string{"list", src, "-o", "stopped"}, [2]string{"stopped", "inventory"}},
	} {
		stderr := captureSyncStderr(t)
		code := conclude(dispatch(context.Background(), c.args, io.Discard))
		if code != exitUsage {
			t.Errorf("%s: %v exits %d, want %d", c.name, c.args, code, exitUsage)
		}
		line, _, _ := strings.Cut(stderr.String(), "\n")
		if !strings.HasPrefix(line, "swath: ") {
			t.Errorf("%s: stderr begins %q, not a refusal", c.name, line)
			continue
		}
		if c.rename[0] != "" {
			line = strings.Replace(line, c.rename[0], c.rename[1], 1)
		}
		if !strings.Contains(string(doc), "\n"+line+"\n") {
			t.Errorf("%s: docs/troubleshooting.md does not quote the message swath prints:\n%s", c.name, line)
		}
	}
}

// TestCLIDocPages keeps docs/commands/ in step with the command tree,
// beside TestCLIDocFlags (the option lists): every command the root help
// lists has a page and a row in swath.md's table, and each page's
// synopsis is the usage lines its --help prints. UPDATE=1 rewrites the
// synopsis blocks.
func TestCLIDocPages(t *testing.T) {
	table, err := os.ReadFile("../../docs/commands/swath.md")
	if err != nil {
		t.Fatal(err)
	}
	usages := map[string][]string{
		"list":       helpOf(command{}).usage,
		"du":         helpOf(command{count: true, du: true}).usage,
		"resume":     resumeHelp.usage,
		"show":       showHelp.usage,
		"completion": completionHelp.usage,
	}
	for _, c := range rootCommands {
		word, _, _ := strings.Cut(c[0], ",")
		switch word {
		case "help", "version":
			continue // the same as --help and --version: rows of the table, no page
		case "diag":
			if !strings.Contains(string(table), "`swath diag`") {
				t.Errorf("docs/commands/swath.md does not mention swath diag")
			}
			continue // documented with the internals (TestCLIDocFlags' diag blocks)
		}
		page := "swath-" + word + ".md"
		if !strings.Contains(string(table), "[`swath "+word+"`]("+page+")") {
			t.Errorf("docs/commands/swath.md's command table has no row for swath %s linking %s", word, page)
		}
		u, ok := usages[word]
		if !ok {
			t.Errorf("swath %s has no usage here: add it to usages", word)
			continue
		}
		var b strings.Builder
		for _, l := range u {
			b.WriteString("swath " + l + "\n")
		}
		docBlock(t, "../../docs/commands/"+page, "synopsis", "text", b.String())
	}
	// swath show's one option, as its --help prints it.
	var out bytes.Buffer
	_ = dispatch(context.Background(), []string{"show", "--help"}, &out)
	_, opts, ok := strings.Cut(out.String(), "Options:\n")
	if !ok {
		t.Fatalf("swath show --help has no options:\n%s", out.String())
	}
	docBlock(t, "../../docs/commands/swath-show.md", "flags", "text", opts)
}
