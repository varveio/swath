// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/spf13/pflag"
)

func script(t *testing.T, shell string) string {
	t.Helper()
	var b bytes.Buffer
	if err := writeCompletion(&b, shell); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

// TestCompletionGolden keeps one script per shell in testdata: run with
// UPDATE=1 to rewrite them after the flag table changes.
func TestCompletionGolden(t *testing.T) {
	for _, shell := range completionShells {
		got := script(t, shell)
		path := filepath.Join("testdata", "completion."+shell)
		if os.Getenv("UPDATE") != "" {
			if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
				t.Fatal(err)
			}
			continue
		}
		want, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if string(want) != got {
			t.Errorf("%s is stale; run UPDATE=1 go test ./cmd/swath -run TestCompletion", path)
		}
	}
}

// TestCompletionHasEveryFlag: every flag of the table and every command
// word is in each script.
func TestCompletionHasEveryFlag(t *testing.T) {
	var names []string
	newFlags(&options{}, true).VisitAll(func(fl *pflag.Flag) {
		if !fl.Hidden {
			names = append(names, fl.Name)
		}
	})
	for _, shell := range completionShells {
		s := script(t, shell)
		for _, n := range names {
			needle := "--" + n
			if shell == "fish" {
				needle = "-l " + n + " "
			}
			if !strings.Contains(s, needle) {
				t.Errorf("%s script lacks --%s", shell, n)
			}
		}
		for _, c := range completionCommands() {
			if !strings.Contains(s, c.word) {
				t.Errorf("%s script lacks the command %s", shell, c.word)
			}
		}
	}
}

// TestCompletionSyntax runs each shell's own syntax check where the shell
// is installed.
func TestCompletionSyntax(t *testing.T) {
	for _, shell := range completionShells {
		path, err := exec.LookPath(shell)
		if err != nil {
			t.Logf("%s is not installed", shell)
			continue
		}
		file := filepath.Join(t.TempDir(), "swath."+shell)
		if err := os.WriteFile(file, []byte(script(t, shell)), 0o644); err != nil {
			t.Fatal(err)
		}
		if out, err := exec.Command(path, "-n", file).CombinedOutput(); err != nil {
			t.Errorf("%s -n: %v\n%s", shell, err, out)
		}
	}
}

// TestBashCompletes drives the bash script as bash would: the words typed,
// the replies.
func TestBashCompletes(t *testing.T) {
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash is not installed")
	}
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "tree"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "swath.bash"), []byte(script(t, "bash")), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "top"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		words     []string // COMP_WORDS, the last being the word completed
		want, not []string
	}{
		{[]string{"swath", "li"}, []string{"list"}, []string{"ls", "du"}},
		{[]string{"swath", ""}, []string{"list", "ls", "du", "resume", "show", "diag", "completion", "help", "version"}, nil},
		{[]string{"swath", "list", "--for"}, []string{"--format"}, []string{"--depth"}},
		{[]string{"swath", "ls", "--for"}, []string{"--format"}, nil},
		{[]string{"swath", "du", "--dep"}, []string{"--depth"}, nil},
		{[]string{"swath", "list", "--format", ""}, []string{"auto", "table", "tsv", "jsonl"}, []string{"--format"}},
		{[]string{"swath", "list", "--format", "=", ""}, []string{"auto", "table", "tsv", "jsonl"}, nil},
		{[]string{"swath", "list", "--format", "=", "t"}, []string{"table", "tsv"}, []string{"auto"}},
		{[]string{"swath", "--color", "never", "list", "--for"}, []string{"--format"}, nil},
		{[]string{"swath", "--color", "=", "never", "du", "--dep"}, []string{"--depth"}, nil},
		{[]string{"swath", "list", "--color", ""}, []string{"auto", "always", "never"}, nil},
		{[]string{"swath", "list", "s"}, []string{"s3://"}, []string{"gs://"}},
		{[]string{"swath", "du", ""}, []string{"gs://", "az://", "s3://", "tree"}, nil},
		{[]string{"swath", "list", "-o", "tr"}, []string{"tree"}, nil},
		{[]string{"swath", "list", "--workers", ""}, nil, []string{"tree", "list", "--workers"}},
		{[]string{"swath", "diag", "c"}, []string{"count"}, []string{"list"}},
		{[]string{"swath", "diag", "count", "--seq"}, []string{"--sequential"}, nil},
		{[]string{"swath", "list", "--seq"}, nil, []string{"--sequential"}},
		{[]string{"swath", "resume", "t"}, []string{"tree"}, []string{"s3://"}},
		{[]string{"swath", "resume", "--wor"}, []string{"--workers"}, []string{"--output"}},
		{[]string{"swath", "show", "--j"}, []string{"--journal"}, nil},
		{[]string{"swath", "show", "--log"}, []string{"--log-format"}, nil},
		{[]string{"swath", "list", "to"}, []string{"top"}, nil},
		{[]string{"swath", "resume", "to"}, nil, []string{"top"}},
		{[]string{"swath", "diag", "list", "--out"}, []string{"--output"}, nil},
		{[]string{"swath", "diag", "list", "--no-se"}, []string{"--no-seed"}, nil},
		{[]string{"swath", "resume", "tree", "--no-an"}, []string{"--no-anonymous"}, nil},
		{[]string{"swath", "completion", ""}, []string{"bash", "zsh", "fish"}, nil},
		{[]string{"swath", "help", "d"}, []string{"du", "diag"}, nil},
	} {
		var quoted []string
		for _, w := range c.words {
			quoted = append(quoted, "'"+w+"'")
		}
		prog := "source swath.bash; COMP_WORDS=(" + strings.Join(quoted, " ") + "); COMP_CWORD=" + strconv.Itoa(len(c.words)-1) +
			"; _swath; printf '%s\\n' \"${COMPREPLY[@]}\""
		cmd := exec.Command(bash, "-c", prog)
		cmd.Dir = dir
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Errorf("%v: %v\n%s", c.words, err, out)
			continue
		}
		got := strings.Fields(string(out))
		for _, w := range c.want {
			if !slices.Contains(got, w) {
				t.Errorf("%v: replies %v lack %q", c.words, got, w)
			}
		}
		for _, w := range c.not {
			if slices.Contains(got, w) {
				t.Errorf("%v: replies %v include %q", c.words, got, w)
			}
		}
	}
}

// TestCompletionCommand: swath completion refuses what it cannot print and
// prints the script of a shell it knows.
func TestCompletionCommand(t *testing.T) {
	for _, args := range [][]string{{"completion"}, {"completion", "powershell"}, {"completion", "bash", "zsh"}} {
		var out bytes.Buffer
		err := dispatch(context.Background(), args, &out)
		var u usageError
		if !errors.As(err, &u) || out.Len() != 0 {
			t.Errorf("%v: err %v, output %q; want a usage error and no output", args, err, out.String())
		}
	}
	var out bytes.Buffer
	if err := dispatch(context.Background(), []string{"completion", "fish"}, &out); err != nil || out.String() != script(t, "fish") {
		t.Errorf("swath completion fish: err %v", err)
	}
	out.Reset()
	if err := dispatch(context.Background(), []string{"completion", "--help"}, &out); !errors.Is(err, pflag.ErrHelp) || !strings.Contains(out.String(), "swath completion bash|zsh|fish") {
		t.Errorf("swath completion --help: err %v, output %q", err, out.String())
	}
}
