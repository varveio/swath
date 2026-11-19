// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package acceptance runs the scenarios in testdata against built swath and
// swath-replay binaries, as a user runs them. See README.md.
package acceptance

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/rogpeppe/go-internal/testscript"
)

// expectations are the values SWATH_EXPECT may take: the behaviour of the
// v0.4.0 release, or the behaviour of the current main branch.
var expectations = []string{"v0.4.0", "current"}

// replayStartup bounds the wait for swath-replay's startup line.
const replayStartup = 30 * time.Second

// processWait bounds each wait of signal and head: for the path that says
// when to signal, and for the program to end after the signal or after its
// reader closed.
const processWait = 60 * time.Second

// pollEvery is how often a bounded wait looks at what it waits for.
const pollEvery = 10 * time.Millisecond

// script is a script's own state for the harness commands: the names of the
// environment variables it started with, and the log of each replay server
// it started, by endpoint.
type script struct {
	envNames []string
	logs     map[string]*replayLog
}

// replayLog is a replay server's stderr, written by the server into a file,
// and how much of it replaylog has handed out.
type replayLog struct {
	path   string
	offset int64
}

type scriptKey struct{}

func state(ts *testscript.TestScript) *script { return ts.Value(scriptKey{}).(*script) }

type inputs struct {
	swath, replay, duckdb, expect string
}

func TestScenarios(t *testing.T) {
	in := readInputs(t)
	bin := stageBinaries(t, in)
	handleIgnoredSignals()
	testdata, err := filepath.Abs("testdata")
	if err != nil {
		t.Fatal(err)
	}
	testscript.Run(t, testscript.Params{
		Dir:                "testdata",
		RequireUniqueNames: true,
		Setup: func(env *testscript.Env) error {
			env.Setenv("PATH", bin+string(os.PathListSeparator)+env.Getenv("PATH"))
			env.Setenv("TESTDATA", testdata)
			env.Setenv("TZ", "UTC")
			if runtime.GOOS == "windows" {
				for _, name := range []string{"SYSTEMROOT", "WINDIR"} {
					env.Setenv(name, os.Getenv(name))
				}
			} else {
				// The owner of every file a script creates.
				env.Setenv("UID", strconv.Itoa(os.Getuid()))
			}
			st := &script{logs: map[string]*replayLog{}}
			for _, kv := range env.Vars {
				if name, _, ok := strings.Cut(kv, "="); ok && name != "" {
					st.envNames = append(st.envNames, name)
				}
			}
			env.Values[scriptKey{}] = st
			return nil
		},
		Condition: func(cond string) (bool, error) {
			if v, ok := strings.CutPrefix(cond, "expect:"); ok {
				for _, e := range expectations {
					if v == e {
						return v == in.expect, nil
					}
				}
				return false, fmt.Errorf("unknown expectation %q: want one of %v", v, expectations)
			}
			return false, fmt.Errorf("unknown condition %q", cond)
		},
		Cmds: map[string]func(*testscript.TestScript, bool, []string){
			"exits":     cmdExits,
			"mktree":    cmdMktree,
			"seq":       cmdSeq,
			"json":      cmdJSON,
			"cmpform":   cmdCmpform,
			"digests":   cmdDigests,
			"dataset":   func(ts *testscript.TestScript, neg bool, args []string) { cmdDataset(ts, neg, args, in.duckdb) },
			"replay":    func(ts *testscript.TestScript, neg bool, args []string) { cmdReplay(ts, neg, args, in.replay) },
			"replaylog": cmdReplaylog,
			"signal":    cmdSignal,
			"head":      cmdHead,
		},
	})
}

// readInputs fails the run, never skips it, when an input is missing or
// names the wrong binary.
func readInputs(t *testing.T) inputs {
	t.Helper()
	var in inputs
	var problems []string
	in.expect = os.Getenv("SWATH_EXPECT")
	known := false
	for _, e := range expectations {
		known = known || in.expect == e
	}
	if !known {
		problems = append(problems, fmt.Sprintf("SWATH_EXPECT=%q: want one of %v", in.expect, expectations))
	}
	for _, b := range []struct {
		env, version string
		dst          *string
	}{{"SWATH_BIN", "swath ", &in.swath}, {"SWATH_REPLAY_BIN", "swath-replay ", &in.replay}} {
		p := os.Getenv(b.env)
		if err := checkBinary(p, b.version); err != nil {
			problems = append(problems, fmt.Sprintf("%s=%q: %v", b.env, p, err))
		}
		*b.dst = p
	}
	duckdb, err := exec.LookPath("duckdb")
	if err != nil {
		problems = append(problems, fmt.Sprintf("duckdb is not on PATH: %v", err))
	}
	in.duckdb = duckdb
	if len(problems) > 0 {
		t.Fatalf("the acceptance suite needs its inputs (see README.md):\n  %s", strings.Join(problems, "\n  "))
	}
	t.Logf("SWATH_EXPECT=%s SWATH_BIN=%s SWATH_REPLAY_BIN=%s duckdb=%s", in.expect, in.swath, in.replay, in.duckdb)
	return in
}

// checkBinary requires an absolute path to an executable whose --version
// line starts with prefix, so that swapped or stale inputs fail here.
func checkBinary(path, prefix string) error {
	if path == "" {
		return errors.New("not set")
	}
	if !filepath.IsAbs(path) {
		return errors.New("not an absolute path")
	}
	fi, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !fi.Mode().IsRegular() || runtime.GOOS != "windows" && fi.Mode()&0o111 == 0 {
		return errors.New("not an executable file")
	}
	out, err := exec.Command(path, "--version").Output()
	if err != nil {
		return fmt.Errorf("--version: %v", err)
	}
	if !strings.HasPrefix(string(out), prefix) {
		return fmt.Errorf("--version printed %q, want a line starting %q", strings.TrimSpace(string(out)), prefix)
	}
	return nil
}

// stageBinaries copies the binaries under test into a directory of their
// canonical names, which the scripts find first on PATH.
func stageBinaries(t *testing.T, in inputs) string {
	t.Helper()
	dir := t.TempDir()
	exe := ""
	if runtime.GOOS == "windows" {
		exe = ".exe"
	}
	for name, src := range map[string]string{"swath": in.swath, "swath-replay": in.replay} {
		b, err := os.ReadFile(src)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name+exe), b, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// exits N PROGRAM [ARGS...] runs PROGRAM and requires exit status N.
func cmdExits(ts *testscript.TestScript, neg bool, args []string) {
	if neg {
		ts.Fatalf("unsupported: ! exits")
	}
	if len(args) < 2 {
		ts.Fatalf("usage: exits N program [args...]")
	}
	want, err := strconv.Atoi(args[0])
	if err != nil {
		ts.Fatalf("exits: %v", err)
	}
	err = ts.Exec(args[1], args[2:]...)
	got := 0
	var exit *exec.ExitError
	switch {
	case errors.As(err, &exit):
		got = exit.ExitCode()
	case err != nil:
		ts.Fatalf("%s: %v", args[1], err)
	}
	if got != want {
		ts.Fatalf("%s exited %d, want %d", args[1], got, want)
	}
}

// mktree DIR SPEC builds the tree SPEC describes under DIR, one entry a
// line, every directory declared before its entries:
//
//	dir PATH MTIME
//	file PATH SIZE MTIME
//	link PATH TARGET          (a hard link to the file TARGET)
//
// PATH is slash-separated below DIR, quoted as a Go string when it holds
// spaces; MTIME is RFC 3339. A file holds SIZE bytes. Files get mode 0644
// and directories 0755, whatever the umask. Every mode and time is set
// after every entry exists, so creating entries does not move them.
func cmdMktree(ts *testscript.TestScript, neg bool, args []string) {
	if neg || len(args) != 2 {
		ts.Fatalf("usage: mktree dir spec")
	}
	root := ts.MkAbs(args[0])
	ts.Check(os.MkdirAll(root, 0o755))
	dirs := map[string]bool{"": true}
	times := map[string]time.Time{}
	modes := map[string]os.FileMode{}
	var order []string
	for i, line := range strings.Split(ts.ReadFile(args[1]), "\n") {
		if line = strings.TrimSpace(line); line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f, err := splitFields(line)
		if err != nil {
			ts.Fatalf("%s:%d: %v", args[1], i+1, err)
		}
		bad := func(format string, a ...any) {
			ts.Fatalf("%s:%d: %s", args[1], i+1, fmt.Sprintf(format, a...))
		}
		if len(f) < 3 {
			bad("too few fields")
		}
		rel := f[1]
		if parent := pathDir(rel); !dirs[parent] {
			bad("the parent of %q is not declared", rel)
		}
		path := filepath.Join(root, filepath.FromSlash(rel))
		mtime := func(s string) time.Time {
			t, err := time.Parse(time.RFC3339, s)
			if err != nil {
				bad("%v", err)
			}
			return t
		}
		switch {
		case f[0] == "dir" && len(f) == 3:
			ts.Check(os.MkdirAll(path, 0o755))
			dirs[rel] = true
			times[path], modes[path] = mtime(f[2]), 0o755
		case f[0] == "file" && len(f) == 4:
			size, err := strconv.Atoi(f[2])
			if err != nil {
				bad("%v", err)
			}
			ts.Check(os.WriteFile(path, bytes.Repeat([]byte{'.'}, size), 0o644))
			times[path], modes[path] = mtime(f[3]), 0o644
		case f[0] == "link" && len(f) == 3:
			ts.Check(os.Link(filepath.Join(root, filepath.FromSlash(f[2])), path))
		default:
			bad("want dir PATH MTIME, file PATH SIZE MTIME or link PATH TARGET")
		}
		order = append(order, path)
	}
	for _, path := range order {
		if mode, ok := modes[path]; ok {
			ts.Check(os.Chmod(path, mode))
		}
		if t, ok := times[path]; ok {
			ts.Check(os.Chtimes(path, t, t))
		}
	}
}

// seq FILE N FORMAT appends N lines to FILE, line i (1 to N) being
// fmt.Sprintf(FORMAT, i) after FORMAT's Go escapes (\t) are read.
func cmdSeq(ts *testscript.TestScript, neg bool, args []string) {
	if neg || len(args) != 3 {
		ts.Fatalf("usage: seq file n format")
	}
	n, err := strconv.Atoi(args[1])
	if err != nil {
		ts.Fatalf("seq: %v", err)
	}
	format, err := strconv.Unquote(`"` + args[2] + `"`)
	if err != nil {
		ts.Fatalf("seq: format %s: %v", args[2], err)
	}
	var b strings.Builder
	for i := 1; i <= n; i++ {
		fmt.Fprintf(&b, format, i)
		b.WriteByte('\n')
	}
	f, err := os.OpenFile(ts.MkAbs(args[0]), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	ts.Check(err)
	_, err = f.WriteString(b.String())
	ts.Check(errors.Join(err, f.Close()))
}

func pathDir(rel string) string {
	if i := strings.LastIndexByte(rel, '/'); i >= 0 {
		return rel[:i]
	}
	return ""
}

// splitFields splits a line at spaces, reading a field that starts with a
// double quote as a Go string literal.
func splitFields(line string) ([]string, error) {
	var out []string
	for line = strings.TrimSpace(line); line != ""; line = strings.TrimSpace(line) {
		if line[0] != '"' {
			field, rest, _ := strings.Cut(line, " ")
			out, line = append(out, field), rest
			continue
		}
		q, err := strconv.QuotedPrefix(line)
		if err != nil {
			return nil, err
		}
		field, _ := strconv.Unquote(q)
		out, line = append(out, field), line[len(q):]
	}
	return out, nil
}

// json FILE PATH... prints the JSON value at each dotted PATH (a number
// indexes an array) on a line of its own, compact, object keys sorted.
// ! json FILE PATH succeeds when PATH is absent.
func cmdJSON(ts *testscript.TestScript, neg bool, args []string) {
	if len(args) < 2 || neg && len(args) != 2 {
		ts.Fatalf("usage: json file path... | ! json file path")
	}
	var doc any
	if err := json.Unmarshal([]byte(ts.ReadFile(args[0])), &doc); err != nil {
		ts.Fatalf("%s: %v", args[0], err)
	}
	for _, path := range args[1:] {
		v, ok := lookup(doc, path)
		switch {
		case neg && ok:
			ts.Fatalf("%s: %s is present: %s", args[0], path, compact(v))
		case neg:
			return
		case !ok:
			ts.Fatalf("%s: %s is absent", args[0], path)
		}
		fmt.Fprintln(ts.Stdout(), compact(v))
	}
}

func lookup(v any, path string) (any, bool) {
	for _, seg := range strings.Split(path, ".") {
		switch x := v.(type) {
		case map[string]any:
			var ok bool
			if v, ok = x[seg]; !ok {
				return nil, false
			}
		case []any:
			i, err := strconv.Atoi(seg)
			if err != nil || i < 0 || i >= len(x) {
				return nil, false
			}
			v = x[i]
		default:
			return nil, false
		}
	}
	return v, true
}

func compact(v any) string {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return err.Error()
	}
	return strings.TrimSuffix(b.String(), "\n")
}

// forms are the placeholders cmpform accepts, each for a value that differs
// from run to run or machine to machine.
var forms = map[string]string{
	"{int}":  `[0-9]+`,
	"{pos}":  `[1-9][0-9]*`,
	"{time}": `20[0-9]{2}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?Z`,
	"{path}": `[^\t"\n]+`,
}

var formRE = regexp.MustCompile(`\{(int|pos|time|path)\}`)

// cmpform FILE WANT compares FILE (or stdout or stderr) with WANT, line by
// line and every character exactly, after expanding $VAR in WANT as cmpenv
// does, except that a placeholder in WANT stands for one value of its form:
// {int} a decimal integer, {pos} a positive one (no leading zero), {time} a
// UTC timestamp in the years 2000 to 2099 (2026-01-02T03:04:05Z, any
// fraction), {path} a temporary path (no tab, quote or line feed).
func cmdCmpform(ts *testscript.TestScript, neg bool, args []string) {
	if neg || len(args) != 2 {
		ts.Fatalf("usage: cmpform file want")
	}
	got := strings.Split(ts.ReadFile(args[0]), "\n")
	want := strings.Split(os.Expand(ts.ReadFile(args[1]), ts.Getenv), "\n")
	var diffs []string
	for i := range max(len(got), len(want)) {
		var g, w string
		if i < len(got) {
			g = got[i]
		}
		if i < len(want) {
			w = want[i]
		}
		if i >= len(got) || i >= len(want) || !lineForm(w).MatchString(g) {
			diffs = append(diffs, fmt.Sprintf("line %d:\n  want %q\n  got  %q", i+1, w, g))
		}
	}
	if len(diffs) > 0 {
		ts.Fatalf("%s and %s differ (%d lines and %d):\n%s", args[0], args[1], len(got), len(want), strings.Join(diffs, "\n"))
	}
}

// lineForm reads a line of cmpform's WANT as an anchored expression: its
// placeholders by their forms, everything else literally.
func lineForm(want string) *regexp.Regexp {
	var b strings.Builder
	b.WriteString(`\A`)
	last := 0
	for _, m := range formRE.FindAllStringIndex(want, -1) {
		b.WriteString(regexp.QuoteMeta(want[last:m[0]]))
		b.WriteString(forms[want[m[0]:m[1]]])
		last = m[1]
	}
	b.WriteString(regexp.QuoteMeta(want[last:]))
	b.WriteString(`\z`)
	return regexp.MustCompile(b.String())
}

// digests DIR prints a line for each directory and file below DIR, in
// lexical order: a directory's slash-separated path and a slash, a file's
// path, size and SHA-256. Comparing two of its outputs shows whether a
// command changed anything in DIR. DIR must be a directory: an empty one
// prints nothing, so a file in its place must not print nothing too.
func cmdDigests(ts *testscript.TestScript, neg bool, args []string) {
	if neg || len(args) != 1 {
		ts.Fatalf("usage: digests dir")
	}
	root := ts.MkAbs(args[0])
	ts.Check(filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == root {
			if !d.IsDir() {
				return fmt.Errorf("%s is not a directory", args[0])
			}
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			fmt.Fprintf(ts.Stdout(), "%s/\n", rel)
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		fmt.Fprintf(ts.Stdout(), "%s %d %x\n", rel, len(b), sha256.Sum256(b))
		return nil
	}))
}

// dataset DIR SQL runs SQL in DuckDB over the committed dataset in DIR and
// prints the rows tab-separated, without a header, NULL as NULL. Before
// the query it checks the manifest's claims against the files: each part
// it names exists with its byte count, SHA-256 and row count, and data/
// holds no other part, and every part has the first part's Parquet schema
// (each node's name, types, repetition and the rest of DuckDB's
// parquet_schema, in order). SQL sees two tables: ds, every column of the
// parts in the manifest's order and each part's row order (the physical
// order: a query without ORDER BY returns it), and ds_schema, the parts'
// Parquet columns (name, type, repetition_type, converted_type).
func cmdDataset(ts *testscript.TestScript, neg bool, args []string, duckdb string) {
	if neg || len(args) != 2 {
		ts.Fatalf("usage: dataset dir sql")
	}
	dir := ts.MkAbs(args[0])
	var m struct {
		Parts []struct {
			Path   string `json:"path"`
			Rows   int64  `json:"rows"`
			Bytes  int64  `json:"bytes"`
			SHA256 string `json:"sha256"`
		} `json:"parts"`
	}
	if err := json.Unmarshal([]byte(ts.ReadFile(filepath.Join(dir, "manifest.json"))), &m); err != nil {
		ts.Fatalf("%s/manifest.json: %v", args[0], err)
	}
	if len(m.Parts) == 0 {
		ts.Fatalf("%s/manifest.json names no part", args[0])
	}
	listed := map[string]bool{}
	var scans, counts, schemas []string
	for i, p := range m.Parts {
		path := filepath.Join(dir, filepath.FromSlash(p.Path))
		listed[path] = true
		b, err := os.ReadFile(path)
		if err != nil {
			ts.Fatalf("manifest part %s: %v", p.Path, err)
		}
		sum := sha256.Sum256(b)
		if int64(len(b)) != p.Bytes || hex.EncodeToString(sum[:]) != p.SHA256 {
			ts.Fatalf("manifest part %s: %d bytes, sha256 %x; the manifest says %d bytes, sha256 %s",
				p.Path, len(b), sum, p.Bytes, p.SHA256)
		}
		scans = append(scans, fmt.Sprintf("SELECT %d AS __part, file_row_number AS __row, * FROM read_parquet(%s, file_row_number = true)",
			i, sqlString(filepath.ToSlash(path))))
		counts = append(counts, fmt.Sprintf("(%d, %s, %d)", i, sqlString(p.Path), p.Rows))
		schemas = append(schemas, fmt.Sprintf("SELECT %d AS part, %s AS path, list(s) AS nodes FROM (SELECT * EXCLUDE (file_name) FROM parquet_schema(%s)) s",
			i, sqlString(p.Path), sqlString(filepath.ToSlash(path))))
	}
	others, _ := filepath.Glob(filepath.Join(dir, "data", "*.parquet"))
	for _, p := range others {
		if !listed[p] {
			ts.Fatalf("%s holds a part the manifest does not name: %s", args[0], filepath.Base(p))
		}
	}
	first := sqlString(filepath.ToSlash(filepath.Join(dir, filepath.FromSlash(m.Parts[0].Path))))
	setup := strings.Join([]string{
		"CREATE TEMP TABLE __schemas AS " + strings.Join(schemas, " UNION ALL "),
		"CREATE TEMP TABLE __check AS SELECT error(printf('part %s has another Parquet schema than part %s: %s; %s', b.path, a.path, CAST(b.nodes AS VARCHAR), CAST(a.nodes AS VARCHAR))) " +
			"FROM __schemas a, __schemas b WHERE a.part = 0 AND b.nodes IS DISTINCT FROM a.nodes",
		"DROP TABLE __check",
		"DROP TABLE __schemas",
		"CREATE TEMP TABLE __rows AS " + strings.Join(scans, " UNION ALL ") + " ORDER BY __part, __row",
		"CREATE TEMP TABLE __check AS SELECT error(printf('part %s has %d rows; the manifest says %d', m.path, coalesce(c.n, 0), m.n)) " +
			"FROM (VALUES " + strings.Join(counts, ", ") + ") m(part, path, n) " +
			"LEFT JOIN (SELECT __part AS part, count(*) AS n FROM __rows GROUP BY __part) c USING (part) WHERE coalesce(c.n, 0) <> m.n",
		"CREATE TEMP TABLE ds AS SELECT * EXCLUDE (__part, __row, file_row_number) FROM __rows ORDER BY __part, __row",
		"CREATE TEMP TABLE ds_schema AS SELECT name, type, repetition_type, converted_type FROM parquet_schema(" + first + ") WHERE num_children IS NULL",
		"DROP TABLE __rows",
	}, ";\n") + ";\n"
	cmd := exec.Command(duckdb, "-no-init", "-batch", "-bail", "-list", "-noheader", "-nullvalue", "NULL", "-separator", "\t")
	cmd.Stdin = strings.NewReader(setup + args[1] + ";\n")
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil || stderr.Len() > 0 {
		ts.Fatalf("duckdb: %v\n%s", err, stderr.String())
	}
	// DuckDB's list mode ends its lines with CRLF on Windows; the scripts
	// compare LF, as swath writes.
	ts.Stdout().Write(bytes.ReplaceAll(stdout.Bytes(), []byte("\r\n"), []byte("\n")))
}

func sqlString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// replay FLAGS... starts swath-replay serve --port 0 FLAGS in the current
// directory, waits for its startup line and sets $REPLAY to the endpoint
// it reports. The server writes its stderr straight into a file, so a line
// it logs before answering a request is there once the answer arrives
// (replaylog reads it). The server is killed when the script ends.
func cmdReplay(ts *testscript.TestScript, neg bool, args []string, bin string) {
	if neg {
		ts.Fatalf("unsupported: ! replay")
	}
	logf, err := os.CreateTemp(filepath.Join(ts.Getenv("WORK"), ".tmp"), "replay-*.log")
	ts.Check(err)
	cmd := exec.Command(bin, append([]string{"serve", "--port", "0"}, args...)...)
	cmd.Dir = ts.MkAbs(".")
	cmd.Stderr = logf
	err = cmd.Start()
	logf.Close()
	ts.Check(err)
	exited := make(chan struct{})
	go func() {
		cmd.Wait()
		close(exited)
	}()
	ts.Defer(func() {
		cmd.Process.Kill()
		<-exited
	})
	stderr := func() string {
		b, _ := os.ReadFile(logf.Name())
		return string(b)
	}
	deadline := time.After(replayStartup)
	tick := time.NewTicker(pollEvery)
	defer tick.Stop()
	for {
		for _, line := range strings.SplitAfter(stderr(), "\n") {
			rest, ok := strings.CutPrefix(line, "swath_replay endpoint=")
			if !ok || !strings.HasSuffix(line, "\n") {
				continue
			}
			endpoint, _, _ := strings.Cut(rest, " ")
			ts.Logf("swath-replay: %s", strings.TrimSuffix(line, "\n"))
			ts.Setenv("REPLAY", endpoint)
			state(ts).logs[endpoint] = &replayLog{path: logf.Name()}
			return
		}
		select {
		case <-exited:
			ts.Fatalf("swath-replay exited before its startup line (%v):\n%s", cmd.ProcessState, stderr())
		case <-deadline:
			ts.Fatalf("swath-replay printed no startup line within %v:\n%s", replayStartup, stderr())
		case <-tick.C:
		}
	}
}

// replaylog ENDPOINT FILE writes to FILE the lines the replay server at
// ENDPOINT has logged since it started, or since the previous replaylog of
// it: whole lines only, in the order the server wrote them. Started with
// --slow-request-log-ms 0, the server logs every listing request it
// answers, before the answer, with its shape, prefix, start-after, whether
// it carried a continuation token and the entries it returned.
func cmdReplaylog(ts *testscript.TestScript, neg bool, args []string) {
	if neg || len(args) != 2 {
		ts.Fatalf("usage: replaylog endpoint file")
	}
	l := state(ts).logs[args[0]]
	if l == nil {
		ts.Fatalf("replaylog: no replay server was started at %s", args[0])
	}
	b, err := os.ReadFile(l.path)
	ts.Check(err)
	b = b[l.offset:]
	b = b[:bytes.LastIndexByte(b, '\n')+1]
	l.offset += int64(len(b))
	ts.Check(os.WriteFile(ts.MkAbs(args[1]), b, 0o644))
}

// signals are the signals signal sends.
var signals = map[string]os.Signal{
	"INT":  os.Interrupt,
	"TERM": syscall.SIGTERM,
	"HUP":  syscall.SIGHUP,
	"KILL": os.Kill,
}

// handleIgnoredSignals gives the programs the scripts start the default
// disposition of the signals signal sends, whatever this process inherited.
// Under nohup it inherits SIGHUP ignored, which would pass on to them (swath
// leaves an ignored SIGHUP ignored, docs/reference/exit-codes.md). A Go
// process starts its children with the signals it handles reset to the
// default, so this process handles each signal it inherited ignored, and
// goes on ignoring it.
func handleIgnoredSignals() {
	var ignored []os.Signal
	for _, sig := range signals {
		if sig != os.Kill && signal.Ignored(sig) {
			ignored = append(ignored, sig)
		}
	}
	if len(ignored) == 0 {
		return
	}
	c := make(chan os.Signal, 1)
	signal.Notify(c, ignored...)
	go func() {
		for range c {
		}
	}()
}

// signal SIG PATH WANT PROGRAM [ARGS...] runs PROGRAM as exec does, waits
// until PATH exists, sends PROGRAM the signal SIG (INT, TERM, HUP or KILL)
// and requires it to end as WANT says: an exit status (130), or "killed"
// for a process the signal ended (on Unix; Windows has no such status).
// Each wait is bounded; PROGRAM ending before PATH exists fails.
func cmdSignal(ts *testscript.TestScript, neg bool, args []string) {
	if neg || len(args) < 4 {
		ts.Fatalf("usage: signal INT|TERM|HUP|KILL path want program [args...]")
	}
	sig, ok := signals[args[0]]
	if !ok {
		ts.Fatalf("signal: unknown signal %q", args[0])
	}
	path := ts.MkAbs(args[1])
	cmd := command(ts, args[3], args[4:])
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	ts.Check(cmd.Start())
	done := make(chan struct{})
	go func() {
		cmd.Wait()
		close(done)
	}()
	defer func() {
		ts.Stdout().Write(stdout.Bytes())
		ts.Stderr().Write(stderr.Bytes())
	}()
	deadline := time.After(processWait)
	tick := time.NewTicker(pollEvery)
	defer tick.Stop()
	for waiting := true; waiting; {
		select {
		case <-done:
			ts.Fatalf("%s ended (%v) before %s existed\n%s", args[3], cmd.ProcessState, args[1], stderr.String())
		case <-deadline:
			cmd.Process.Kill()
			<-done
			ts.Fatalf("%s did not exist within %v", args[1], processWait)
		case <-tick.C:
			_, err := os.Stat(path)
			waiting = err != nil
		}
	}
	ts.Check(cmd.Process.Signal(sig))
	awaitEnd(ts, cmd, done, args[2], sig, "after SIG"+args[0])
}

// head N WANT PROGRAM [ARGS...] runs PROGRAM as exec does with its stdout
// on a pipe, reads N lines from it and closes it, as head -n N does, and
// requires PROGRAM to end as WANT says (an exit status) within a bound.
// The N lines are head's stdout, and PROGRAM's stderr its stderr.
func cmdHead(ts *testscript.TestScript, neg bool, args []string) {
	if neg || len(args) < 3 {
		ts.Fatalf("usage: head n want program [args...]")
	}
	n, err := strconv.Atoi(args[0])
	if err != nil || n < 1 {
		ts.Fatalf("head: want a positive line count, got %q", args[0])
	}
	r, w, err := os.Pipe()
	ts.Check(err)
	cmd := command(ts, args[2], args[3:])
	var stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = w, &stderr
	err = cmd.Start()
	w.Close()
	if err != nil {
		r.Close()
		ts.Fatalf("%s: %v", args[2], err)
	}
	done := make(chan struct{})
	go func() {
		cmd.Wait()
		close(done)
	}()
	defer func() { ts.Stderr().Write(stderr.Bytes()) }()
	// The lines are read aside, so that a program that writes fewer than N
	// and neither ends nor closes its stdout is killed after processWait.
	type read struct {
		lines []byte
		n     int
		err   error
	}
	got := make(chan read, 1)
	go func() {
		br := bufio.NewReader(r)
		var lines []byte
		for i := 0; i < n; i++ {
			line, err := br.ReadString('\n')
			lines = append(lines, line...)
			if err != nil {
				got <- read{lines, i, err}
				return
			}
		}
		got <- read{lines, n, nil}
	}()
	var res read
	select {
	case res = <-got:
	case <-time.After(processWait):
		cmd.Process.Kill()
		<-done
		r.Close()
		res = <-got
		ts.Stdout().Write(res.lines)
		ts.Fatalf("%s wrote %d lines within %v, not %d", args[2], res.n, processWait, n)
	}
	r.Close()
	ts.Stdout().Write(res.lines)
	if res.err != nil {
		if !endWithin(cmd, done) {
			ts.Fatalf("%s wrote %d lines, not %d (%v), and did not end within %v", args[2], res.n, n, res.err, processWait)
		}
		ts.Fatalf("%s wrote %d lines, not %d (%v)", args[2], res.n, n, res.err)
	}
	awaitEnd(ts, cmd, done, args[1], nil, "after its reader closed")
}

// endWithin waits, within processWait, for cmd to end (done closed), and
// otherwise kills it and reports false once it has ended.
func endWithin(cmd *exec.Cmd, done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	case <-time.After(processWait):
		cmd.Process.Kill()
		<-done
		return false
	}
}

// awaitEnd waits, within processWait, for cmd to end and requires it to end
// as want says: an exit status, or "killed" by the signal sig.
func awaitEnd(ts *testscript.TestScript, cmd *exec.Cmd, done <-chan struct{}, want string, sig os.Signal, after string) {
	if !endWithin(cmd, done) {
		ts.Fatalf("%s did not end within %v %s", filepath.Base(cmd.Path), processWait, after)
	}
	ws, _ := cmd.ProcessState.Sys().(syscall.WaitStatus)
	got := strconv.Itoa(cmd.ProcessState.ExitCode())
	if ws.Signaled() {
		got = "killed"
		if sig == nil || ws.Signal() != sig {
			got = "killed by " + ws.Signal().String()
		}
	}
	if got != want {
		ts.Fatalf("%s ended %s (%v), want %s", filepath.Base(cmd.Path), got, cmd.ProcessState, want)
	}
}

// command is PROGRAM ARGS as exec would run it: found on the script's PATH,
// in the script's directory, with the script's environment (the variables
// it started with, as env has set them since).
func command(ts *testscript.TestScript, program string, args []string) *exec.Cmd {
	path := program
	if filepath.Base(program) == program {
		path = ""
		for _, dir := range filepath.SplitList(ts.Getenv("PATH")) {
			for _, ext := range []string{"", ".exe"} {
				if fi, err := os.Stat(filepath.Join(dir, program+ext)); err == nil && fi.Mode().IsRegular() {
					path = filepath.Join(dir, program+ext)
					break
				}
			}
			if path != "" {
				break
			}
		}
		if path == "" {
			ts.Fatalf("%s: not found on the script's PATH", program)
		}
	}
	cmd := exec.Command(path, args...)
	// Wait returns at most this long after PROGRAM ends, even if a process
	// it started still holds its stderr.
	cmd.WaitDelay = processWait
	cmd.Dir = ts.MkAbs(".")
	for _, name := range state(ts).envNames {
		cmd.Env = append(cmd.Env, name+"="+ts.Getenv(name))
	}
	cmd.Env = append(cmd.Env, "PWD="+cmd.Dir)
	return cmd
}
