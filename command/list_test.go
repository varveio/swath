// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/internal/command"
	"github.com/varveio/swath/internal/faults"
	"github.com/varveio/swath/internal/journal"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/s3/s3test"
)

// tree is a directory holding the files names, empty.
func tree(t *testing.T, names ...string) string {
	t.Helper()
	dir := t.TempDir()
	for _, n := range names {
		p := filepath.Join(dir, filepath.FromSlash(n))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(n), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// TestListOptionsCoverFlags: every flag swath list takes is one field's,
// and every field's flag is one swath list takes, so that a flag added to
// the command fails here until ListOptions has it.
func TestListOptionsCoverFlags(t *testing.T) {
	taken := command.ListFlags()
	var tagged []string
	ty := reflect.TypeFor[ListOptions]()
	for i := range ty.NumField() {
		if tag := ty.Field(i).Tag.Get("flag"); tag != "" {
			tagged = append(tagged, strings.Split(tag, ",")...)
		}
	}
	for _, n := range taken {
		if !slices.Contains(tagged, n) {
			t.Errorf("--%s: no ListOptions field", n)
		}
	}
	for _, n := range tagged {
		if !slices.Contains(taken, n) {
			t.Errorf("ListOptions names --%s, which swath list does not take", n)
		}
	}
	if sorted := slices.Sorted(slices.Values(tagged)); len(slices.Compact(sorted)) != len(tagged) {
		t.Errorf("a flag is named twice: %v", tagged)
	}
}

// TestListArgs: options become --name=value flags, which take any value as
// it is, and the source comes after --, which takes any operand as it is.
func TestListArgs(t *testing.T) {
	zero, off, hour := int64(0), false, time.Hour
	since := time.Date(2026, 1, 2, 3, 4, 5, 6, time.FixedZone("x", 3600))
	got := listArgs(&ListOptions{
		Source: "-dir", Output: "-o.tsv", Include: []string{"a/*", "-b"}, MinSize: &zero, MaxSize: &zero, StallTimeout: &hour,
		ModifiedSince: since, Verbose: 2, ProgressLine: &off, S3FIPS: &off, S3PathStyle: new(true), RequestRate: 2.5,
		SortBuffer: 1 << 30, Overwrite: true,
	})
	want := []string{"--output=-o.tsv", "--overwrite", "--include=a/*", "--include=-b", "--min-size=0", "--max-size=0", "--modified-since=2026-01-02T03:04:05.000000006+01:00",
		"--sort-buffer=1073741824", "--stall-timeout=" + source.FormatDuration(time.Hour), "--request-rate=2.5", "--verbose", "--verbose", "--no-progress",
		"--s3-path-style", "--s3-fips=false", "--", "-dir"}
	if !slices.Equal(got, want) {
		t.Errorf("args\n got %q\nwant %q", got, want)
	}
	if d, err := source.ParseDuration(source.FormatDuration(90 * time.Second)); err != nil || d != 90*time.Second {
		t.Errorf("a duration does not survive its flag: %v %v", d, err)
	}
	if got := listArgs(&ListOptions{}); len(got) != 0 {
		t.Errorf("zero options: %q", got)
	}
}

// TestListCompleted: a run writes what swath list writes, ends completed,
// and hands its progress, the final one last.
func TestListCompleted(t *testing.T) {
	src := tree(t, "a", "b/c", "-d")
	var stdout bytes.Buffer
	var mu sync.Mutex
	var seen []ListProgress
	r, err := List(context.Background(), ListOptions{Source: src, Format: "tsv", Columns: []string{"key"}, NoHeader: true, Stdout: &stdout,
		Progress: func(p ListProgress) { mu.Lock(); seen = append(seen, p); mu.Unlock() }})
	if err != nil {
		t.Fatal(err)
	}
	if want := (ListResult{Status: ListCompleted, Class: "ok", ExitCode: 0, Complete: true}); r != want {
		t.Errorf("result %+v, want %+v", r, want)
	}
	if got, want := stdout.String(), "-d\na\nb/\nb/c\n"; got != want {
		t.Errorf("rows %q, want %q", got, want)
	}
	if len(seen) == 0 {
		t.Fatal("no progress")
	}
	if last := seen[len(seen)-1]; last.RowsFetched != 4 || last.StartedAt.IsZero() {
		t.Errorf("final progress %+v", last)
	}
}

// TestListDataset: a dataset run leaves the dataset and its outcome as
// swath list does, and a second run into it is refused, not in use.
func TestListDataset(t *testing.T) {
	src := tree(t, "a", "b")
	out := filepath.Join(t.TempDir(), "ds")
	if _, err := List(context.Background(), ListOptions{Source: src, Output: out}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(out, "manifest.json")); err != nil {
		t.Fatal(err)
	}
	r, err := List(context.Background(), ListOptions{Source: src, Output: out})
	if r.Class != "refused" || r.ExitCode != 2 || r.Status != ListFailed || errors.Is(err, ErrOutputInUse) || errors.Is(err, ErrSourceNotFound) {
		t.Errorf("a second run: %+v %v", r, err)
	}
	var le *ListError
	if !errors.As(err, &le) || le.Result != r {
		t.Errorf("not a ListError with its result: %#v", err)
	}
}

// TestListOutputInUse: an output whose lock another run holds is
// ErrOutputInUse, and that run's files are left as they are.
func TestListOutputInUse(t *testing.T) {
	src := tree(t, "a")
	out := filepath.Join(t.TempDir(), "ds")
	lock, err := journal.TakeLock(out)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release()
	var stderr bytes.Buffer
	r, err := List(context.Background(), ListOptions{Source: src, Output: out, Overwrite: true, Stderr: &stderr})
	if !errors.Is(err, ErrOutputInUse) || errors.Is(err, ErrSourceNotFound) || errors.Is(err, ErrSourceDenied) {
		t.Fatalf("err %v", err)
	}
	if r.Status != ListFailed || r.ExitCode != 1 || r.Class != "fatal" {
		t.Errorf("result %+v", r)
	}
	if !strings.Contains(stderr.String(), "swath: ") {
		t.Errorf("stderr has no error line: %q", stderr.String())
	}
}

// TestListSourceNotFound: a local root and a bucket that do not exist are
// ErrSourceNotFound; an output that cannot be written, or a missing file
// the run reads besides its source, is not.
func TestListSourceNotFound(t *testing.T) {
	s3test.NoCredentials(t)
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	t.Setenv("AWS_ACCESS_KEY_ID", "key") // a bucket output's requests are signed
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	endpoint := s3test.Bucket(t, s3test.Keys(10, 2)) // serves bucket bkt only
	for _, c := range []struct {
		name string
		opts ListOptions
	}{
		{"local", ListOptions{Source: filepath.Join(t.TempDir(), "nothing")}},
		{"local, a dataset", ListOptions{Source: filepath.Join(t.TempDir(), "nothing"), Output: filepath.Join(t.TempDir(), "ds")}},
		{"s3", ListOptions{Source: "s3://nobucket/p/", EndpointURL: endpoint, S3PathStyle: new(true), Anonymous: true, Workers: 1,
			Output: filepath.Join(t.TempDir(), "ds")}},
		// A bucket output that is there: the source's failure is still the source's.
		{"s3, a bucket output", ListOptions{Source: "s3://nobucket/p/", EndpointURL: endpoint, S3PathStyle: new(true), Anonymous: true, Workers: 1,
			Output: "s3://bkt/ds-s3/", OutputEndpointURL: endpoint, WorkDir: filepath.Join(t.TempDir(), "work")}},
		{"local, a bucket output", ListOptions{Source: filepath.Join(t.TempDir(), "nothing"),
			Output: "s3://bkt/ds-local/", OutputEndpointURL: endpoint, WorkDir: filepath.Join(t.TempDir(), "work")}},
	} {
		t.Run(c.name, func(t *testing.T) {
			r, err := List(context.Background(), c.opts)
			if !errors.Is(err, ErrSourceNotFound) || errors.Is(err, ErrOutputInUse) || r.Status != ListFailed || r.ExitCode != 1 {
				t.Errorf("%+v %v", r, err)
			}
		})
	}
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	for name, opts := range map[string]ListOptions{
		"an output below a file": {Source: tree(t, "a"), Output: filepath.Join(file, "ds")},
		"a --seed-from not there": {Source: "s3://bkt/", EndpointURL: endpoint, S3PathStyle: new(true), Anonymous: true, Workers: 1,
			SeedFrom: filepath.Join(t.TempDir(), "nothing"), Output: filepath.Join(t.TempDir(), "ds")},
		"a --ca-bundle not there": {Source: "s3://bkt/p/", EndpointURL: endpoint, S3PathStyle: new(true), Anonymous: true, Workers: 1,
			CABundle: filepath.Join(t.TempDir(), "nothing.pem"), Output: filepath.Join(t.TempDir(), "ds")},
		"an output bucket not there": {Source: "s3://bkt/", EndpointURL: endpoint, S3PathStyle: new(true), Anonymous: true, Workers: 1,
			Output: "s3://nobucket/ds/", OutputEndpointURL: endpoint, WorkDir: filepath.Join(t.TempDir(), "work")},
		"an output bucket not there, a local source": {Source: tree(t, "a"),
			Output: "s3://nobucket/ds/", OutputEndpointURL: endpoint, WorkDir: filepath.Join(t.TempDir(), "work")},
	} {
		r, err := List(context.Background(), opts)
		if err == nil || errors.Is(err, ErrSourceNotFound) || errors.Is(err, ErrSourceDenied) {
			t.Errorf("%s: %+v %v", name, r, err)
		}
	}
}

// TestListSourceDenied: a local root that cannot be opened is
// ErrSourceDenied; an output that cannot be made is not.
func TestListSourceDenied(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("needs a directory this user cannot open")
	}
	src := tree(t, "a")
	if err := os.Chmod(src, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(src, 0o755) })
	r, err := List(context.Background(), ListOptions{Source: src, Output: filepath.Join(t.TempDir(), "ds")})
	if !errors.Is(err, ErrSourceDenied) || errors.Is(err, ErrSourceNotFound) || r.Status != ListFailed {
		t.Errorf("%+v %v", r, err)
	}
	// The output's own permission error is not the source's.
	parent := t.TempDir()
	if err := os.Chmod(parent, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(parent, 0o755) })
	r, err = List(context.Background(), ListOptions{Source: tree(t, "a"), Output: filepath.Join(parent, "ds")})
	if err == nil || errors.Is(err, ErrSourceDenied) {
		t.Errorf("an output that cannot be made: %+v %v", r, err)
	}
}

// deniedBucket serves bucket bkt, and answers every request for bucket
// denied 403 AccessDenied.
func deniedBucket(t *testing.T) string {
	h := s3test.Handler(s3test.Backend(t, s3test.Keys(10, 2)))
	return s3test.Serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/denied" || strings.HasPrefix(r.URL.Path, "/denied/") {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>`))
			return
		}
		h.ServeHTTP(w, r)
	}))
}

// TestListSourceDeniedBucketOutput: a bucket source that refuses access is
// ErrSourceDenied whether the output is a directory or a bucket; a bucket
// output that refuses it is not.
func TestListSourceDeniedBucketOutput(t *testing.T) {
	s3test.NoCredentials(t)
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	t.Setenv("AWS_ACCESS_KEY_ID", "key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	endpoint := deniedBucket(t)
	src := ListOptions{Source: "s3://denied/p/", EndpointURL: endpoint, S3PathStyle: new(true), Workers: 1}
	for name, out := range map[string]ListOptions{
		"a directory output": {Output: filepath.Join(t.TempDir(), "ds")},
		"a bucket output":    {Output: "s3://bkt/ds/", OutputEndpointURL: endpoint, WorkDir: filepath.Join(t.TempDir(), "work")},
	} {
		opts := src
		opts.Output, opts.OutputEndpointURL, opts.WorkDir = out.Output, out.OutputEndpointURL, out.WorkDir
		r, err := List(context.Background(), opts)
		if !errors.Is(err, ErrSourceDenied) || errors.Is(err, ErrSourceNotFound) || r.Status != ListFailed || r.Cause != "access_denied" {
			t.Errorf("%s: %+v %v", name, r, err)
		}
	}
	r, err := List(context.Background(), ListOptions{Source: "s3://bkt/", EndpointURL: endpoint, S3PathStyle: new(true), Workers: 1,
		Output: "s3://denied/ds/", OutputEndpointURL: endpoint, WorkDir: filepath.Join(t.TempDir(), "work")})
	if err == nil || errors.Is(err, ErrSourceDenied) || errors.Is(err, ErrSourceNotFound) {
		t.Errorf("a bucket output that refuses access: %+v %v", r, err)
	}
}

// TestListUsage: options the command refuses are ErrUsage, exit 2, and
// nothing runs.
func TestListUsage(t *testing.T) {
	out := filepath.Join(t.TempDir(), "ds")
	for _, opts := range []ListOptions{
		{Source: tree(t, "a"), Output: out, Format: "csv"},
		{Source: "", Output: out},
		{Source: tree(t, "a"), Output: out, AzureAssumeFlat: true}, // another provider's flag
	} {
		r, err := List(context.Background(), opts)
		if !errors.Is(err, ErrUsage) || r.ExitCode != 2 || r.Class != "usage" || r.Status != ListFailed {
			t.Errorf("%+v: %+v %v", opts, r, err)
		}
	}
	if _, err := os.Stat(out); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a refused run made its output: %v", err)
	}
}

// TestListCanceled: a run whose context is canceled while a request is in
// flight ends canceled, and its error is the context's, whether the
// context was canceled with a cause or without.
func TestListCanceled(t *testing.T) {
	s3test.NoCredentials(t)
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	for _, cause := range []error{nil, errors.New("the caller's cause")} {
		reached := make(chan struct{})
		var once sync.Once
		srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			once.Do(func() { close(reached) })
			<-r.Context().Done() // held until the run lets go of the request
		}))
		ctx, cancel := context.WithCancelCause(context.Background())
		go func() {
			<-reached
			cancel(cause)
		}()
		out := filepath.Join(t.TempDir(), "ds")
		r, err := List(ctx, ListOptions{Source: "s3://bkt/p/", EndpointURL: srv.URL, S3PathStyle: new(true), Anonymous: true, Workers: 1, Output: out})
		srv.Close()
		select {
		case <-reached:
		default:
			t.Fatal("no request reached the server")
		}
		if r.Status != ListCanceled || !errors.Is(err, context.Canceled) || errors.Is(err, ErrSourceNotFound) || r.ExitCode == 0 {
			t.Errorf("cause %v: %+v %v", cause, r, err)
		}
	}
}

// TestListProgressWhileRunning: a run held in its first request hands
// Progress its progress meanwhile, as the progress file has it.
func TestListProgressWhileRunning(t *testing.T) {
	s3test.NoCredentials(t)
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	held := make(chan struct{})
	var once sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(held) })
		<-r.Context().Done() // held until the run lets go of the request
	}))
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := filepath.Join(t.TempDir(), "ds")
	var during *ListProgress
	// MaxDuration ends a run that no progress reached, which fails here.
	r, _ := List(ctx, ListOptions{Source: "s3://bkt/p/", EndpointURL: srv.URL, S3PathStyle: new(true), Anonymous: true, Workers: 1, Output: out,
		MaxDuration: 20 * time.Second,
		Progress: func(p ListProgress) {
			select {
			case <-held:
			default:
				return // not held yet
			}
			if during == nil && p.State == "listing" { // not the final progress, which says failed
				during = &p
				cancel() // the run ends once a progress came while it was held
			}
		}})
	if during == nil || r.Status != ListCanceled || r.Class == "deadline" {
		t.Fatalf("no progress while the run was held: %+v", r)
	}
	if during.AttemptID == "" || during.RunID == "" || during.StartedAt.IsZero() {
		t.Errorf("progress %+v", *during)
	}
	var file struct {
		AttemptID string `json:"attempt_id"`
		RunID     string `json:"run_id"`
	}
	b, err := os.ReadFile(filepath.Join(out, ".swath", "progress.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &file); err != nil || file.AttemptID != during.AttemptID || file.RunID != during.RunID {
		t.Errorf("progress.json %s, the callback's %+v", b, *during)
	}
}

// TestListLogStartsAfresh: a call's refusal is printed as its own options
// ask, as swath list prints it, whatever the call before it asked for.
func TestListLogStartsAfresh(t *testing.T) {
	src := tree(t, "a")
	var first bytes.Buffer
	if _, err := List(context.Background(), ListOptions{Source: src, LogFormat: "json", Quiet: 2, Stderr: &first}); err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	_, err := List(context.Background(), ListOptions{Source: src, Format: "csv", Stderr: &stderr})
	if !errors.Is(err, ErrUsage) {
		t.Fatalf("not refused: %v", err)
	}
	if got := stderr.String(); !strings.HasPrefix(got, "swath: ") {
		t.Errorf("refusal %q, want a text line", got)
	}
	stderr.Reset()
	if _, err := List(context.Background(), ListOptions{Source: src, Format: "csv", LogFormat: "json", Stderr: &stderr}); !errors.Is(err, ErrUsage) {
		t.Fatalf("not refused: %v", err)
	}
	if got := stderr.String(); !strings.HasPrefix(got, "{") {
		t.Errorf("refusal %q, want a JSON event", got)
	}
}

// TestListMinSize: a nil MinSize gives no --min-size, and an explicit zero
// gives --min-size=0, which swath list refuses with --metadata=names (no
// size to filter on).
func TestListMinSize(t *testing.T) {
	src := tree(t, "a")
	if _, err := List(context.Background(), ListOptions{Source: src, Metadata: "names"}); err != nil {
		t.Errorf("nil: %v", err)
	}
	r, err := List(context.Background(), ListOptions{Source: src, Metadata: "names", MinSize: new(int64(0))})
	if !errors.Is(err, ErrUsage) || r.ExitCode != 2 {
		t.Errorf("zero: %+v %v", r, err)
	}
}

// writeFunc is a writer whose type cannot be compared.
type writeFunc func([]byte) (int, error)

func (f writeFunc) Write(b []byte) (int, error) { return f(b) }

// TestListStderrUncomparable: a Stderr whose type cannot be compared takes
// the command's lines, call after call.
func TestListStderrUncomparable(t *testing.T) {
	var mu sync.Mutex
	var got strings.Builder
	w := writeFunc(func(b []byte) (int, error) { mu.Lock(); defer mu.Unlock(); return got.Write(b) })
	for i := range 2 {
		if _, err := List(context.Background(), ListOptions{Stderr: w}); !errors.Is(err, ErrUsage) {
			t.Fatalf("call %d: %v", i, err)
		}
		mu.Lock()
		if n := strings.Count(got.String(), "swath: "); n < i+1 {
			t.Errorf("call %d: stderr %q", i, got.String())
		}
		mu.Unlock()
	}
}

// TestListCanceledAfterFailure: a context canceled after the run failed,
// from its final progress, leaves the run failed with its reason.
func TestListCanceledAfterFailure(t *testing.T) {
	s3test.NoCredentials(t)
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	endpoint := s3test.Bucket(t, s3test.Keys(10, 2)) // serves bucket bkt only
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var final string
	r, err := List(ctx, ListOptions{Source: "s3://nobucket/p/", EndpointURL: endpoint, S3PathStyle: new(true), Anonymous: true, Workers: 1,
		Output: filepath.Join(t.TempDir(), "ds"),
		Progress: func(p ListProgress) {
			if p.State == "failed" {
				final = p.State
				cancel()
			}
		}})
	if final == "" || ctx.Err() == nil {
		t.Fatal("the final progress did not cancel")
	}
	if r.Status != ListFailed || !errors.Is(err, ErrSourceNotFound) || errors.Is(err, context.Canceled) {
		t.Errorf("%+v %v", r, err)
	}
}

// TestListFinalProgressAfterReport: the final progress comes once the
// command has ended: the report it was asked for is written. It is the
// only progress that says done, and the last.
func TestListFinalProgressAfterReport(t *testing.T) {
	src := tree(t, "a", "b")
	report := filepath.Join(t.TempDir(), "report.json")
	var mu sync.Mutex
	var reported []bool
	var states []string
	_, err := List(context.Background(), ListOptions{Source: src, Output: filepath.Join(t.TempDir(), "ds"), Report: report,
		Progress: func(p ListProgress) {
			_, err := os.Stat(report)
			mu.Lock()
			reported, states = append(reported, err == nil), append(states, p.State)
			mu.Unlock()
		}})
	if err != nil {
		t.Fatal(err)
	}
	if len(reported) == 0 || !reported[len(reported)-1] {
		t.Errorf("the final progress came before the report was written: %v", reported)
	}
	if n := slices.Index(states, "done"); n != len(states)-1 {
		t.Errorf("states %v: done before the final progress", states)
	}
}

// TestListQueuedCanceled: a call waiting for another call's run returns
// canceled when its context ends, and runs nothing; the run it waited for
// goes on.
func TestListQueuedCanceled(t *testing.T) {
	s3test.NoCredentials(t)
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	reached := make(chan struct{})
	var once sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(reached) })
		<-r.Context().Done() // held until the run lets go of the request
	}))
	defer srv.Close()
	first, stop := context.WithCancel(context.Background())
	defer stop()
	done := make(chan ListResult, 1)
	go func() {
		r, _ := List(first, ListOptions{Source: "s3://bkt/p/", EndpointURL: srv.URL, S3PathStyle: new(true), Anonymous: true, Workers: 1,
			Output: filepath.Join(t.TempDir(), "ds")})
		done <- r
	}()
	<-reached // the first call's run holds the turn
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	out := filepath.Join(t.TempDir(), "queued")
	r, err := List(ctx, ListOptions{Source: tree(t, "a"), Output: out})
	if r.Status != ListCanceled || !errors.Is(err, context.Canceled) || errors.Is(err, ErrUsage) || r.ExitCode == 0 {
		t.Errorf("queued: %+v %v", r, err)
	}
	if _, err := os.Stat(out); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a queued call ran: %v", err)
	}
	select {
	case r := <-done:
		t.Fatalf("the first run ended while held: %+v", r)
	default:
	}
	stop()
	if r := <-done; r.Status != ListCanceled {
		t.Errorf("first: %+v", r)
	}
	if _, err := List(context.Background(), ListOptions{Source: tree(t, "a")}); err != nil {
		t.Errorf("a call after both: %v", err)
	}
}

// TestListCanceledAfterRun: a context canceled once the run has failed,
// after inventory.Run returned and before the command finalizes the run,
// leaves the run failed with its reason: with no cause, and with a cause
// the run's own error matches (a missing local source, os.ErrNotExist).
func TestListCanceledAfterRun(t *testing.T) {
	s3test.NoCredentials(t)
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	endpoint := s3test.Bucket(t, s3test.Keys(10, 2)) // serves bucket bkt only
	for _, c := range []struct {
		name  string
		cause error
		opts  func(t *testing.T) ListOptions
	}{
		{"no cause", nil, func(*testing.T) ListOptions {
			return ListOptions{Source: "s3://nobucket/p/", EndpointURL: endpoint, S3PathStyle: new(true), Anonymous: true, Workers: 1}
		}},
		{"the run's error as the cause", os.ErrNotExist, func(t *testing.T) ListOptions {
			return ListOptions{Source: filepath.Join(t.TempDir(), "missing")}
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			var reached atomic.Bool
			defer faults.Set(func(p string) error {
				if p == "run-returned" {
					reached.Store(true)
					cancel(c.cause)
				}
				return nil
			})()
			opts := c.opts(t)
			opts.Output = filepath.Join(t.TempDir(), "ds")
			r, err := List(ctx, opts)
			if !reached.Load() {
				t.Fatal("the run did not return")
			}
			if r.Status != ListFailed || !errors.Is(err, ErrSourceNotFound) || errors.Is(err, context.Canceled) {
				t.Errorf("%+v %v", r, err)
			}
		})
	}
}

// TestListCanceledTurnFree: a call whose context has ended returns
// canceled and runs nothing, also when its turn is free. Each call's wait
// has both its turn and its context's end ready, either of which it may
// take; 64 calls take the turn at least once but with odds of 2^-64.
func TestListCanceledTurnFree(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	src, dir := tree(t, "a"), t.TempDir()
	for i := range 64 {
		out := filepath.Join(dir, strconv.Itoa(i))
		r, err := List(ctx, ListOptions{Source: src, Output: out})
		if r.Status != ListCanceled || !errors.Is(err, context.Canceled) {
			t.Fatalf("call %d: %+v %v", i, r, err)
		}
		if _, err := os.Stat(out); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("call %d ran: %v", i, err)
		}
	}
}

// TestListPanicReleasesTurn: a Stdout that panics, writing the report, and
// a caller that recovers leave the next call its turn.
func TestListPanicReleasesTurn(t *testing.T) {
	src := tree(t, "a")
	func() {
		defer func() {
			if recover() == nil {
				t.Error("the Stdout did not panic")
			}
		}()
		_, _ = List(context.Background(), ListOptions{Source: src, Output: filepath.Join(t.TempDir(), "ds"), Report: "-",
			Stdout: writeFunc(func([]byte) (int, error) { panic("the caller's Stdout") })})
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) // a guard: the next call waits for its turn
	defer cancel()
	if r, err := List(ctx, ListOptions{Source: src}); err != nil {
		t.Errorf("the next call: %+v %v", r, err)
	}
}
