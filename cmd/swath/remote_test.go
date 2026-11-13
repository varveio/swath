// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/dataset/datasettest"
	"github.com/varveio/swath/internal/faults"
	"github.com/varveio/swath/internal/journal"
	"github.com/varveio/swath/internal/listing"
	"github.com/varveio/swath/internal/source/s3/s3test"
	"github.com/varveio/swath/objfile/azblob/azfake"
	"github.com/varveio/swath/objfile/gcs/gcsfake"
	"github.com/varveio/swath/objfile/s3/s3fake"
)

// TestRemoteOutputRefusals: what a bucket -o refuses at parse time.
func TestRemoteOutputRefusals(t *testing.T) {
	work := filepath.Join(t.TempDir(), "w")
	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"-o", "az://acct/c/p"}, "--work-dir"},
		{[]string{"-o", "az://acct/c/p", "--work-dir", work, "--output-s3-profile", "p"}, "for an S3 destination"},
		{[]string{"-o", "gs://b/p", "--work-dir", work, "--output-azure-sas-token", "sig=x"}, "for an Azure destination"},
		{[]string{"-o", filepath.Join(t.TempDir(), "ds"), "--output-azure-sas-token", "sig=x"}, "apply to a dataset in a bucket"},
		{[]string{"-o", "s3://b/p"}, "--work-dir"},
		{[]string{"-o", "s3://b/p", "--work-dir", work, "--output-gcs-credentials-file", "x.json"}, "for a GCS destination"},
		{[]string{"-o", "gs://b/p", "--work-dir", work, "--output-s3-region", "eu-west-1"}, "for an S3 destination"},
		{[]string{"-o", "gs://b/p", "--work-dir", work, "--output-s3-profile", "p"}, "for an S3 destination"},
		{[]string{"-o", filepath.Join(t.TempDir(), "ds"), "--output-s3-profile", "p"}, "apply to a dataset in a bucket"},
		{[]string{"-o", "gs://b/p"}, "--work-dir"},
		{[]string{"-o", "gs://b/p", "--work-dir", work, "--overwrite"}, "not supported"},
		{[]string{"-o", "gs://b/p", "--work-dir", work, "--format", "tsv"}, "--format"},
		{[]string{"-o", "gs://B/p", "--work-dir", work}, "bucket"},
		{[]string{"-o", "gs://b/p", "--work-dir", "gs://b/w"}, "local"},
		{[]string{"-o", filepath.Join(t.TempDir(), "ds"), "--work-dir", work}, "--work-dir"},
		{[]string{"-o", filepath.Join(t.TempDir(), "ds"), "--output-gcs-credentials-file", "x.json"}, "apply to a dataset in a bucket"},
		{[]string{"--output-endpoint-url", "http://x/"}, "apply to a dataset in a bucket"},
		{[]string{"-o", "gs://b/p", "--work-dir", work, "--report", filepath.Join(work, dataset.RemoteCommitName)}, "commit.json"},
	} {
		_, _, err := parseList(append([]string{"s3://src"}, c.args...), io.Discard, command{})
		var u usageError
		if !errors.As(err, &u) || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%v: %v, want a usage error naming %q", c.args, err, c.want)
		}
	}
	o, _, err := parseList([]string{"s3://src", "-o", "gs://b/p", "--work-dir", work}, io.Discard, command{})
	if err != nil || o.remote == nil || o.remote.String() != "gs://b/p/" || cfgOut(o) != work {
		t.Fatalf("a gs:// -o: %v, %+v", err, o.remote)
	}
	o, _, err = parseList([]string{"s3://src", "-o", "s3://b/p", "--work-dir", work, "--output-s3-region", "eu-west-1", "--output-s3-profile", "w"}, io.Discard, command{})
	if err != nil || o.remote == nil || o.remote.String() != "s3://b/p/" || o.outputRegion != "eu-west-1" || o.outputProfile != "w" {
		t.Fatalf("an s3:// -o: %v, %+v", err, o.remote)
	}
	o, _, err = parseList([]string{"s3://src", "-o", "az://acct/c/p", "--work-dir", work, "--output-azure-sas-token", "sig=x"}, io.Discard, command{})
	if err != nil || o.remote == nil || o.remote.String() != "az://acct/c/p/" || o.outputSAS != "sig=x" {
		t.Fatalf("an az:// -o: %v, %+v", err, o.remote)
	}
}

// TestRemoteDatasetDirectoryNames exercises explicit directories, parent prefixes
// and bucket names through publication and resume of the recorded destination.
func TestRemoteDatasetDirectoryNames(t *testing.T) {
	datasettest.RequireJournal(t)
	captureStderr(t)
	s3test.NoCredentials(t)
	t.Setenv("STORAGE_EMULATOR_HOST", "")
	awsProfiles(t, "output")
	credentials := serviceAccount(t, "output")
	src := httptest.NewServer(s3test.Handler(s3test.Backend(t, s3test.Keys(3, 7))))
	t.Cleanup(src.Close)
	for _, dest := range []string{
		"s3://out/inventory.parquet/", "gs://out/inventory.parquet/", "az://acct/out/inventory.parquet/",
		"s3://out/inventory.PaRqUeT/", "gs://out/inventory.PaRqUeT/", "az://acct/out/inventory.PaRqUeT/",
		"s3://out/parent.parquet/inventory", "gs://out/parent.parquet/inventory", "az://acct/out/parent.parquet/inventory",
		"s3://out.parquet", "gs://out.parquet", "az://acct/out.parquet",
	} {
		t.Run(dest, func(t *testing.T) {
			work := filepath.Join(t.TempDir(), "inventory.parquet") + "/"
			o, _, err := parseList([]string{"s3://bkt", "-o", dest, "--work-dir", work}, io.Discard, command{})
			if err != nil || o.remote == nil || o.remote.String() != strings.TrimSuffix(dest, "/")+"/" {
				t.Fatalf("remote %+v, %v", o.remote, err)
			}
			var handler http.Handler
			var manifest func() ([]byte, bool)
			var auth []string
			key := o.remote.Prefix + dataset.ManifestName
			switch o.remote.Scheme {
			case "s3":
				f := s3fake.New(o.remote.Container)
				handler = f
				manifest = func() ([]byte, bool) { obj, ok := f.Get(key); return obj.Data, ok }
				auth = []string{"--output-s3-profile", "output"}
			case "gs":
				f := gcsfake.New(o.remote.Container)
				handler = f
				manifest = func() ([]byte, bool) { obj, ok := f.Get(key); return obj.Data, ok }
				auth = []string{"--output-gcs-credentials-file", credentials}
			case "az":
				f := azfake.New(o.remote.Account, o.remote.Container)
				handler = f
				manifest = func() ([]byte, bool) { obj, ok := f.Get(key); return obj.Data, ok }
				auth = []string{"--output-azure-sas-token", "sv=1&sig=OUTPUT"}
			}
			dst := httptest.NewServer(handler)
			t.Cleanup(dst.Close)
			endpoint := dst.URL
			if o.remote.Scheme == "gs" {
				endpoint += "/storage/v1/"
			}
			args := []string{"list", "s3://bkt", "--endpoint-url", src.URL, "--s3-path-style", "--anonymous", "--workers", "2",
				"-o", dest, "--work-dir", work, "--output-endpoint-url", endpoint, "--no-stats"}
			args = append(args, auth...)
			if o.remote.Scheme == "s3" {
				args = append(args, "--output-s3-region", "us-east-1")
			}
			// Stop after a durable part so resume must re-parse the recorded -o.
			restore := faults.Set(func(point string) error {
				if point == "recorded" {
					return faults.ErrCrash
				}
				return nil
			})
			err = dispatch(context.Background(), args, io.Discard)
			restore()
			if !errors.Is(err, faults.ErrCrash) {
				t.Fatalf("first attempt: %v", err)
			}
			if _, ok := manifest(); ok {
				t.Fatal("committed before resume")
			}
			if err := dispatch(context.Background(), append([]string{"resume", work}, auth...), io.Discard); err != nil {
				t.Fatalf("resume: %v", err)
			}
			data, ok := manifest()
			if !ok || !dataset.Committed(work) {
				t.Fatal("resume did not publish the dataset")
			}
			var m dataset.Manifest
			if err := json.Unmarshal(data, &m); err != nil {
				t.Fatal(err)
			}
			var rows int64
			for _, part := range m.Parts {
				rows += part.Rows
			}
			if rows != 3 {
				t.Fatalf("%d rows committed, want 3", rows)
			}
		})
	}
}

// serviceAccount writes a service account key whose token endpoint is a
// test server handing out token; it returns the key file.
func serviceAccount(t *testing.T, token string) string {
	t.Helper()
	tokens := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": token, "token_type": "Bearer", "expires_in": 3600})
	}))
	t.Cleanup(tokens.Close)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	der, _ := x509.MarshalPKCS8PrivateKey(key)
	b, _ := json.Marshal(map[string]string{"type": "service_account", "project_id": "p", "private_key_id": "k",
		"private_key": string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})), "client_email": "w@p.iam.gserviceaccount.com",
		"client_id": "1", "token_uri": tokens.URL})
	path := filepath.Join(t.TempDir(), "sa.json")
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestRemoteListAndResume: swath list -o gs://... writes the dataset to
// the bucket through its own credentials (the destination's token on every
// upload, none on the anonymous source's requests), is stopped once its
// first part is durable, and swath resume WORK_DIR, given a rotated
// credentials file, completes it there.
func TestRemoteListAndResume(t *testing.T) {
	datasettest.RequireJournal(t)
	captureStderr(t)
	t.Setenv("STORAGE_EMULATOR_HOST", "")
	s3test.NoCredentials(t)
	var srcAuth sync.Map
	h := s3test.Handler(s3test.Backend(t, s3test.Keys(20000, 7)))
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if a := r.Header.Get("Authorization"); a != "" {
			srcAuth.Store(a, true)
		}
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(src.Close)
	f := gcsfake.New("dst")
	var mu sync.Mutex
	dstAuth := map[string]int{}
	dst := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		dstAuth[r.Header.Get("Authorization")]++
		mu.Unlock()
		f.ServeHTTP(w, r)
	}))
	t.Cleanup(dst.Close)
	ctx := context.Background()
	work := filepath.Join(t.TempDir(), "work")
	args := []string{"s3://bkt", "--endpoint-url", src.URL, "--s3-path-style", "--anonymous", "--workers", "2", "--part-size", "4KiB",
		"-o", "gs://dst/ds", "--work-dir", work, "--output-endpoint-url", dst.URL + "/storage/v1/", "--output-gcs-credentials-file", serviceAccount(t, "first")}
	// The first attempt stops once its first part is durable (recorded
	// in the journal), whatever the machine's speed: a kill, as far as the
	// work directory and the bucket can tell.
	var recorded atomic.Int64
	restore := faults.Set(func(p string) error {
		if p == "recorded" && recorded.Add(1) == 1 {
			return faults.ErrCrash
		}
		return nil
	})
	err := run(ctx, args, io.Discard, false)
	restore()
	if !errors.Is(err, faults.ErrCrash) {
		t.Fatalf("first attempt: %v", err)
	}
	st, err := journal.Read(work)
	if err != nil || len(st.Records) != 1 || st.Header.Identity.Destination == nil || st.Records[0].Object == "" {
		t.Fatalf("the work directory's journal: %v, %+v", err, st)
	}
	if _, ok := f.Get("ds/" + dataset.ManifestName); ok {
		t.Fatal("committed before the resume")
	}
	if err := resume(ctx, []string{work, "--output-gcs-credentials-file", serviceAccount(t, "second"), "--max-duration", "0"}, io.Discard); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if _, ok := f.Get("ds/" + dataset.ManifestName); !ok || !dataset.Committed(work) {
		t.Fatalf("not committed: %v", f.Names())
	}
	var m dataset.Manifest
	o, _ := f.Get("ds/" + dataset.ManifestName)
	if err := json.Unmarshal(o.Data, &m); err != nil || len(m.Parts) < 2 {
		t.Fatalf("manifest: %v, %d parts", err, len(m.Parts))
	}
	var rows int64
	for _, p := range m.Parts {
		rows += p.Rows
	}
	if rows != 20000 {
		t.Fatalf("%d rows committed", rows)
	}
	mu.Lock()
	defer mu.Unlock()
	if dstAuth["Bearer first"] == 0 || dstAuth["Bearer second"] == 0 || dstAuth[""] != 0 {
		t.Fatalf("destination requests by Authorization: %v", dstAuth)
	}
	srcAuth.Range(func(k, _ any) bool {
		t.Fatalf("the anonymous source was sent credentials: %v", k)
		return false
	})
	if _, err := os.Stat(filepath.Join(work, dataset.DataDir)); err == nil {
		t.Fatal("the work directory holds data/")
	}
	// Resumed again, the complete run has nothing to do: exit 0.
	if err := resume(ctx, []string{work}, io.Discard); err != nil {
		t.Fatalf("resume of a complete run: %v", err)
	}
}

// TestRemoteOutcomes: how the failures of a dataset in a bucket end: a
// destination that holds a manifest, or overlaps the source, is refused
// (exit 2); an unresolved commit is resumable (exit 75) when the work
// directory's journal is intact; a candidate mismatch, a lost race and an
// altered retained part need a person (exit 1).
func TestRemoteOutcomes(t *testing.T) {
	work := t.TempDir()
	j, err := journal.Create(work, journal.Identity{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	j.Close()
	for _, c := range []struct {
		err       error
		class     string
		code      int
		resumable bool
	}{
		{listing.ErrDestinationExists, listing.ClassRefused, exitUsage, true},
		{listing.ErrOverlap, listing.ClassRefused, exitUsage, true},
		{fmt.Errorf("publish: %w", dataset.ErrCommitUnresolved), listing.ClassUnresolved, exitStuck, true},
		{fmt.Errorf("%w: %w", dataset.ErrCommitUnresolved, dataset.ErrCandidateMismatch), listing.ClassFatal, exitFatal, false},
		{&dataset.LostRaceError{}, listing.ClassFatal, exitFatal, false},
		{fmt.Errorf("resume: %w", dataset.ErrRemoteAltered), listing.ClassFatal, exitFatal, false},
	} {
		o := classify(c.err, nil, work)
		if o.Class != c.class || o.ExitCode != c.code || o.Resumable != c.resumable {
			t.Errorf("%v: %+v, want %s, exit %d, resumable %v", c.err, o, c.class, c.code, c.resumable)
		}
	}
}

// TestOutputEndpointRedaction: --output-endpoint-url's user information
// and query never reach a recorded command line: the command's canonical
// argv, the library's second pass, or the report of a run refused early.
func TestOutputEndpointRedaction(t *testing.T) {
	const secret = "SECRETVALUE"
	ep := "https://user:" + secret + "@example.invalid/storage/v1/?token=" + secret
	args := []string{"s3://src", "-o", "gs://dst/ds", "--work-dir", filepath.Join(t.TempDir(), "work"), "--output-endpoint-url", ep}
	o, _, err := parseList(args, io.Discard, command{})
	if err != nil {
		t.Fatal(err)
	}
	for _, argv := range [][]string{o.argv, redactArgv(o.argv), redactArgv([]string{"list", "--output-endpoint-url=" + ep, "--output-endpoint-url", ep, "s3://src"})} {
		if got := strings.Join(argv, " "); strings.Contains(got, secret) {
			t.Fatalf("the endpoint's credentials are in %s", got)
		}
	}
	report := filepath.Join(t.TempDir(), "report.json")
	captureStderr(t)
	_ = run(context.Background(), append(args, "--report", report), io.Discard, false)
	b, err := os.ReadFile(report)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), secret) {
		t.Fatalf("the report holds the endpoint's credentials: %s", b)
	}
}

// awsProfiles writes a shared config file of profiles, each name's access
// key id the name in upper case, and points AWS_CONFIG_FILE at it.
func awsProfiles(t *testing.T, names ...string) {
	t.Helper()
	var b strings.Builder
	for _, n := range names {
		b.WriteString("[profile " + n + "]\naws_access_key_id = " + strings.ToUpper(n) + "\naws_secret_access_key = s3cr3t\n")
	}
	path := filepath.Join(t.TempDir(), "config")
	if err := os.WriteFile(path, []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AWS_CONFIG_FILE", path)
}

// TestRemoteS3ListAndResume: swath list -o s3://... writes the dataset to
// an S3-compatible endpoint, path-style, signing with its own profile
// (the destination's key on every upload, no credential on the anonymous
// source's requests), is stopped by its deadline, and swath resume
// WORK_DIR, given another profile (rotated credentials), completes it
// there. The region given is frozen in the journal's identity.
func TestRemoteS3ListAndResume(t *testing.T) {
	datasettest.RequireJournal(t)
	captureStderr(t)
	s3test.NoCredentials(t)
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	awsProfiles(t, "first", "second")
	var srcAuth sync.Map
	h := s3test.Handler(s3test.Backend(t, s3test.Keys(60000, 7)))
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if a := r.Header.Get("Authorization"); a != "" {
			srcAuth.Store(a, true)
		}
		time.Sleep(100 * time.Millisecond) // 60 pages over 2 workers: the run outlasts its deadline
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(src.Close)
	f := s3fake.New("dst")
	var mu sync.Mutex
	dstKeys := map[string]int{}
	dst := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, _, _ := strings.Cut(strings.TrimPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 Credential="), "/")
		mu.Lock()
		dstKeys[key]++
		mu.Unlock()
		f.ServeHTTP(w, r)
	}))
	t.Cleanup(dst.Close)
	ctx := context.Background()
	work := filepath.Join(t.TempDir(), "work")
	args := []string{"s3://bkt", "--endpoint-url", src.URL, "--s3-path-style", "--anonymous", "--workers", "2", "--part-size", "4KiB",
		"-o", "s3://dst/ds", "--work-dir", work, "--output-endpoint-url", dst.URL, "--output-s3-profile", "first", "--output-s3-region", "eu-west-3"}
	if c := exitCode(t, run(ctx, append(args, "--max-duration", "1500ms"), io.Discard, false)); c != exitDeadline {
		t.Fatalf("first attempt: exit %d", c)
	}
	st, err := journal.Read(work)
	if err != nil || len(st.Records) == 0 || st.Header.Identity.Destination == nil || st.Header.Identity.Destination.Region != "eu-west-3" {
		t.Fatalf("the work directory's journal: %v, %+v", err, st.Header.Identity.Destination)
	}
	if _, ok := f.Get("ds/" + dataset.ManifestName); ok {
		t.Fatal("committed before the resume")
	}
	if err := resume(ctx, []string{work, "--output-s3-profile", "second", "--max-duration", "0"}, io.Discard); err != nil {
		t.Fatalf("resume: %v", err)
	}
	o, ok := f.Get("ds/" + dataset.ManifestName)
	if !ok || !dataset.Committed(work) {
		t.Fatalf("not committed: %v", f.Names())
	}
	var m dataset.Manifest
	if err := json.Unmarshal(o.Data, &m); err != nil || len(m.Parts) < 2 {
		t.Fatalf("manifest: %v, %d parts", err, len(m.Parts))
	}
	var rows int64
	for _, p := range m.Parts {
		rows += p.Rows
	}
	if rows != 60000 {
		t.Fatalf("%d rows committed", rows)
	}
	mu.Lock()
	defer mu.Unlock()
	if dstKeys["FIRST"] == 0 || dstKeys["SECOND"] == 0 || len(dstKeys) != 2 {
		t.Fatalf("destination requests by access key: %v", dstKeys)
	}
	srcAuth.Range(func(k, _ any) bool {
		t.Fatalf("the anonymous source was sent credentials: %v", k)
		return false
	})
	if len(f.Uploads()) != 0 {
		t.Fatalf("uploads left open: %v", f.Uploads())
	}
}

// TestRemoteAzureListAndResume: swath list -o az://... writes the dataset
// to an emulator-style endpoint (the account its first path segment),
// every request carrying the destination's SAS, is stopped by its
// deadline, and swath resume WORK_DIR, refused without the SAS (never
// recorded), completes it there given another (a rotated one). The
// account is frozen in the journal's identity; no file of the work
// directory holds a SAS.
func TestRemoteAzureListAndResume(t *testing.T) {
	datasettest.RequireJournal(t)
	captureStderr(t)
	s3test.NoCredentials(t)
	h := s3test.Handler(s3test.Backend(t, s3test.Keys(60000, 7)))
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond) // 60 pages over 2 workers: the run outlasts its deadline
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(src.Close)
	f := azfake.New("acct", "dst")
	var mu sync.Mutex
	sigs := map[string]int{}
	dst := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		sigs[r.URL.Query().Get("sig")]++
		mu.Unlock()
		f.ServeHTTP(w, r)
	}))
	t.Cleanup(dst.Close)
	ctx := context.Background()
	work := filepath.Join(t.TempDir(), "work")
	args := []string{"s3://bkt", "--endpoint-url", src.URL, "--s3-path-style", "--anonymous", "--workers", "2", "--part-size", "4KiB",
		"-o", "az://acct/dst/ds", "--work-dir", work, "--output-endpoint-url", dst.URL, "--output-azure-sas-token", "sv=1&sig=FIRSTSIG"}
	if c := exitCode(t, run(ctx, append(args, "--max-duration", "1500ms"), io.Discard, false)); c != exitDeadline {
		t.Fatalf("first attempt: exit %d", c)
	}
	st, err := journal.Read(work)
	if err != nil || len(st.Records) == 0 || st.Header.Identity.Destination == nil || st.Header.Identity.Destination.Account != "acct" {
		t.Fatalf("the work directory's journal: %v, %+v", err, st.Header.Identity.Destination)
	}
	if _, ok := f.Get("ds/" + dataset.ManifestName); ok {
		t.Fatal("committed before the resume")
	}
	if err := resume(ctx, []string{work, "--max-duration", "0"}, io.Discard); err == nil || !strings.Contains(err.Error(), "--output-azure-sas-token") {
		t.Fatalf("resume without the SAS: %v", err)
	}
	if err := resume(ctx, []string{work, "--output-azure-sas-token", "sv=1&sig=SECONDSIG", "--max-duration", "0"}, io.Discard); err != nil {
		t.Fatalf("resume: %v", err)
	}
	o, ok := f.Get("ds/" + dataset.ManifestName)
	if !ok || !dataset.Committed(work) {
		t.Fatalf("not committed: %v", f.Names())
	}
	var m dataset.Manifest
	if err := json.Unmarshal(o.Data, &m); err != nil || len(m.Parts) < 2 {
		t.Fatalf("manifest: %v, %d parts", err, len(m.Parts))
	}
	var rows int64
	for _, p := range m.Parts {
		rows += p.Rows
	}
	if rows != 60000 {
		t.Fatalf("%d rows committed", rows)
	}
	mu.Lock()
	defer mu.Unlock()
	if sigs["FIRSTSIG"] == 0 || sigs["SECONDSIG"] == 0 || len(sigs) != 2 {
		t.Fatalf("destination requests by signature: %v", sigs)
	}
	_ = filepath.WalkDir(work, func(path string, e fs.DirEntry, err error) error {
		if err != nil || e.IsDir() {
			return err
		}
		b, _ := os.ReadFile(path)
		if bytes.Contains(b, []byte("FIRSTSIG")) || bytes.Contains(b, []byte("SECONDSIG")) {
			t.Errorf("%s holds a SAS", path)
		}
		return nil
	})
}

// TestRemoteAzureSASEchoed: an Azure destination refuses every write with
// an answer that quotes the request's query back, its escapes in lower
// case; the SAS's signature, with characters a URL reserves, reaches
// neither the run's error, error.json nor the report. The source is a
// local tree, which keeps no journal, so it runs on Windows too.
func TestRemoteAzureSASEchoed(t *testing.T) {
	captureStderr(t)
	root := t.TempDir()
	for i := range 300 {
		if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("f%04d", i)), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	const secret = "ZQXSECRET"
	f := azfake.New("acct", "dst")
	dst := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			f.ServeHTTP(w, r)
			return
		}
		w.Header().Set("x-ms-error-code", "AuthorizationPermissionMismatch")
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprintf(w, "<Error><Code>AuthorizationPermissionMismatch</Code><Message>%s</Message></Error>", strings.ToLower(r.URL.RawQuery))
	}))
	t.Cleanup(dst.Close)
	work := filepath.Join(t.TempDir(), "work")
	report := filepath.Join(t.TempDir(), "report.json")
	err := run(context.Background(), []string{root, "--part-size", "4KiB",
		"-o", "az://acct/dst/ds", "--work-dir", work, "--output-endpoint-url", dst.URL, "--report", report,
		"--output-azure-sas-token", "sv=1&sp=rwd&sig=" + url.QueryEscape("AB+"+secret+"/CD=")}, io.Discard, false)
	if err == nil {
		t.Fatal("a run whose writes are refused succeeded")
	}
	errFile, _ := os.ReadFile(filepath.Join(work, dataset.ErrorName))
	rep, _ := os.ReadFile(report)
	if len(errFile) == 0 || len(rep) == 0 {
		t.Fatalf("no error.json (%d bytes) or report (%d bytes)", len(errFile), len(rep))
	}
	for _, text := range []string{err.Error(), string(errFile), string(rep)} {
		if strings.Contains(strings.ToLower(text), strings.ToLower(secret)) {
			t.Fatalf("the signature escaped: %s", text)
		}
	}
	if !strings.Contains(string(errFile), "403") {
		t.Fatalf("the refusal is not in error.json: %s", errFile)
	}
}
