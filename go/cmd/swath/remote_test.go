// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/dataset/datasettest"
	"github.com/varveio/swath/go/internal/journal"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
	"github.com/varveio/swath/go/objfile/gcs/gcsfake"
)

// TestRemoteOutputRefusals: what -o gs://... refuses at parse time.
func TestRemoteOutputRefusals(t *testing.T) {
	work := filepath.Join(t.TempDir(), "w")
	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"-o", "s3://b/p", "--work-dir", work}, "gs:// only"},
		{[]string{"-o", "az://acct/c/p", "--work-dir", work}, "gs:// only"},
		{[]string{"-o", "gs://b/p"}, "--work-dir"},
		{[]string{"-o", "gs://b/p", "--work-dir", work, "--overwrite"}, "not supported"},
		{[]string{"-o", "gs://b/p", "--work-dir", work, "--format", "tsv"}, "--format"},
		{[]string{"-o", "gs://B/p", "--work-dir", work}, "bucket"},
		{[]string{"-o", "gs://b/p", "--work-dir", "gs://b/w"}, "local"},
		{[]string{"-o", filepath.Join(t.TempDir(), "ds"), "--work-dir", work}, "--work-dir"},
		{[]string{"-o", filepath.Join(t.TempDir(), "ds"), "--output-gcs-credentials-file", "x.json"}, "apply to a dataset in a bucket"},
		{[]string{"--output-endpoint-url", "http://x/"}, "apply to a dataset in a bucket"},
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
// upload, none on the anonymous source's requests), is stopped by its
// deadline, and swath resume WORK_DIR, given a rotated credentials file,
// completes it there.
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
		time.Sleep(50 * time.Millisecond)
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
	if c := exitCode(t, run(ctx, append(args, "--max-duration", "400ms"), io.Discard, false)); c != exitDeadline {
		t.Fatalf("first attempt: exit %d", c)
	}
	st, err := journal.Read(work)
	if err != nil || len(st.Records) == 0 || st.Header.Identity.Destination == nil || st.Records[0].Object == "" {
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
}
