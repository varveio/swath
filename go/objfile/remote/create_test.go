// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package remote

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/go/objfile"
	"github.com/varveio/swath/go/objfile/gcs"
	"github.com/varveio/swath/go/objfile/gcs/gcsfake"
)

// TestCreate: a destination writes and reads back under its prefix, on
// the emulator STORAGE_EMULATOR_HOST names, without credentials.
func TestCreate(t *testing.T) {
	f := gcsfake.New("bkt")
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	t.Setenv("STORAGE_EMULATOR_HOST", strings.TrimPrefix(srv.URL, "http://"))
	d, err := Create(context.Background(), "gs://bkt/ds", CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if !strings.HasPrefix(d.Scope(), "gs://bkt/ds/ at ") {
		t.Fatalf("scope %q", d.Scope())
	}
	r, err := d.Writer().PutIfAbsent(context.Background(), "manifest.json", []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := f.Get("ds/manifest.json"); !ok || r.Name != "manifest.json" {
		t.Fatalf("objects %v, receipt %+v", f.Names(), r)
	}
	obj, err := d.Open(context.Background(), "manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	defer obj.Close()
	b, err := io.ReadAll(io.NewSectionReader(obj, 0, obj.Size()))
	if err != nil || string(b) != `{}` || obj.Identity() != r.Identity {
		t.Fatalf("read back %q (%v), identity %s, receipt %s", b, err, obj.Identity(), r.Identity)
	}
}

// TestCreateRefusals: only gs:// is written; an endpoint carrying
// credentials and an unreadable credentials file are refused.
func TestCreateRefusals(t *testing.T) {
	t.Setenv("STORAGE_EMULATOR_HOST", "")
	for _, loc := range []string{"s3://b/p", "az://acct/c/p"} {
		if _, err := Create(context.Background(), loc, CreateOptions{}); err == nil || !strings.Contains(err.Error(), "not supported") {
			t.Errorf("Create(%s) = %v", loc, err)
		}
	}
	if _, err := Create(context.Background(), "gs://b", CreateOptions{Endpoint: "https://user:" + secret + "@host/storage/v1/"}); err == nil || strings.Contains(err.Error(), secret) {
		t.Errorf("endpoint with user information: %v", err)
	}
	_, err := Create(context.Background(), "gs://b", CreateOptions{CredentialsFile: filepath.Join(t.TempDir(), "missing.json")})
	if err == nil || !strings.Contains(err.Error(), "destination credentials") {
		t.Errorf("missing credentials file: %v", err)
	}
}

// serviceAccountFile writes a service account key whose token endpoint is
// tokenURL.
func serviceAccountFile(t *testing.T, tokenURL string) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	der, _ := x509.MarshalPKCS8PrivateKey(key)
	b, _ := json.Marshal(map[string]string{"type": "service_account", "project_id": "p", "private_key_id": "k", "client_id": "1",
		"private_key": string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})), "client_email": "w@p.iam.gserviceaccount.com", "token_uri": tokenURL})
	path := filepath.Join(t.TempDir(), "sa.json")
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestCreateQuietAuth: with the SDK's debug logging switched on by the
// environment, a destination using Application Default Credentials logs no
// credential: no token, no signed assertion, no resumable session URI.
// (The storage SDK does not hand its logger to the API client it builds,
// so request metadata may still be logged: names and URLs, never a
// credential, which the transport below that logger adds.)
func TestCreateQuietAuth(t *testing.T) {
	const token = "SYNTHETIC_ACCESS_TOKEN"
	f := gcsfake.New("bkt")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"access_token":%q,"token_type":"Bearer","expires_in":3600}`, token)
			return
		}
		f.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	t.Setenv("STORAGE_EMULATOR_HOST", "")
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", serviceAccountFile(t, srv.URL+"/token"))
	t.Setenv("GOOGLE_SDK_GO_LOGGING_LEVEL", "debug")
	rd, wr, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	stderr := os.Stderr
	os.Stderr = wr
	captured := make(chan []byte)
	go func() { b, _ := io.ReadAll(rd); captured <- b }()
	d, err := Create(context.Background(), "gs://bkt/ds", CreateOptions{Endpoint: srv.URL + "/storage/v1/", Write: gcs.WriteOptions{ChunkSize: 256 << 10}})
	if err == nil {
		_, err = d.Writer().PutIfAbsent(context.Background(), "m", []byte("{}"))
	}
	if err == nil {
		var u objfile.Upload
		if u, err = d.Writer().NewWriter(context.Background(), "big", objfile.CreateOnly); err == nil {
			if _, err = u.Write(make([]byte, 3<<18)); err == nil { // a resumable session of three chunks
				err = u.Close()
			}
		}
	}
	if d != nil {
		d.Close()
	}
	os.Stderr = stderr
	wr.Close()
	out := <-captured
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{token, "assertion=", "upload_id", "Bearer", "Authorization"} {
		if bytes.Contains(out, []byte(secret)) {
			t.Fatalf("stderr holds %q: %s", secret, out)
		}
	}
}

// TestCreateTokenBounded: a token endpoint that never answers holds a
// request no longer than its context, and no longer than the request
// timeout without one.
func TestCreateTokenBounded(t *testing.T) {
	release := make(chan struct{})
	f := gcsfake.New("bkt")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			select {
			case <-release:
			case <-r.Context().Done():
			}
			return
		}
		f.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) }) // before the server closes: it waits for its handlers
	t.Setenv("STORAGE_EMULATOR_HOST", "")
	sa := serviceAccountFile(t, srv.URL+"/token")
	for _, c := range []struct {
		name    string
		timeout time.Duration
		ctx     time.Duration
	}{{"context", 5 * time.Second, 200 * time.Millisecond}, {"request timeout", 200 * time.Millisecond, 0}} {
		t.Run(c.name, func(t *testing.T) {
			d, err := Create(context.Background(), "gs://bkt/ds", CreateOptions{Endpoint: srv.URL + "/storage/v1/", CredentialsFile: sa, RequestTimeout: c.timeout})
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			ctx := context.Background()
			if c.ctx > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, c.ctx)
				defer cancel()
			}
			start := time.Now()
			if _, err := d.Open(ctx, "m"); err == nil {
				t.Fatal("an open without a token succeeded")
			}
			if took := time.Since(start); took > 30*time.Second { // the request timeout on each of the read's bounded attempts
				t.Fatalf("the open waited %v on the token endpoint", took)
			}
		})
	}
}
