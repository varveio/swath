// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package remote

import (
	"context"
	"io"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

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
