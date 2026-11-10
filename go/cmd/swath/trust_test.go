// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/pem"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/source/azblob/azblobtest"
	"github.com/varveio/swath/go/internal/source/gcs/gcstest"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
)

// tlsFront serves plain over HTTPS, under a certificate no system trusts,
// and returns its URL with plain's path and a PEM file of the
// certificate.
func tlsFront(t *testing.T, plain string) (endpoint, bundle string) {
	t.Helper()
	u, err := url.Parse(plain)
	if err != nil {
		t.Fatal(err)
	}
	path := u.Path
	u.Path = ""
	srv := httptest.NewTLSServer(httputil.NewSingleHostReverseProxy(u))
	t.Cleanup(srv.Close)
	bundle = filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(bundle, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	return srv.URL + path, bundle
}

// TestTrustFlagsOnResume: --ca-bundle and --no-verify-ssl are operational:
// a resume replays the recorded ones (--no-verify-ssl warning again, as
// every run that has it), and may give them again or undo
// --no-verify-ssl (--verify-ssl).
func TestTrustFlagsOnResume(t *testing.T) {
	for _, n := range []string{"ca-bundle", "no-verify-ssl", "verify-ssl"} {
		if !mayChange(n) {
			t.Errorf("--%s cannot be given to swath resume", n)
		}
	}
	if fl := newFlags(&options{}, true).Lookup("verify-ssl"); fl == nil || !fl.Hidden {
		t.Fatal("no hidden --verify-ssl undoes a recorded --no-verify-ssl")
	}
}

// TestTrustFlags: --ca-bundle and --no-verify-ssl mean the same for every
// object store. An endpoint behind a private certificate fails without
// them, lists with its certificate in --ca-bundle (or, for S3, in
// AWS_CA_BUNDLE), and lists with --no-verify-ssl, which warns and shows in
// the report.
func TestTrustFlags(t *testing.T) {
	s3test.NoCredentials(t)
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_CA_BUNDLE", "")
	az := azblobtest.Serve(t, []string{"a", "b"})
	roots := map[string]struct {
		root, plain string
		env         string // the provider's own variable for the CA bundle, if any
	}{
		"s3":     {"s3://bkt", s3test.Bucket(t, []string{"a", "b"}), "AWS_CA_BUNDLE"},
		"gcs":    {"gs://bkt", gcstest.Bucket(t, []string{"a", "b"}, 0), ""},
		"azblob": {"az://" + az.Account + "/" + az.Container, azEndpoint(az), ""},
	}
	for name, r := range roots {
		t.Run(name, func(t *testing.T) {
			endpoint, bundle := tlsFront(t, r.plain)
			list := func(args ...string) (stats map[string]any, stderr string, err error) {
				t.Helper()
				errBuf := captureStderr(t)
				var out bytes.Buffer
				args = append(append([]string{r.root, "--endpoint-url", endpoint, "--anonymous", "--request-timeout", "5s",
					"--report", "-", "-o", filepath.Join(t.TempDir(), "ds")}, args...), unjournaled...)
				err = run(context.Background(), args, &out, diagList)
				var rep struct {
					Source struct{ Stats map[string]any }
				}
				if jerr := json.Unmarshal(out.Bytes(), &rep); jerr != nil && err == nil {
					t.Fatalf("report: %v", jerr)
				}
				return rep.Source.Stats, errBuf.String(), err
			}
			if _, _, err := list(); err == nil || !strings.Contains(err.Error(), "certificate") {
				t.Fatalf("an untrusted certificate: %v", err)
			}
			if stats, stderr, err := list("--ca-bundle", bundle); err != nil || stats["no_verify_ssl"] != nil || strings.Contains(stderr, source.NoVerifyWarning) {
				t.Fatalf("--ca-bundle: %v; stats %v, stderr %q", err, stats, stderr)
			}
			stats, stderr, err := list("--no-verify-ssl")
			if err != nil || stats["no_verify_ssl"] != true || strings.Count(stderr, source.NoVerifyWarning) != 1 {
				t.Fatalf("--no-verify-ssl: %v; stats %v, stderr %q", err, stats, stderr)
			}
			if r.env != "" {
				t.Setenv(r.env, bundle)
				if _, _, err := list(); err != nil {
					t.Fatalf("%s: %v", r.env, err)
				}
				// The flag wins over the variable.
				t.Setenv(r.env, filepath.Join(t.TempDir(), "missing.pem"))
				if _, _, err := list("--ca-bundle", bundle); err != nil {
					t.Fatalf("--ca-bundle over %s: %v", r.env, err)
				}
			}
		})
	}
}
