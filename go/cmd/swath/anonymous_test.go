// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/varveio/swath/go/internal/source/azblob/azblobtest"
	"github.com/varveio/swath/go/internal/source/objhttp"
)

// remoteBody is a credential endpoint's HTML error page (what a metadata
// address that is not Azure's answers), which no default line may show.
const remoteBody = "<!DOCTYPE html>\n<html><head><style>body{margin:0}</style></head>\n<body>404 Not Found</body></html>"

// TestPublicBucketHint: a listing that fails because no credentials were
// found or they would not load (swath never falls back to anonymous
// requests) fails in one line that names its source and --anonymous,
// sends no listing request, and keeps the typed failure and its class:
// S3, refused at construction; Azure, whose default chain fails the first
// request; GCS, a credentials file that does not parse and one whose token
// endpoint refuses the grant with an HTML page. The public container lists
// with --anonymous.
func TestPublicBucketHint(t *testing.T) {
	captureStderr(t)
	home := t.TempDir()
	t.Setenv("HOME", home)
	for _, k := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_REGION", "AWS_DEFAULT_REGION",
		"AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI",
		"AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET", "AZURE_CLIENT_CERTIFICATE_PATH", "AZURE_USERNAME", "AZURE_FEDERATED_TOKEN_FILE",
		"IDENTITY_ENDPOINT", "MSI_ENDPOINT", "AZURE_STORAGE_KEY", "AZURE_STORAGE_SAS_TOKEN", "GOOGLE_APPLICATION_CREDENTIALS"} {
		t.Setenv(k, "")
	}
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(home, "config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(home, "credentials"))
	t.Setenv("AZURE_TOKEN_CREDENTIALS", "EnvironmentCredential") // the chain without the CLI logins or a metadata probe

	var s3Requests atomic.Int64
	s3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { s3Requests.Add(1) }))
	defer s3.Close()
	az := azblobtest.Serve(t, []string{"a", "b"})
	var azRequests atomic.Int64
	azProxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		azRequests.Add(1)
		w.WriteHeader(http.StatusForbidden)
	}))
	defer azProxy.Close()
	var gcsRequests atomic.Int64
	gcs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { gcsRequests.Add(1) }))
	defer gcs.Close()
	tok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, remoteBody)
	}))
	defer tok.Close()
	refusedGrant, broken := filepath.Join(home, "refused.json"), filepath.Join(home, "broken.json")
	os.WriteFile(refusedGrant, fmt.Appendf(nil, `{"type":"authorized_user","client_id":"c","client_secret":"s","refresh_token":"rt","token_uri":%q}`, tok.URL), 0o600)
	os.WriteFile(broken, []byte(`{"type":"authorized_user",`), 0o600)
	for _, c := range []struct {
		name, source string
		args         []string
		requests     *atomic.Int64
		cause        string // in the chain, not the line
	}{
		{"s3", "AWS default chain", []string{"s3://bkt", "--endpoint-url", s3.URL}, &s3Requests, "EC2 IMDS"},
		{"azblob normal endpoint", "Entra ID (DefaultAzureCredential)", []string{"az://replay/corpus"}, &azRequests, "Attempted credentials:\n"},
		{"azblob", "Entra ID (DefaultAzureCredential)", []string{"az://replay/corpus", "--endpoint-url", azProxy.URL}, &azRequests, "AZURE_TENANT_ID"},
		{"gcs broken file", "file " + broken, []string{"gs://bkt", "--endpoint-url", gcs.URL, "--gcs-credentials-file", broken}, &gcsRequests, "unexpected end of JSON"},
		{"gcs refused grant", "file " + refusedGrant, []string{"gs://bkt", "--endpoint-url", gcs.URL, "--gcs-credentials-file", refusedGrant}, &gcsRequests, remoteBody},
	} {
		out := filepath.Join(t.TempDir(), "ds")
		err := run(context.Background(), append(append(c.args, "-o", out), unjournaled...), io.Discard, diagList)
		want := "could not obtain credentials (" + c.source + "); for public storage use --anonymous; use -v for details"
		if err == nil || err.Error() != want || c.requests.Load() != 0 {
			t.Errorf("%s: %v, %d requests; want %q and none sent", c.name, err, c.requests.Load(), want)
			continue
		}
		var ce *objhttp.CredentialError
		var ee *exitError
		if !errors.As(err, &ce) || !ce.CredentialRefused() || !strings.Contains(ce.Err.Error(), c.cause) ||
			!errors.As(err, &ee) || ee.code != exitFatal || ee.out.Cause != "auth" || !strings.Contains(ee.out.Message, c.cause) {
			t.Errorf("%s: the failure's chain or class changed: %#v", c.name, ee)
		}
	}
	// The hint is right: the public container lists with --anonymous.
	out := filepath.Join(t.TempDir(), "ds")
	if err := run(context.Background(), append([]string{"az://replay/corpus", "--endpoint-url", azEndpoint(az), "--anonymous", "-o", out}, unjournaled...), io.Discard, diagList); err != nil {
		t.Fatalf("--anonymous: %v", err)
	}
	if _, err := os.Stat(out); err != nil {
		t.Fatal(err)
	}
}

// TestCredentialRefusalShown: a refused credential's multi-line cause (an
// SDK chain report quoting a remote HTML page) stays off stderr at the
// default verbosity, in text and json alike (the outcome's message
// included), and -q; -v shows it once, in the outcome; -qq prints
// nothing. A transient credential failure, and the service refusing
// credentials, are not credential refusals: they print as they are.
func TestCredentialRefusalShown(t *testing.T) {
	cause := fmt.Errorf("Azure Blob client: %w", &objhttp.CredentialError{Source: "Entra ID (DefaultAzureCredential)",
		Err: errors.New("DefaultAzureCredential: failed to acquire a token.\nAttempted credentials:\n\tManagedIdentityCredential: RESPONSE 404\n" + remoteBody)})
	const line = "could not obtain credentials (Entra ID (DefaultAzureCredential)); for public storage use --anonymous"
	const brief = line + "; use -v for details"
	show := func(term termOptions) []string {
		t.Helper()
		stderr := captureSyncStderr(t)
		setLog(term, "")
		o := classify(cause, nil, "")
		if code := conclude(&exitError{error: explain(cause), code: o.ExitCode, out: o}); code != exitFatal {
			t.Errorf("%+v: exit %d", term, code)
		}
		return slices.Collect(strings.Lines(stderr.String()))
	}
	event := func(l string) (msg, message string) {
		var ev struct{ Msg, Message string }
		if json.Unmarshal([]byte(l), &ev) != nil {
			t.Fatalf("not an event: %s", l)
		}
		return ev.Msg, ev.Message
	}
	for _, term := range []termOptions{{}, {quiet: 1}} {
		if got := show(term); !slices.Equal(got, []string{"swath: " + brief + "\n"}) {
			t.Errorf("%+v: %q", term, got)
		}
	}
	if got := show(termOptions{quiet: 2}); len(got) != 0 {
		t.Errorf("-qq: %q", got)
	}
	// -v: the outcome, which carries the cause, then the line.
	var o outcome
	if got := show(termOptions{verbose: 1}); len(got) != 2 || json.Unmarshal([]byte(strings.TrimPrefix(got[0], "swath: outcome: ")), &o) != nil ||
		o.Message != cause.Error() || got[1] != "swath: "+line+"\n" {
		t.Errorf("-v: %q", got)
	}
	if got := show(termOptions{logFormat: logJSON}); len(got) != 1 {
		t.Errorf("json: %q", got)
	} else if msg, message := event(got[0]); msg != brief || message != brief {
		t.Errorf("json: msg %q, message %q", msg, message)
	}
	if got := show(termOptions{logFormat: logJSON, verbose: 1}); len(got) != 1 {
		t.Errorf("json -v: %q", got)
	} else if msg, message := event(got[0]); msg != line || message != cause.Error() {
		t.Errorf("json -v: msg %q, message %q", msg, message)
	}
	for _, err := range []error{
		&objhttp.CredentialError{Source: "Entra ID (DefaultAzureCredential)", Transient: true, Err: errors.New("token endpoint: 503")},
		fmt.Errorf("Azure Blob: account a: %w", errors.New("403 AuthorizationFailure")),
	} {
		if got := explain(err); got != err {
			t.Errorf("%v: explained as %v", err, got)
		}
	}
}
