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
// requests) fails in one line, sends no listing request, and keeps the
// typed failure, its class and its whole cause in the outcome. The line
// names the source and the cause's first line; --anonymous only where
// swath searched on its own (S3's default chain, refused at construction;
// Azure's DefaultAzureCredential, which fails the first request), not for
// a configured credential (S3 keys in the environment, an Azure SAS, GCS
// credentials files: one that does not parse, one whose token endpoint
// refuses the grant with an HTML page); -v when the cause has more. The
// public container lists with --anonymous.
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
	const anonymous = ". For public storage use --anonymous"
	for _, c := range []struct {
		name, env string // env: AWS_ACCESS_KEY_ID's value
		args      []string
		requests  *atomic.Int64
		want      string // the line, the cause's first line as FIRST
		first     string // in the cause's first line
		cause     string // in the cause, not the line
	}{
		{"s3 default chain", "", []string{"s3://bkt", "--endpoint-url", s3.URL}, &s3Requests,
			"could not obtain credentials (AWS default chain): FIRST" + anonymous, "EC2 IMDS", "EC2 IMDS"},
		{"s3 environment", "AKIDEXAMPLE", []string{"s3://bkt", "--endpoint-url", s3.URL}, &s3Requests,
			"could not obtain credentials (environment): AWS_ACCESS_KEY_ID is set without AWS_SECRET_ACCESS_KEY", "AWS_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID"},
		{"azblob normal endpoint", "", []string{"az://replay/corpus"}, &azRequests,
			"could not obtain credentials (Entra ID, DefaultAzureCredential): DefaultAzureCredential: failed to acquire a token" + anonymous + "; -v shows the details",
			"failed to acquire a token", "Attempted credentials:\n"},
		{"azblob", "", []string{"az://replay/corpus", "--endpoint-url", azProxy.URL}, &azRequests,
			"could not obtain credentials (Entra ID, DefaultAzureCredential): DefaultAzureCredential: failed to acquire a token" + anonymous + "; -v shows the details",
			"failed to acquire a token", "AZURE_TENANT_ID"},
		{"azblob sas", "", []string{"az://replay/corpus", "--endpoint-url", azProxy.URL, "--azure-sas-token", "%zz"}, &azRequests,
			"could not obtain credentials (shared access signature): not a query string", "not a query string", "not a query string"},
		{"gcs broken file", "", []string{"gs://bkt", "--endpoint-url", gcs.URL, "--gcs-credentials-file", broken}, &gcsRequests,
			"could not obtain credentials (file " + broken + "): FIRST", "JSON", "unexpected end of JSON"},
		{"gcs refused grant", "", []string{"gs://bkt", "--endpoint-url", gcs.URL, "--gcs-credentials-file", refusedGrant}, &gcsRequests,
			"could not obtain credentials (file " + refusedGrant + "): FIRST. -v shows the details", "400", remoteBody},
	} {
		t.Setenv("AWS_ACCESS_KEY_ID", c.env)
		out := filepath.Join(t.TempDir(), "ds")
		err := run(context.Background(), append(append(c.args, "-o", out), unjournaled...), io.Discard, diagList)
		var ce *objhttp.CredentialError
		var ee *exitError
		if !errors.As(err, &ce) || !ce.CredentialRefused() || !strings.Contains(ce.Err.Error(), c.cause) ||
			!errors.As(err, &ee) || ee.code != exitFatal || ee.out.Cause != "auth" || !strings.Contains(ee.out.Message, c.cause) || c.requests.Load() != 0 {
			t.Errorf("%s: %v, %d requests: the failure's chain or class changed, or a request went out", c.name, err, c.requests.Load())
			continue
		}
		first, _, _ := strings.Cut(ce.Err.Error(), "\n")
		if want := strings.Replace(c.want, "FIRST", strings.TrimSpace(first), 1); err.Error() != want || !strings.Contains(first, c.first) {
			t.Errorf("%s:\n got  %q\n want %q", c.name, err, want)
		}
	}
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	// The hint is right: the public container lists with --anonymous.
	out := filepath.Join(t.TempDir(), "ds")
	if err := run(context.Background(), append([]string{"az://replay/corpus", "--endpoint-url", azEndpoint(az), "--anonymous", "-o", out}, unjournaled...), io.Discard, diagList); err != nil {
		t.Fatalf("--anonymous: %v", err)
	}
	if _, err := os.Stat(out); err != nil {
		t.Fatal(err)
	}
}

// TestCredentialLine: the line is the source and the cause's first line
// that is not blank (lines end at \n or \r), trimmed and capped at
// causeCap runes, control characters in either escaped; the --anonymous
// advice only where swath searched on its own; the -v advice only when the
// line leaves part of the cause out (lines before or after it, or the
// cap), and not under -v.
func TestCredentialLine(t *testing.T) {
	long := strings.Repeat("é", causeCap+50)
	for _, c := range []struct {
		cause               string
		discovered, verbose bool
		want                string
	}{
		{"none found", true, false, "could not obtain credentials (S): none found. For public storage use --anonymous"},
		{"none found.", true, false, "could not obtain credentials (S): none found. For public storage use --anonymous"},
		{"refused\n<html>\n</html>", true, false, "could not obtain credentials (S): refused. For public storage use --anonymous; -v shows the details"},
		{"refused\n<html>\n</html>", true, true, "could not obtain credentials (S): refused. For public storage use --anonymous"},
		{"exited 4: nope", false, false, "could not obtain credentials (S): exited 4: nope"},
		{"  exited 4: nope \n\t \n", false, false, "could not obtain credentials (S): exited 4: nope"},
		{"exited 4: nope\nusage: ...", false, false, "could not obtain credentials (S): exited 4: nope. -v shows the details"},
		{"exited 4: nope\nusage: ...", false, true, "could not obtain credentials (S): exited 4: nope"},
		{long, false, false, "could not obtain credentials (S): " + long[:2*causeCap] + "... -v shows the details"},
		{long, true, true, "could not obtain credentials (S): " + long[:2*causeCap] + "... For public storage use --anonymous"},
		{"", true, false, "could not obtain credentials (S). For public storage use --anonymous"},
		{"refused\rDETAIL", false, false, "could not obtain credentials (S): refused. -v shows the details"},
		{"refused\r\n", false, false, "could not obtain credentials (S): refused"},
		{"\nDETAIL", false, false, "could not obtain credentials (S): DETAIL. -v shows the details"},
		{"\nDETAIL", false, true, "could not obtain credentials (S): DETAIL"},
		{"bad\x1b[31mred\tx", false, false, `could not obtain credentials (S): bad\x1b[31mred\tx`},
	} {
		if got := credentialLine("S", c.cause, c.discovered, c.verbose); got != c.want {
			t.Errorf("%q discovered %v verbose %v:\n got  %q\n want %q", c.cause, c.discovered, c.verbose, got, c.want)
		}
	}
	// A source is a path or a profile name, which may hold anything.
	if got, want := credentialLine("file /tmp/a\nb.json", "refused", false, false), `could not obtain credentials (file /tmp/a\nb.json): refused`; got != want {
		t.Errorf("\n got  %q\n want %q", got, want)
	}
}

// TestCredentialRefusalShown: a refused credential's multi-line cause (an
// SDK chain report quoting a remote HTML page) stays off stderr at the
// default verbosity, in text and json alike (the outcome's message
// included), and -q, its first line in the line; -v shows it whole once,
// in the outcome; -qq prints nothing. A transient credential failure, and
// the service refusing credentials, are not credential refusals: they
// print as they are.
func TestCredentialRefusalShown(t *testing.T) {
	cause := fmt.Errorf("Azure Blob client: %w", &objhttp.CredentialError{Source: "Entra ID, DefaultAzureCredential", Discovered: true,
		Err: errors.New("DefaultAzureCredential: failed to acquire a token.\nAttempted credentials:\n\tManagedIdentityCredential: RESPONSE 404\n" + remoteBody)})
	const line = "could not obtain credentials (Entra ID, DefaultAzureCredential): DefaultAzureCredential: failed to acquire a token. For public storage use --anonymous"
	const brief = line + "; -v shows the details"
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
		&objhttp.CredentialError{Source: "Entra ID, DefaultAzureCredential", Discovered: true, Transient: true, Err: errors.New("token endpoint: 503")},
		fmt.Errorf("Azure Blob: account a: %w", errors.New("403 AuthorizationFailure")),
	} {
		if got := explain(err); got != err {
			t.Errorf("%v: explained as %v", err, got)
		}
	}
}
