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
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/varveio/swath/go/objfile"
	"github.com/varveio/swath/go/objfile/azblob"
	"github.com/varveio/swath/go/objfile/azblob/azfake"
	"github.com/varveio/swath/go/objfile/gcs"
	"github.com/varveio/swath/go/objfile/gcs/gcsfake"
	"github.com/varveio/swath/go/objfile/s3"
	"github.com/varveio/swath/go/objfile/s3/s3fake"
)

// TestOpenDestination: a destination writes and reads back under its prefix, on
// the emulator STORAGE_EMULATOR_HOST names, without credentials.
func TestOpenDestination(t *testing.T) {
	f := gcsfake.New("bkt")
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	t.Setenv("STORAGE_EMULATOR_HOST", strings.TrimPrefix(srv.URL, "http://"))
	d, err := OpenDestination(context.Background(), "gs://bkt/ds", DestinationOptions{})
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

// TestOpenDestinationRefusals: an endpoint carrying credentials and an unreadable
// credentials file are refused.
func TestOpenDestinationRefusals(t *testing.T) {
	t.Setenv("STORAGE_EMULATOR_HOST", "")
	if _, err := OpenDestination(context.Background(), "gs://b", DestinationOptions{Endpoint: "https://user:" + secret + "@host/storage/v1/"}); err == nil || strings.Contains(err.Error(), secret) {
		t.Errorf("endpoint with user information: %v", err)
	}
	_, err := OpenDestination(context.Background(), "gs://b", DestinationOptions{CredentialsFile: filepath.Join(t.TempDir(), "missing.json")})
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

// TestOpenDestinationQuietAuth: with the SDK's debug logging switched on by the
// environment, a destination using Application Default Credentials logs no
// credential: no token, no signed assertion, no resumable session URI.
// (The storage SDK does not hand its logger to the API client it builds,
// so request metadata may still be logged: names and URLs, never a
// credential, which the transport below that logger adds.)
func TestOpenDestinationQuietAuth(t *testing.T) {
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
	d, err := OpenDestination(context.Background(), "gs://bkt/ds", DestinationOptions{Endpoint: srv.URL + "/storage/v1/", GCSWrite: gcs.WriteOptions{ChunkSize: 256 << 10}})
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

// TestOpenDestinationTokenBounded: a token endpoint that never answers holds a
// request no longer than its context, and no longer than the request
// timeout without one.
func TestOpenDestinationTokenBounded(t *testing.T) {
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
			d, err := OpenDestination(context.Background(), "gs://bkt/ds", DestinationOptions{Endpoint: srv.URL + "/storage/v1/", CredentialsFile: sa, RequestTimeout: c.timeout})
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

// s3Destination serves an s3fake bucket "bkt", recording each request's
// Authorization; it returns the fake, its URL and the record.
func s3Destination(t *testing.T) (*s3fake.Server, string, *sync.Map) {
	t.Helper()
	f := s3fake.New("bkt")
	var auth sync.Map
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth.Store(r.Header.Get("Authorization"), true)
		f.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return f, srv.URL, &auth
}

// awsEnv clears the AWS environment a test does not set, and points the
// shared files at an empty directory.
func awsEnv(t *testing.T) {
	dir := t.TempDir()
	for _, v := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_REGION", "AWS_DEFAULT_REGION",
		"AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3", "AWS_USE_FIPS_ENDPOINT", "AWS_USE_DUALSTACK_ENDPOINT", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN",
		"AWS_CONTAINER_CREDENTIALS_FULL_URI", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"} {
		t.Setenv(v, "")
	}
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(dir, "config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(dir, "credentials"))
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
}

// TestOpenDestinationS3: an s3:// destination writes and reads back under its
// prefix on the endpoint named (path-style), signing every request with
// the profile's credentials; the region is the profile's when not given;
// an endpoint the environment sets is not applied.
func TestOpenDestinationS3(t *testing.T) {
	awsEnv(t)
	f, url, auth := s3Destination(t)
	_, other, otherAuth := s3Destination(t)
	t.Setenv("AWS_ENDPOINT_URL_S3", other) // the source's, say: never the destination's
	t.Setenv("AWS_ENDPOINT_URL", other)
	cfg := filepath.Join(t.TempDir(), "config")
	if err := os.WriteFile(cfg, []byte("[profile dst]\nregion = eu-west-3\naws_access_key_id = AKIDDST\naws_secret_access_key = "+secret+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AWS_CONFIG_FILE", cfg)
	d, err := OpenDestination(context.Background(), "s3://bkt/ds", DestinationOptions{Endpoint: url, Profile: "dst"})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if d.Scope() != "s3://bkt/ds/ at "+url || d.Region() != "eu-west-3" {
		t.Fatalf("scope %q, region %q", d.Scope(), d.Region())
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
	n := 0
	auth.Range(func(k, _ any) bool {
		n++
		if a := k.(string); !strings.HasPrefix(a, "AWS4-HMAC-SHA256 Credential=AKIDDST/") || !strings.Contains(a, "/eu-west-3/s3/") {
			t.Errorf("a request signed %q", a)
		}
		return true
	})
	otherAuth.Range(func(k, _ any) bool { t.Errorf("the environment's endpoint was sent a request"); return false })
	if n == 0 {
		t.Fatal("no request recorded")
	}

	// The region given wins; without one or a profile's, us-east-1.
	if d, err := OpenDestination(context.Background(), "s3://bkt/ds", DestinationOptions{Endpoint: url, Profile: "dst", Region: "ap-south-1"}); err != nil || d.Region() != "ap-south-1" {
		t.Fatalf("given region: %v", err)
	}
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(t.TempDir(), "none"))
	if d, err := OpenDestination(context.Background(), "s3://bkt/ds", DestinationOptions{Endpoint: url}); err != nil || d.Region() != "us-east-1" {
		t.Fatalf("default region: %v", err)
	}
}

// TestOpenDestinationS3Refusals: an endpoint carrying credentials, a part size S3
// refuses, and a missing profile are refused before any request.
func TestOpenDestinationS3Refusals(t *testing.T) {
	awsEnv(t)
	if _, err := OpenDestination(context.Background(), "s3://b", DestinationOptions{Endpoint: "https://user:" + secret + "@host/"}); err == nil || strings.Contains(err.Error(), secret) {
		t.Errorf("endpoint with user information: %v", err)
	}
	if _, err := OpenDestination(context.Background(), "s3://b", DestinationOptions{S3Write: s3.WriteOptions{PartSize: 1 << 20}}); err == nil {
		t.Error("a 1 MiB part size: accepted")
	}
	if _, err := OpenDestination(context.Background(), "s3://b", DestinationOptions{Profile: "missing"}); err == nil {
		t.Error("a missing profile: accepted")
	}
}

// TestOpenDestinationS3CABundle: a CA bundle the AWS configuration names
// (AWS_CA_BUNDLE, or a profile's ca_bundle) is the destination's trust: an
// endpoint whose certificate it holds is reached, and without it the
// same endpoint is refused.
func TestOpenDestinationS3CABundle(t *testing.T) {
	awsEnv(t)
	f := s3fake.New("bkt")
	srv := httptest.NewTLSServer(f)
	t.Cleanup(srv.Close)
	bundle := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(bundle, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AWS_ACCESS_KEY_ID", "AKID")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "s3cr3t")
	put := func(name string, o DestinationOptions) error {
		o.Endpoint = srv.URL
		d, err := OpenDestination(context.Background(), "s3://bkt/ds", o)
		if err != nil {
			return err
		}
		defer d.Close()
		_, err = d.Writer().PutIfAbsent(context.Background(), name, []byte(`{}`))
		return err
	}
	if err := put("none.json", DestinationOptions{S3Write: s3.WriteOptions{MaxAttempts: 1}}); err == nil {
		t.Fatal("an endpoint no bundle trusts was reached")
	}
	t.Run("environment", func(t *testing.T) {
		t.Setenv("AWS_CA_BUNDLE", bundle)
		if err := put("env.json", DestinationOptions{}); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("profile", func(t *testing.T) {
		cfg := filepath.Join(t.TempDir(), "config")
		if err := os.WriteFile(cfg, []byte("[profile dst]\naws_access_key_id = AKID\naws_secret_access_key = s3cr3t\nca_bundle = "+bundle+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("AWS_CONFIG_FILE", cfg)
		if err := put("profile.json", DestinationOptions{Profile: "dst"}); err != nil {
			t.Fatal(err)
		}
	})
	if _, ok := f.Get("ds/env.json"); !ok {
		t.Fatalf("objects %v", f.Names())
	}
	if _, ok := f.Get("ds/profile.json"); !ok {
		t.Fatalf("objects %v", f.Names())
	}
}

// azureDestination serves an azfake container "ctr" of account "acct",
// recording each request's query; it returns the fake, its URL and the
// record.
func azureDestination(t *testing.T) (*azfake.Server, string, *sync.Map) {
	t.Helper()
	f := azfake.New("acct", "ctr")
	var queries sync.Map
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries.Store(r.URL.Query().Get("sig"), true)
		f.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return f, srv.URL, &queries
}

// TestOpenDestinationAzure: an az:// destination writes, in blocks and in one
// request, and reads back under its prefix on the endpoint named, the
// account its first path segment, every request carrying the SAS; the
// scope names the endpoint and holds no SAS.
func TestOpenDestinationAzure(t *testing.T) {
	f, url, sigs := azureDestination(t)
	sas := "?sv=2026-06-06&sp=rcwd&sig=" + secret
	d, err := OpenDestination(context.Background(), "az://acct/ctr/ds", DestinationOptions{Endpoint: url + "/", SAS: sas, AzureWrite: azblob.WriteOptions{BlockSize: 64 << 10}})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if d.Scope() != "az://acct/ctr/ds/ at "+url+"/" || strings.Contains(d.Scope(), secret) || d.Region() != "" {
		t.Fatalf("scope %q, region %q", d.Scope(), d.Region())
	}
	r, err := d.Writer().PutIfAbsent(context.Background(), "manifest.json", []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	u, err := d.Writer().NewWriter(context.Background(), "data/p", objfile.CreateOnly)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := u.Write(make([]byte, 3<<16+1)); err != nil {
		t.Fatal(err)
	}
	if err := u.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.Get("ds/manifest.json"); !ok || r.Name != "manifest.json" || f.Count(azfake.OpChunk) != 4 {
		t.Fatalf("blobs %v, receipt %+v, %d blocks", f.Names(), r, f.Count(azfake.OpChunk))
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
	sigs.Range(func(k, _ any) bool {
		if k.(string) != secret {
			t.Errorf("a request carried the signature %q", k)
		}
		return true
	})
}

// TestOpenDestinationAzureRefusals: an endpoint carrying credentials, a SAS that is
// not a query string and a block size the service refuses are refused
// before any request, quoting no credential; a write the service refuses
// shows no signature, even when the service's answer quotes it.
func TestOpenDestinationAzureRefusals(t *testing.T) {
	if _, err := OpenDestination(context.Background(), "az://acct/ctr", DestinationOptions{Endpoint: "https://user:" + secret + "@host/", SAS: "sig=x"}); err == nil || strings.Contains(err.Error(), secret) {
		t.Errorf("endpoint with user information: %v", err)
	}
	if _, err := OpenDestination(context.Background(), "az://acct/ctr", DestinationOptions{SAS: "sig=%zz" + secret}); err == nil || strings.Contains(err.Error(), secret) {
		t.Errorf("a SAS that is not a query string: %v", err)
	}
	if _, err := OpenDestination(context.Background(), "az://acct/ctr", DestinationOptions{SAS: "sig=x", AzureWrite: azblob.WriteOptions{BlockSize: azblob.MaxBlockSize + 1}}); err == nil {
		t.Error("a block size over the limit: accepted")
	}
	// The service's answer quotes the request's query back: as sent, with its
	// escapes in lower case, escaped once more, and with a second signature.
	sig := url.QueryEscape("AB+" + secret + "/CD=")
	for name, c := range map[string]struct {
		sas  string
		echo func(string) string
	}{
		"as sent":   {"sv=1&sig=" + sig, func(q string) string { return q }},
		"lower":     {"sv=1&sig=" + sig, func(q string) string { return strings.NewReplacer("%2B", "%2b", "%2F", "%2f", "%3D", "%3d").Replace(q) }},
		"escaped":   {"sv=1&sig=" + sig, url.QueryEscape},
		"two sigs":  {"sv=1&sig=other&sig=" + sig, func(q string) string { return q }},
		"sig first": {"sig=" + sig + "&sv=1", func(q string) string { return strings.ToLower(q) }},
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("x-ms-error-code", "AuthenticationFailed")
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprintf(w, "<Error><Code>AuthenticationFailed</Code><Message>%s</Message></Error>", c.echo(r.URL.RawQuery))
		}))
		d, err := OpenDestination(context.Background(), "az://acct/ctr", DestinationOptions{Endpoint: srv.URL, SAS: c.sas})
		if err != nil {
			t.Fatal(err)
		}
		_, err = d.Writer().PutIfAbsent(context.Background(), "m", []byte("x"))
		if err == nil || !strings.Contains(err.Error(), "403") || strings.Contains(strings.ToLower(fmt.Sprintf("%+v", err)), strings.ToLower(secret)) {
			t.Errorf("%s: a refused write whose answer quotes the request: %v", name, err)
		}
		d.Close()
		srv.Close()
	}
}

// TestAzureService: the account's own host by default; on an endpoint, the
// account is the first path segment after the endpoint's path.
func TestAzureService(t *testing.T) {
	for _, c := range [][3]string{
		{"acct", "", "https://acct.blob.core.windows.net/"},
		{"devstoreaccount1", "http://127.0.0.1:10000", "http://127.0.0.1:10000/devstoreaccount1/"},
		{"acct", "https://gw.example.com/blob/", "https://gw.example.com/blob/acct/"},
	} {
		if got := azureService(c[0], c[1]); got != c[2] {
			t.Errorf("azureService(%q, %q) = %q, want %q", c[0], c[1], got, c[2])
		}
	}
}
