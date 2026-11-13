// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/spf13/pflag"

	"github.com/varveio/swath/internal/registry"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/azblob/azblobtest"
	"github.com/varveio/swath/internal/source/objhttp"
)

// openFlags opens root with the command-line flags args, collecting the
// warnings.
func openFlags(t *testing.T, root string, warned *[]string, args ...string) (registry.Source, error) {
	t.Helper()
	f, err := registry.Lookup(root)
	if err != nil {
		t.Fatal(err)
	}
	fs := pflag.NewFlagSet("t", pflag.ContinueOnError)
	o := f.RegisterFlags(fs, false)
	if err := fs.Parse(args); err != nil {
		t.Fatal(err)
	}
	return open(context.Background(), registry.Request{Root: root, Options: o, Workers: 1, Warn: func(s string) { *warned = append(*warned, s) }})
}

// TestAdmissionKeepsCredentialFailure: credentials that cannot be had
// fail namespace admission as what they are (class auth, the command's
// --anonymous hint), not as a namespace that could not be established
// (unsupported, exit 2).
func TestAdmissionKeepsCredentialFailure(t *testing.T) {
	c := &pages{err: &objhttp.CredentialError{Source: "Entra ID (DefaultAzureCredential)", Err: errors.New("no credential in the chain")}}
	_, err := New(context.Background(), Options{Account: "acct", Container: "c", Client: c, Workers: 1})
	var ce *objhttp.CredentialError
	if !errors.As(err, &ce) || !ce.CredentialRefused() || errors.Is(err, source.ErrUnsupported) || objhttp.Label(err) != "auth" ||
		strings.Contains(err.Error(), "cannot establish") || !strings.HasPrefix(err.Error(), "Azure Blob client: credentials (") {
		t.Fatalf("%v (class %q)", err, objhttp.Label(err))
	}
	if c.lists.Load() != 0 {
		t.Fatalf("%d list requests", c.lists.Load())
	}
}

// TestAnonymousCredentialConflicts: --anonymous beside an explicit SAS,
// given by flag or in the source URL, is refused (exit 2), as for every
// provider, before any request.
func TestAnonymousCredentialConflicts(t *testing.T) {
	for _, c := range []struct {
		root string
		args []string
		want string
	}{
		{"az://acct/c", []string{"--azure-sas-token", "sv=1&sig=x"}, "--anonymous excludes --azure-sas-token"},
		{"az://acct/c?sv=1&sig=x", nil, "--anonymous excludes the SAS in the source URL"},
		{"az://acct/c?" + RedactedSAS, nil, "--anonymous excludes the SAS in the source URL"},
	} {
		var warned []string
		src, err := openFlags(t, c.root, &warned, append(c.args, "--anonymous", "--endpoint-url", "http://127.0.0.1:1")...)
		if src != nil || !errors.Is(err, source.ErrUnsupported) || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%s %q: opened %v, error %v", c.root, c.args, src, err)
		}
	}
}

// TestAnonymousNote: ambient Azure credentials --anonymous ignores are
// named in the one-line warning every provider gives.
func TestAnonymousNote(t *testing.T) {
	r := azblobtest.Serve(t, []string{"a"})
	for _, v := range []string{EnvSAS, EnvKey, "AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET", "AZURE_CLIENT_CERTIFICATE_PATH",
		"AZURE_USERNAME", "AZURE_FEDERATED_TOKEN_FILE", "IDENTITY_ENDPOINT", "MSI_ENDPOINT", "AZURE_TOKEN_CREDENTIALS"} {
		t.Setenv(v, "")
	}
	t.Setenv(EnvKey, "a2V5")
	var warned []string
	if _, err := openFlags(t, "az://"+r.Account+"/"+r.Container, &warned, "--anonymous", "--endpoint-url", base(r)); err != nil {
		t.Fatal(err)
	}
	want := "--anonymous: ignoring the Azure Blob credentials in the environment (AZURE_STORAGE_KEY)"
	if len(warned) != 1 || warned[0] != want {
		t.Fatalf("warnings %q, want %q", warned, want)
	}
}

// TestAdmissionKeepsServiceRefusal: a signed caller whose credentials the
// service refuses at namespace admission (a SAS failing authentication,
// a principal without access) fails as that refusal, class auth or
// access denied, never as a namespace that could not be established; an
// anonymous caller's 401 stays the unknown namespace (TestAdmission).
func TestAdmissionKeepsServiceRefusal(t *testing.T) {
	t.Setenv(EnvKey, "")
	for _, c := range []struct {
		status int
		code   string
		class  string
	}{
		{403, "AuthenticationFailed", "auth"},
		{403, "AuthorizationFailure", "access_denied"},
		{401, "", "auth"},
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if c.code != "" {
				w.Header().Set("x-ms-error-code", c.code)
			}
			w.WriteHeader(c.status)
			fmt.Fprintf(w, `<?xml version="1.0" encoding="utf-8"?><Error><Code>%s</Code><Message>no</Message></Error>`, c.code)
		}))
		_, err := New(context.Background(), Options{Account: "acct", Container: "c", Endpoint: srv.URL, SAS: "sv=2024-01-01&sig=INVALID", Workers: 1})
		srv.Close()
		if err == nil || errors.Is(err, source.ErrUnsupported) || objhttp.Label(err) != c.class || strings.Contains(err.Error(), "cannot establish") {
			t.Errorf("%d %s: %v (class %q), want %s", c.status, c.code, err, objhttp.Label(err), c.class)
		}
	}
}
