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

	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/objhttp"
)

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
