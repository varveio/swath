// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/spf13/pflag"

	"github.com/varveio/swath/internal/registry"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/azblob"
	"github.com/varveio/swath/internal/source/azblob/azblobtest"
	"github.com/varveio/swath/internal/source/keyrange"
)

// azblobBase is a replay's endpoint without the account segment, as
// --endpoint-url takes it.
func azblobBase(r *azblobtest.Replay) string { return strings.TrimSuffix(r.Endpoint, "/"+r.Account) }

// TestAzblobAnonymousCredentialConflicts: --anonymous beside an explicit
// SAS, given by flag or in the source URL, is refused (exit 2), as for
// every provider, before any request.
func TestAzblobAnonymousCredentialConflicts(t *testing.T) {
	for _, c := range []struct {
		root string
		args []string
		want string
	}{
		{"az://acct/c", []string{"--azure-sas-token", "sv=1&sig=x"}, "--anonymous excludes --azure-sas-token"},
		{"az://acct/c?sv=1&sig=x", nil, "--anonymous excludes the SAS in the source URL"},
		{"az://acct/c?" + azblob.RedactedSAS, nil, "--anonymous excludes the SAS in the source URL"},
	} {
		src, err := openArgs(t, c.root, false, nil, append(c.args, "--anonymous", "--endpoint-url", "http://127.0.0.1:1")...)
		if src != nil || !errors.Is(err, source.ErrUnsupported) || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%s %q: opened %v, error %v", c.root, c.args, src, err)
		}
	}
}

// TestAzblobAnonymousNote: ambient Azure credentials --anonymous ignores
// are named in the one-line warning every provider gives.
func TestAzblobAnonymousNote(t *testing.T) {
	r := azblobtest.Serve(t, []string{"a"})
	for _, v := range []string{azblob.EnvSAS, azblob.EnvKey, "AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET", "AZURE_CLIENT_CERTIFICATE_PATH",
		"AZURE_USERNAME", "AZURE_FEDERATED_TOKEN_FILE", "IDENTITY_ENDPOINT", "MSI_ENDPOINT", "AZURE_TOKEN_CREDENTIALS"} {
		t.Setenv(v, "")
	}
	t.Setenv(azblob.EnvKey, "a2V5")
	var warned []string
	if _, err := openArgs(t, "az://"+r.Account+"/"+r.Container, false, func(_, s string) { warned = append(warned, s) }, "--anonymous", "--endpoint-url", azblobBase(r)); err != nil {
		t.Fatal(err)
	}
	want := "--anonymous: ignoring the Azure Blob credentials in the environment (AZURE_STORAGE_KEY)"
	if len(warned) != 1 || warned[0] != want {
		t.Fatalf("warnings %q, want %q", warned, want)
	}
}

// TestAzblobEngineFlagsShared: the provider takes the store's and the
// range engine's flag groups the command registered once, and its own
// prefixed flags (the engine takes its policy from the options: the
// provider's TestPolicyReachesEngine).
func TestAzblobEngineFlagsShared(t *testing.T) {
	r := azblobtest.Serve(t, []string{"a"})
	f := factory(t, "az://replay/corpus")
	fs := pflag.NewFlagSet("t", pflag.ContinueOnError)
	v := f.RegisterFlags(fs, true)
	if err := fs.Parse([]string{"--structure", "--page-size", "7", "--anonymous", "--endpoint-url", azblobBase(r), "--request-timeout", "7s", "--azure-assume-flat"}); err != nil {
		t.Fatal(err)
	}
	src, err := f.Open(context.Background(), registry.Request{Root: "az://replay/corpus", Options: v, Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	opt, err := v.(*azblobFlagged).options("")
	if err != nil || !opt.Seed || opt.Structure != keyrange.StructureOn || !opt.Anonymous || opt.Timeout.Seconds() != 7 || opt.PageSize != 7 || !opt.Flat {
		t.Fatalf("flags not applied: %+v (%v)", opt, err)
	}
	if got := identityField(src.(source.Identified).Identity(), "flat_asserted"); got != "true" {
		t.Fatalf("flat_asserted %q", got)
	}
}
