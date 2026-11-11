// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/source/azblob/azblobtest"
	"github.com/varveio/swath/go/internal/source/azblob/client"
	"github.com/varveio/swath/go/internal/source/objhttp"
)

// TestFetchFlags: a default listing sends no include and leaves the asked-for
// columns null; each fetch flag sends its include on every data request (a
// probe leaves it out) and fills its column, a blob without any getting the
// empty value and one encrypted with a customer-provided key null metadata;
// the properties every listing carries reach the row either way.
func TestFetchFlags(t *testing.T) {
	entries := []client.Item{
		{Name: "a", Size: 1, Props: client.Props{LeaseState: "available", AccessTierInferred: client.True,
			Metadata: `{"k":"v"}`, Tags: `{"t":"1"}`, TagCount: 1, ImmutabilityMode: "Locked", ImmutabilityUntil: 5, LegalHold: client.True}},
		{Name: "b", Size: 2},
		{Name: "c", Size: 3, Props: client.Props{CustomerProvidedKeySHA256: "key="}},
	}
	for _, c := range []struct {
		name string
		set  func(*Options)
		want client.Include
		rows [3]client.Props
	}{
		{"default", func(*Options) {}, client.Include{}, [3]client.Props{
			{LeaseState: "available", AccessTierInferred: client.True, TagCount: 1}, {}, {CustomerProvidedKeySHA256: "key="}}},
		{"metadata", func(o *Options) { o.FetchMetadata = true }, client.Include{Metadata: true}, [3]client.Props{
			{LeaseState: "available", AccessTierInferred: client.True, TagCount: 1, Metadata: `{"k":"v"}`}, {Metadata: "{}"},
			{CustomerProvidedKeySHA256: "key="}}},
		{"tags", func(o *Options) { o.FetchTags = true }, client.Include{Tags: true}, [3]client.Props{
			{LeaseState: "available", AccessTierInferred: client.True, TagCount: 1, Tags: `{"t":"1"}`}, {Tags: "{}"},
			{CustomerProvidedKeySHA256: "key=", Tags: "{}"}}},
		{"immutability", func(o *Options) { o.FetchImmutability = true }, client.Include{Immutability: true}, [3]client.Props{
			{LeaseState: "available", AccessTierInferred: client.True, TagCount: 1, ImmutabilityMode: "Locked", ImmutabilityUntil: 5, LegalHold: client.True},
			{LegalHold: client.False}, {CustomerProvidedKeySHA256: "key=", LegalHold: client.False}}},
	} {
		t.Run(c.name, func(t *testing.T) {
			st := azblobtest.NewEntryStore(entries)
			opt := Options{Account: "acct", Container: "c", Client: st}
			c.set(&opt)
			rows := listRows(t, opt)
			if len(rows) != 3 {
				t.Fatalf("%d rows", len(rows))
			}
			for i, r := range rows {
				if r.Data.Props != c.rows[i] {
					t.Errorf("%s: %+v, want %+v", r.Key, r.Data.Props, c.rows[i])
				}
			}
			for _, r := range st.Log() {
				if r.In.Delimiter == "" && r.In.Include != c.want || r.In.Delimiter != "" && r.In.Include != (client.Include{}) {
					t.Errorf("sent %+v (delimiter %q), want %+v", r.In.Include, r.In.Delimiter, c.want)
				}
			}
		})
	}
}

func listRows(t *testing.T, opt Options) []Row {
	t.Helper()
	rows, _ := list(t, opt, 2)
	return rows
}

// refusing is a flat account whose listings are refused as Azure refuses
// include=tags to a SAS without the t permission.
type refusing struct{ *azblobtest.Store }

func (refusing) List(context.Context, *client.ListInput) (*client.ListOutput, error) {
	return nil, &client.Error{Status: 403, Code: "AuthorizationPermissionMismatch",
		Message: "This request is not authorized to perform this operation using this permission."}
}

// rejecting is a flat account whose listings fail on a rejected
// credential, not on a missing permission.
type rejecting struct{ *azblobtest.Store }

func (rejecting) List(context.Context, *client.ListInput) (*client.ListOutput, error) {
	return nil, &client.Error{Status: 403, Code: "AuthenticationFailed", Message: "Server failed to authenticate the request."}
}

// TestFetchTagsHintOnlyForPermission: a rejected credential with
// --azure-fetch-tags is reported as it is, without the tags hint, which
// would point at the wrong cause.
func TestFetchTagsHintOnlyForPermission(t *testing.T) {
	_, _, err := run(t, Options{Account: "acct", Container: "c", Client: rejecting{azblobtest.NewStore([]string{"a"})}, FetchTags: true}, 1)
	if err == nil || strings.Contains(err.Error(), "t permission") {
		t.Fatalf("%v", err)
	}
}

// TestFetchTagsRefused: a listing refused while it asks for tags names the
// permission tags need, its class kept (access denied); without the flag
// the same refusal is reported as it is.
func TestFetchTagsRefused(t *testing.T) {
	for _, tags := range []bool{true, false} {
		_, _, err := run(t, Options{Account: "acct", Container: "c", Client: refusing{azblobtest.NewStore([]string{"a"})}, FetchTags: tags}, 1)
		if err == nil {
			t.Fatal("listed")
		}
		if f := objhttp.Normalize(err); f == nil || (&client.Error{Status: f.Status, Code: f.Code}).Failure().Class != objhttp.ClassAccessDenied {
			t.Errorf("tags=%v: %v is not access denied", tags, err)
		}
		if strings.Contains(err.Error(), "t permission") != tags {
			t.Errorf("tags=%v: %v", tags, err)
		}
	}
}

// TestFetchIdentity: a fetch flag is part of the resume identity, named
// only when given, so a default run's identity is as before the flags.
func TestFetchIdentity(t *testing.T) {
	names := func(opt Options) []string {
		t.Helper()
		opt.Client = &routed{pages: &pages{}}
		src, err := open(context.Background(), registry.Request{Root: "az://acct/c", Options: &opt, Workers: 1})
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, f := range src.(source.Identified).Identity().Fields {
			out = append(out, f.Name+"="+f.Value)
		}
		return out
	}
	def := names(Options{})
	if slices.ContainsFunc(def, func(s string) bool { return strings.HasPrefix(s, "fetch") }) {
		t.Fatalf("default identity %q", def)
	}
	all := names(Options{FetchMetadata: true, FetchTags: true, FetchImmutability: true})
	if want := append(def, "fetch_metadata=true", "fetch_tags=true", "fetch_immutability=true"); !slices.Equal(all, want) {
		t.Fatalf("identity %q, want %q", all, want)
	}
}
