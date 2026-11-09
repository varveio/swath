// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"flag"
	"reflect"
	"testing"

	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
)

// TestIdentityRecordsInputs: the identity is the inputs the listing was
// asked with, compared as given: the bucket and prefix, the endpoint, the
// region (with no --region, what the environment resolved, so a resume in
// an environment that selects another region or AWS partition, and so
// possibly another bucket of the same name, cannot match), the addressing
// flags and the owner columns.
func TestIdentityRecordsInputs(t *testing.T) {
	s3test.NoCredentials(t)
	t.Setenv("AWS_DEFAULT_REGION", "")
	ident := func(region string, opt Options) map[string]string {
		t.Helper()
		t.Setenv("AWS_REGION", region)
		opt.Anonymous = true
		src, err := open(context.Background(), registry.Request{Root: "s3://bkt/p/", Options: &opt, Workers: 1})
		if err != nil {
			t.Fatal(err)
		}
		m := map[string]string{}
		for _, f := range src.(source.Identified).Identity().Fields {
			m[f.Name] = f.Canonical
			if f.Canonical == "" {
				m[f.Name] = f.Value
			}
		}
		return m
	}
	us, cn := ident("us-west-2", Options{}), ident("cn-north-1", Options{})
	want := map[string]string{"source": "s3://bkt/p/", "endpoint": "", "region": "us-west-2", "path style": "false", "fips": "false",
		"dual-stack": "false", "fetch owner": "false"}
	if !reflect.DeepEqual(us, want) {
		t.Fatalf("us-west-2 identity: %v, want %v", us, want)
	}
	if cn["region"] != "cn-north-1" || reflect.DeepEqual(us, cn) {
		t.Fatalf("cn-north-1 identity: %v", cn)
	}
	for name, opt := range map[string]Options{"FIPS": {FIPS: true}, "dual-stack": {DualStack: true}, "path-style": {PathStyle: true},
		"fetch owner": {FetchOwner: true}, "region": {Region: "eu-west-1"}} {
		if reflect.DeepEqual(ident("us-west-2", opt), us) {
			t.Errorf("%s: the plain identity", name)
		}
	}
	// A FIPS region alias is recorded as given: nothing resolves it.
	if alias := ident("us-west-2", Options{Region: "fips-us-east-1"}); alias["region"] != "fips-us-east-1" ||
		reflect.DeepEqual(alias, ident("us-west-2", Options{Region: "us-east-1", FIPS: true})) {
		t.Fatalf("FIPS alias identity: %v", alias)
	}
	// An explicit endpoint is recorded as given; the region is still the
	// resolved one. Scheme and host case do not matter.
	ep := ident("eu-west-1", Options{Endpoint: "HTTP://Example.COM:9000/"})
	if ep["endpoint"] != "http://example.com:9000" || ep["region"] != "eu-west-1" {
		t.Fatalf("explicit endpoint identity: %v", ep)
	}
	// The addressing style is part of the identity: https://store/bkt and
	// https://bkt.store/ can be different namespaces.
	if path := ident("eu-west-1", Options{Endpoint: "https://store.example", PathStyle: true}); reflect.DeepEqual(path, ident("eu-west-1", Options{Endpoint: "https://store.example"})) {
		t.Fatalf("path-style identity: %v", path)
	}
	// Endpoint paths are kept byte for byte past one trailing slash: /base
	// and /base/ send the same requests, /base// does not.
	endpoint := func(e string) string { return ident("eu-west-1", Options{Endpoint: e, PathStyle: true})["endpoint"] }
	if a, b, c := endpoint("http://store/base"), endpoint("http://store/base/"), endpoint("http://store/base//"); a != b || b == c || a != "http://store/base" {
		t.Fatalf("endpoint paths: %q %q %q", a, b, c)
	}
}

// TestSuppliedClientIdentity: a supplied client that names its route is
// identified by it (in place of the endpoint); one that does not declines resume, so two unrelated
// clients never share an identity.
func TestSuppliedClientIdentity(t *testing.T) {
	anon, err := open(context.Background(), registry.Request{Root: "s3://bkt", Options: &Options{Client: &stub{}}, Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	if anon.(source.Capable).Capabilities().Resume {
		t.Fatal("a supplied client without a route declares resume")
	}
	named, err := open(context.Background(), registry.Request{Root: "s3://bkt", Options: &Options{Client: routed{&stub{}, "https://elsewhere/"}}, Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !named.(source.Capable).Capabilities().Resume {
		t.Fatal("a supplied client with a route declines resume")
	}
	for _, f := range named.(source.Identified).Identity().Fields {
		if f.Name == "endpoint" && f.Value != "https://elsewhere/bkt" {
			t.Fatalf("endpoint %q", f.Value)
		}
	}
}

type routed struct {
	*stub
	base string
}

func (r routed) Route(bucket string) string { return r.base + bucket }

// TestCappedRunCannotResume: an object-count cap counts what one process
// listed, so a capped run declares no resume (it keeps no journal).
func TestCappedRunCannotResume(t *testing.T) {
	src, err := open(context.Background(), registry.Request{Root: "s3://bkt", Options: &Options{Anonymous: true, Endpoint: "http://127.0.0.1:1", Policy: keyrange.Policy{Sequential: true, MaxKeys: 5}}, Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	if src.(source.Capable).Capabilities().Resume {
		t.Fatal("a capped run declares resume")
	}
}

// TestEngineFlagsShared: S3 takes the range engine's flag group the command
// registered once and passed to it, and opens with the parsed policy.
func TestEngineFlagsShared(t *testing.T) {
	f := flag.NewFlagSet("t", flag.ContinueOnError)
	other := keyrange.Flags(f, true)
	v := flags(f, true, other)
	if err := f.Parse([]string{"-seed", "-floors", "-anonymous", "-endpoint", "http://127.0.0.1:1"}); err != nil {
		t.Fatal(err)
	}
	src, err := open(context.Background(), registry.Request{Root: "s3://bkt", Options: v, Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	p := src.(*opened[Object]).p
	if !other.Policy.Seed || !p.opt.Seed || !p.opt.Floors || !p.opt.Anonymous || !p.Options().Floors {
		t.Fatalf("policy not shared: %+v", p.opt.Policy)
	}
	// Defaults (no flags parsed) hold the engine's defaults.
	src, err = open(context.Background(), registry.Request{Root: "s3://bkt", Options: defaults(), Workers: 2})
	if err != nil || src.(*opened[Object]).p.opt.Seed {
		t.Fatalf("defaults: %v", err)
	}
}
