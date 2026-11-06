// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"flag"
	"testing"

	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/source/keyrange"
)

// TestIdentityFreezesEffectiveRoute: with neither an endpoint nor a region
// given, the identity records what the environment resolved, so a resume
// in an environment that selects another region or AWS partition (and so
// possibly another bucket of the same name) cannot match.
func TestIdentityFreezesEffectiveRoute(t *testing.T) {
	t.Setenv("AWS_CONFIG_FILE", "/nonexistent")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent")
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
	if us["route"] != "https://bkt.s3.us-west-2.amazonaws.com/" || us["region"] != "us-west-2" {
		t.Fatalf("us-west-2 identity: %v", us)
	}
	if cn["route"] != "https://bkt.s3.cn-north-1.amazonaws.com.cn/" || cn["region"] != "cn-north-1" {
		t.Fatalf("cn-north-1 identity: %v", cn)
	}
	if fips := ident("us-west-2", Options{FIPS: true}); fips["route"] == us["route"] {
		t.Fatalf("the FIPS route has the plain route's identity: %v", fips)
	}
	if dual := ident("us-west-2", Options{DualStack: true}); dual["route"] == us["route"] {
		t.Fatalf("the dual-stack route has the plain route's identity: %v", dual)
	}
	// An explicit endpoint is recorded as the client uses it; the region is
	// still the resolved one. Scheme and host case do not matter.
	ep := ident("eu-west-1", Options{Endpoint: "HTTP://Example.COM:9000/"})
	if ep["route"] != "http://bkt.example.com:9000/" || ep["region"] != "eu-west-1" {
		t.Fatalf("explicit endpoint identity: %v", ep)
	}
	// The addressing style is part of the route: https://store/bkt and
	// https://bkt.store/ can be different namespaces.
	path := ident("eu-west-1", Options{Endpoint: "https://store.example", PathStyle: true})
	if path["route"] != "https://store.example/bkt" || path["route"] == ident("eu-west-1", Options{Endpoint: "https://store.example"})["route"] {
		t.Fatalf("path-style identity: %v", path)
	}
	// Endpoint paths are kept byte for byte past the client's own trailing
	// slash folding: /base and /base/ request the same URLs, /base// does not.
	route := func(e string) string { return ident("eu-west-1", Options{Endpoint: e, PathStyle: true})["route"] }
	if a, b, c := route("http://store/base"), route("http://store/base/"), route("http://store/base//"); a != b || b == c || a != "http://store/base/bkt" {
		t.Fatalf("endpoint paths: %q %q %q", a, b, c)
	}
}

// TestSuppliedClientIdentity: a supplied client that names its route is
// identified by it; one that does not declines resume, so two unrelated
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
		if f.Name == "route" && f.Value != "https://elsewhere/bkt" {
			t.Fatalf("route %q", f.Value)
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
	p := src.(*opened).p
	if !other.Policy.Seed || !p.opt.Seed || !p.opt.Floors || !p.opt.Anonymous || !p.Options().Floors {
		t.Fatalf("policy not shared: %+v", p.opt.Policy)
	}
	// Defaults (no flags parsed) hold the engine's defaults.
	src, err = open(context.Background(), registry.Request{Root: "s3://bkt", Options: defaults(), Workers: 2})
	if err != nil || src.(*opened).p.opt.Seed {
		t.Fatalf("defaults: %v", err)
	}
}
