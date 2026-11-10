// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/spf13/pflag"

	"github.com/varveio/swath/go/internal/journal"
	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
)

// TestIdentityRecordsInputs: the identity is the inputs the listing was
// asked with, compared as given: the bucket and prefix, the endpoint, the
// region (with no --s3-region, what the environment resolved, so a resume in
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
	// Path-style, endpoint paths are kept byte for byte past one trailing
	// slash: /base and /base/ send the same requests, /base// does not.
	endpoint := func(e string, pathStyle bool) string {
		return ident("eu-west-1", Options{Endpoint: e, PathStyle: pathStyle})["endpoint"]
	}
	if a, b, c := endpoint("http://store/base", true), endpoint("http://store/base/", true), endpoint("http://store/base//", true); a != b || b == c || a != "http://store/base" {
		t.Fatalf("path-style endpoint paths: %q %q %q", a, b, c)
	}
	// Virtual-host, the SDK keeps the path as given (bkt.store/base and
	// bkt.store/base/ are two request paths): no slash is folded, but for
	// an endpoint with no path ("/" is sent either way).
	if a, b := endpoint("https://store/base", false), endpoint("https://store/base/", false); a == b || a != "https://store/base" || b != "https://store/base/" {
		t.Fatalf("virtual-host endpoint paths: %q %q", a, b)
	}
	if a, b := endpoint("https://Store", false), endpoint("https://store/", false); a != b || a != "https://store" {
		t.Fatalf("virtual-host endpoint without a path: %q %q", a, b)
	}
}

// TestResumeRefusesEndpointPath: a journal's identity is checked against the
// resume's (journal.Check, as the pipeline does): virtual-host, an endpoint
// path with and without its trailing slash names two request paths, so
// either refuses a resume of the other; path-style, the two send the same
// requests and resume each other. A supplied client's route is compared
// byte for byte.
func TestResumeRefusesEndpointPath(t *testing.T) {
	s3test.NoCredentials(t)
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_DEFAULT_REGION", "")
	identity := func(opt Options) journal.Identity {
		t.Helper()
		if opt.Client == nil {
			opt.Anonymous = true
		}
		src, err := open(context.Background(), registry.Request{Root: "s3://bkt/p/", Options: &opt, Workers: 1})
		if err != nil {
			t.Fatal(err)
		}
		id := journal.Identity{Provider: "s3"}
		for _, f := range src.(source.Identified).Identity().Fields {
			id.Source = append(id.Source, journal.Field{Name: f.Name, Value: f.Value, Canonical: f.Canonical})
		}
		return id
	}
	for _, c := range []struct {
		name        string
		stored, now Options
		refused     bool
	}{
		{"virtual-host /base, now /base/", Options{Endpoint: "https://store.example/base"}, Options{Endpoint: "https://store.example/base/"}, true},
		{"virtual-host /base/, now /base", Options{Endpoint: "https://store.example/base/"}, Options{Endpoint: "https://store.example/base"}, true},
		{"virtual-host, host case", Options{Endpoint: "https://store.example/base/"}, Options{Endpoint: "HTTPS://Store.Example/base/"}, false},
		{"virtual-host, no path, now /", Options{Endpoint: "https://store.example"}, Options{Endpoint: "https://store.example/"}, false},
		{"path-style /base, now /base/", Options{Endpoint: "https://store.example/base", PathStyle: true}, Options{Endpoint: "https://store.example/base/", PathStyle: true}, false},
		{"the same route", Options{Client: routed{&stub{}, "https://elsewhere/x/"}}, Options{Client: routed{&stub{}, "https://elsewhere/x/"}}, false},
		{"route without, now with a slash", Options{Client: routedSuffix{routed{&stub{}, "https://elsewhere/x/"}, ""}},
			Options{Client: routedSuffix{routed{&stub{}, "https://elsewhere/x/"}, "/"}}, true},
	} {
		err := journal.Check(identity(c.stored), identity(c.now))
		var mm *journal.MismatchError
		if c.refused != (err != nil) || err != nil && (!errors.As(err, &mm) || mm.Field != "endpoint") {
			t.Errorf("%s: %v, want refused %v (field endpoint)", c.name, err, c.refused)
		}
	}
}

// routedSuffix names its route with suffix appended.
type routedSuffix struct {
	routed
	suffix string
}

func (r routedSuffix) Route(bucket string) string { return r.routed.Route(bucket) + r.suffix }

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

// factory is the registered S3 factory.
func factory(t *testing.T) registry.Factory {
	t.Helper()
	f, err := registry.Lookup("s3://bkt")
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// TestEngineFlagsShared: S3 takes the store's and the range engine's flag
// groups the command registered once and passed to it, and opens with the
// parsed values.
func TestEngineFlagsShared(t *testing.T) {
	s3test.NoCredentials(t)
	f := pflag.NewFlagSet("t", pflag.ContinueOnError)
	v := factory(t).RegisterFlags(f, true)
	if err := f.Parse([]string{"--floors", "--anonymous", "--endpoint-url", "http://127.0.0.1:1", "--request-timeout", "7s"}); err != nil {
		t.Fatal(err)
	}
	src, err := open(context.Background(), registry.Request{Root: "s3://bkt", Options: v, Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	p := src.(*opened[Object]).p
	if !p.opt.Seed || !p.opt.Floors || !p.opt.Anonymous || !p.Options().Floors || p.opt.Endpoint != "http://127.0.0.1:1" || p.opt.Timeout.Seconds() != 7 {
		t.Fatalf("shared flags not applied: %+v", p.opt)
	}
	// Defaults (no flags parsed) hold the engine's defaults. Anonymous is set
	// so the client builds without credentials (none are configured here).
	o := defaults().(*flagged)
	o.store = &source.Store{Anonymous: true}
	src, err = open(context.Background(), registry.Request{Root: "s3://bkt", Options: o, Workers: 2})
	if err != nil || src.(*opened[Object]).p.opt.Seed {
		t.Fatalf("defaults: %v", err)
	}
}
