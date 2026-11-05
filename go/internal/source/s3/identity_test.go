// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"testing"

	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/source"
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
	if us["endpoint"] != "https://s3.us-west-2.amazonaws.com" || us["region"] != "us-west-2" {
		t.Fatalf("us-west-2 identity: %v", us)
	}
	if cn["endpoint"] != "https://s3.cn-north-1.amazonaws.com.cn" || cn["region"] != "cn-north-1" {
		t.Fatalf("cn-north-1 identity: %v", cn)
	}
	if fips := ident("us-west-2", Options{FIPS: true}); fips["endpoint"] == us["endpoint"] {
		t.Fatalf("the FIPS route has the plain route's identity: %v", fips)
	}
	// An explicit endpoint is recorded as the client uses it; the region is
	// still the resolved one.
	ep := ident("eu-west-1", Options{Endpoint: "HTTP://Example.COM:9000/"})
	if ep["endpoint"] != "http://example.com:9000" || ep["region"] != "eu-west-1" {
		t.Fatalf("explicit endpoint identity: %v", ep)
	}
}

// TestCappedRunCannotResume: an object-count cap counts what one process
// listed, so a capped run declares no resume (it keeps no journal).
func TestCappedRunCannotResume(t *testing.T) {
	src, err := open(context.Background(), registry.Request{Root: "s3://bkt", Options: &Options{Anonymous: true, Endpoint: "http://127.0.0.1:1", Sequential: true, MaxKeys: 5}, Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	if src.(source.Capable).Capabilities().Resume {
		t.Fatal("a capped run declares resume")
	}
}
