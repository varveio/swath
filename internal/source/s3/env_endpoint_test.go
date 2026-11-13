// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/spf13/pflag"

	"github.com/varveio/swath/internal/registry"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/s3/client"
	"github.com/varveio/swath/internal/source/s3/s3test"
)

// openArgs opens s3://bkt with the command-line flags args.
func openArgs(t *testing.T, args ...string) (*opened[Object], error) {
	t.Helper()
	f := pflag.NewFlagSet("t", pflag.ContinueOnError)
	o := factory(t).RegisterFlags(f, false)
	if err := f.Parse(args); err != nil {
		t.Fatal(err)
	}
	src, err := open(context.Background(), registry.Request{Root: "s3://bkt", Options: o, Workers: 1, Warn: func(string) {}})
	if err != nil {
		return nil, err
	}
	return src.(*opened[Object]), nil
}

// clearEndpointEnv unsets the AWS endpoint variables the host may hold.
func clearEndpointEnv(t *testing.T) {
	for _, v := range []string{"AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL", "AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "AWS_USE_FIPS_ENDPOINT", "AWS_USE_DUALSTACK_ENDPOINT"} {
		t.Setenv(v, "")
	}
}

// TestEndpointFromEnvironment: AWS_ENDPOINT_URL_S3, else AWS_ENDPOINT_URL,
// is --endpoint-url when the flag is not given: the request goes to it
// (path-style), the identity records it and no tariff is claimed;
// AWS_IGNORE_CONFIGURED_ENDPOINT_URLS turns both off, and the flag wins.
func TestEndpointFromEnvironment(t *testing.T) {
	s3test.NoCredentials(t)
	clearEndpointEnv(t)
	t.Setenv("AWS_REGION", "us-east-1")
	var hits atomic.Int64
	var path atomic.Value
	srv := s3test.Serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		path.Store(r.URL.Path)
		w.Write([]byte(`<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`))
	}))
	const elsewhere = "http://127.0.0.1:1" // nothing listens: a request there fails

	endpoint := func(o *opened[Object]) string { return o.Identity().Fields[1].Value }
	for _, c := range []struct {
		name string
		env  map[string]string
		args []string
		want string
	}{
		{"AWS_ENDPOINT_URL", map[string]string{"AWS_ENDPOINT_URL": srv}, nil, srv},
		{"AWS_ENDPOINT_URL_S3 first", map[string]string{"AWS_ENDPOINT_URL_S3": srv, "AWS_ENDPOINT_URL": elsewhere}, nil, srv},
		{"the flag wins", map[string]string{"AWS_ENDPOINT_URL_S3": elsewhere, "AWS_ENDPOINT_URL": elsewhere}, []string{"--endpoint-url", srv}, srv},
		{"ignored", map[string]string{"AWS_ENDPOINT_URL": elsewhere, "AWS_IGNORE_CONFIGURED_ENDPOINT_URLS": "true"}, nil, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			for k, v := range c.env {
				t.Setenv(k, v)
			}
			o, err := openArgs(t, append(c.args, "--anonymous")...)
			if err != nil {
				t.Fatal(err)
			}
			if got := endpoint(o); got != c.want {
				t.Fatalf("identity endpoint %q, want %q", got, c.want)
			}
			if c.want == "" {
				if usd, _ := o.Tariff(); usd == 0 || o.p.opt.PathStyle {
					t.Fatalf("AWS's own endpoint: tariff %v, path style %v", usd, o.p.opt.PathStyle)
				}
				return
			}
			if usd, _ := o.Tariff(); usd != 0 || !o.p.opt.PathStyle {
				t.Fatalf("a custom endpoint: tariff %v, path style %v", usd, o.p.opt.PathStyle)
			}
			before := hits.Load()
			if _, err := o.p.client.ListObjectsV2(context.Background(), &client.ListInput{Bucket: "bkt"}); err != nil {
				t.Fatal(err)
			}
			if hits.Load() != before+1 || path.Load() != "/bkt" {
				t.Fatalf("%d requests reached the listener, the last for %v", hits.Load()-before, path.Load())
			}
		})
	}
}

// TestFIPSAndDualStackFromEnvironment: AWS_USE_FIPS_ENDPOINT and
// AWS_USE_DUALSTACK_ENDPOINT are --s3-fips and --s3-dualstack, which an
// explicit flag overrides.
func TestFIPSAndDualStackFromEnvironment(t *testing.T) {
	s3test.NoCredentials(t)
	clearEndpointEnv(t)
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_USE_FIPS_ENDPOINT", "TRUE")
	t.Setenv("AWS_USE_DUALSTACK_ENDPOINT", "true")
	o, err := openArgs(t, "--anonymous")
	if err != nil {
		t.Fatal(err)
	}
	if !o.p.opt.FIPS || !o.p.opt.DualStack {
		t.Fatalf("fips %v, dual-stack %v from the environment", o.p.opt.FIPS, o.p.opt.DualStack)
	}
	o, err = openArgs(t, "--anonymous", "--s3-fips=false", "--s3-dualstack=false")
	if err != nil {
		t.Fatal(err)
	}
	if o.p.opt.FIPS || o.p.opt.DualStack {
		t.Fatalf("the flags did not win: fips %v, dual-stack %v", o.p.opt.FIPS, o.p.opt.DualStack)
	}
}

// TestNoEffectRefused: FIPS or dual-stack with a custom endpoint selects
// nothing, so the pair is refused (exit 2), naming where each came from.
func TestNoEffectRefused(t *testing.T) {
	s3test.NoCredentials(t)
	for _, c := range []struct {
		env  map[string]string
		args []string
		want string
	}{
		{nil, []string{"--endpoint-url=http://store.example", "--s3-fips"}, "--s3-fips has no effect with --endpoint-url"},
		{nil, []string{"--endpoint-url=http://store.example", "--s3-dualstack"}, "--s3-dualstack has no effect with --endpoint-url"},
		{map[string]string{"AWS_ENDPOINT_URL": "http://store.example"}, []string{"--s3-dualstack"}, "--s3-dualstack has no effect with AWS_ENDPOINT_URL"},
		{map[string]string{"AWS_USE_FIPS_ENDPOINT": "true"}, []string{"--endpoint-url=http://store.example"}, "AWS_USE_FIPS_ENDPOINT has no effect with --endpoint-url"},
	} {
		t.Run(c.want, func(t *testing.T) {
			clearEndpointEnv(t)
			for k, v := range c.env {
				t.Setenv(k, v)
			}
			o, err := openArgs(t, append(c.args, "--anonymous")...)
			if o != nil || !errors.Is(err, source.ErrUnsupported) || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("opened %v, error %v", o, err)
			}
		})
	}
}

// TestAnonymousCredentialConflicts: --anonymous beside an explicit S3
// credential is refused (exit 2), as for every provider.
func TestAnonymousCredentialConflicts(t *testing.T) {
	s3test.NoCredentials(t)
	clearEndpointEnv(t)
	for _, flag := range []string{"--s3-profile=work", "--s3-bearer-token-command=true"} {
		name, _, _ := strings.Cut(flag, "=")
		o, err := openArgs(t, "--anonymous", flag)
		if o != nil || !errors.Is(err, source.ErrUnsupported) || !strings.Contains(err.Error(), "--anonymous excludes "+name) {
			t.Fatalf("%s: opened %v, error %v", flag, o, err)
		}
	}
}

// TestAnonymousNote: the warning names the AWS credentials --anonymous
// ignores, among them those the old note left out.
func TestAnonymousNote(t *testing.T) {
	s3test.NoCredentials(t)
	clearEndpointEnv(t)
	t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::1:role/r")
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", "/nonexistent")
	var warned []string
	f := pflag.NewFlagSet("t", pflag.ContinueOnError)
	o := factory(t).RegisterFlags(f, false)
	if err := f.Parse([]string{"--anonymous", "--endpoint-url=http://store.example"}); err != nil {
		t.Fatal(err)
	}
	if _, err := open(context.Background(), registry.Request{Root: "s3://bkt", Options: o, Workers: 1, Warn: func(s string) { warned = append(warned, s) }}); err != nil {
		t.Fatal(err)
	}
	want := "--anonymous: ignoring the S3 credentials in the environment (AWS_ROLE_ARN, AWS_WEB_IDENTITY_TOKEN_FILE)"
	if len(warned) != 1 || warned[0] != want {
		t.Fatalf("warnings %q, want %q", warned, want)
	}
}
