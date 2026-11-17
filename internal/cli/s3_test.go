// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/spf13/pflag"

	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/registry"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/s3"
	"github.com/varveio/swath/internal/source/s3/s3test"
)

// openS3Args opens s3://bkt with the command-line flags args as the
// command does, telling warn the warnings (nil: none kept).
func openS3Args(t *testing.T, warn func(code, msg string), args ...string) (*s3Source[s3.Object], error) {
	t.Helper()
	src, err := openArgs(t, "s3://bkt", false, warn, args...)
	if err != nil {
		return nil, err
	}
	return src.(*s3Source[s3.Object]), nil
}

// clearEndpointEnv unsets the AWS endpoint variables the host may hold.
func clearEndpointEnv(t *testing.T) {
	for _, v := range []string{"AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL", "AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "AWS_USE_FIPS_ENDPOINT", "AWS_USE_DUALSTACK_ENDPOINT"} {
		t.Setenv(v, "")
	}
}

// countOnce lists src once, unordered into no sink, as a dry run does.
func countOnce(t *testing.T, src registry.Source) order.Stats {
	t.Helper()
	st, _, err := src.Run(context.Background(), registry.Env{Count: true, Order: order.Options{Workers: 1}})
	if err != nil {
		t.Fatal(err)
	}
	return st
}

// TestS3EndpointFromEnvironment: AWS_ENDPOINT_URL_S3, else AWS_ENDPOINT_URL,
// is --endpoint-url when the flag is not given: the requests go to it
// (path-style), the identity records it and no tariff is claimed;
// AWS_IGNORE_CONFIGURED_ENDPOINT_URLS turns both off, and the flag wins.
func TestS3EndpointFromEnvironment(t *testing.T) {
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

	endpoint := func(o *s3Source[s3.Object]) string { return o.Identity().Fields[1].Value }
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
			o, err := openS3Args(t, nil, append(c.args, "--anonymous")...)
			if err != nil {
				t.Fatal(err)
			}
			if got := endpoint(o); got != c.want {
				t.Fatalf("identity endpoint %q, want %q", got, c.want)
			}
			pathStyle := identityField(o.Identity(), "path style") == "true"
			if c.want == "" {
				if usd, _ := o.Tariff(); usd == 0 || pathStyle {
					t.Fatalf("AWS's own endpoint: tariff %v, path style %v", usd, pathStyle)
				}
				return
			}
			if usd, _ := o.Tariff(); usd != 0 || !pathStyle {
				t.Fatalf("a custom endpoint: tariff %v, path style %v", usd, pathStyle)
			}
			before := hits.Load()
			countOnce(t, o)
			if hits.Load() == before || path.Load() != "/bkt" {
				t.Fatalf("%d requests reached the listener, the last for %v", hits.Load()-before, path.Load())
			}
		})
	}
}

// TestS3FIPSAndDualStackFromEnvironment: AWS_USE_FIPS_ENDPOINT and
// AWS_USE_DUALSTACK_ENDPOINT are --s3-fips and --s3-dualstack, which an
// explicit flag overrides.
func TestS3FIPSAndDualStackFromEnvironment(t *testing.T) {
	s3test.NoCredentials(t)
	clearEndpointEnv(t)
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_USE_FIPS_ENDPOINT", "TRUE")
	t.Setenv("AWS_USE_DUALSTACK_ENDPOINT", "true")
	o, err := openS3Args(t, nil, "--anonymous")
	if err != nil {
		t.Fatal(err)
	}
	if fips, dual := identityField(o.Identity(), "fips"), identityField(o.Identity(), "dual-stack"); fips != "true" || dual != "true" {
		t.Fatalf("fips %v, dual-stack %v from the environment", fips, dual)
	}
	o, err = openS3Args(t, nil, "--anonymous", "--s3-fips=false", "--s3-dualstack=false")
	if err != nil {
		t.Fatal(err)
	}
	if fips, dual := identityField(o.Identity(), "fips"), identityField(o.Identity(), "dual-stack"); fips != "false" || dual != "false" {
		t.Fatalf("the flags did not win: fips %v, dual-stack %v", fips, dual)
	}
}

// TestS3NoEffectRefused: FIPS or dual-stack with a custom endpoint selects
// nothing, so the pair is refused (exit 2), naming where each came from.
func TestS3NoEffectRefused(t *testing.T) {
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
			o, err := openS3Args(t, nil, append(c.args, "--anonymous")...)
			if o != nil || !errors.Is(err, source.ErrUnsupported) || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("opened %v, error %v", o, err)
			}
		})
	}
}

// TestS3AnonymousCredentialConflicts: --anonymous beside an explicit S3
// credential is refused (exit 2), as for every provider.
func TestS3AnonymousCredentialConflicts(t *testing.T) {
	s3test.NoCredentials(t)
	clearEndpointEnv(t)
	for _, flag := range []string{"--s3-profile=work", "--s3-bearer-token-command=true"} {
		name, _, _ := strings.Cut(flag, "=")
		o, err := openS3Args(t, nil, "--anonymous", flag)
		if o != nil || !errors.Is(err, source.ErrUnsupported) || !strings.Contains(err.Error(), "--anonymous excludes "+name) {
			t.Fatalf("%s: opened %v, error %v", flag, o, err)
		}
	}
}

// TestS3AnonymousNote: the warning names the AWS credentials --anonymous
// ignores, among them those the old note left out.
func TestS3AnonymousNote(t *testing.T) {
	s3test.NoCredentials(t)
	clearEndpointEnv(t)
	t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::1:role/r")
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", "/nonexistent")
	var warned []string
	if _, err := openS3Args(t, func(_, s string) { warned = append(warned, s) }, "--anonymous", "--endpoint-url=http://store.example"); err != nil {
		t.Fatal(err)
	}
	want := "--anonymous: ignoring the S3 credentials in the environment (AWS_ROLE_ARN, AWS_WEB_IDENTITY_TOKEN_FILE)"
	if len(warned) != 1 || warned[0] != want {
		t.Fatalf("warnings %q, want %q", warned, want)
	}
}

// TestS3EndpointURLDefaultsPathStyle: a custom endpoint addresses buckets
// path-style unless --no-s3-path-style, as Java's --force-path-style
// default; the flag's outcome is what the identity records, and what the
// requests are sent with.
func TestS3EndpointURLDefaultsPathStyle(t *testing.T) {
	s3test.NoCredentials(t)
	t.Setenv("AWS_REGION", "us-east-1")
	var path, host atomic.Value
	srv := s3test.Serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path.Store(r.URL.Path)
		host.Store(r.Host)
		w.Write([]byte(`<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`))
	}))
	opened := func(args ...string) *s3Source[s3.Object] {
		t.Helper()
		o, err := openS3Args(t, nil, append(args, "--anonymous")...)
		if err != nil {
			t.Fatal(err)
		}
		return o
	}
	pathStyle := func(o *s3Source[s3.Object]) bool { return identityField(o.Identity(), "path style") == "true" }
	if o := opened("--endpoint-url", "http://store.example:9000"); !pathStyle(o) {
		t.Fatal("a custom endpoint is not path-style by default")
	}
	if o := opened("--endpoint-url", "http://store.example:9000", "--no-s3-path-style"); pathStyle(o) {
		t.Fatal("--no-s3-path-style is not virtual-host addressing")
	}
	if o := opened(); pathStyle(o) {
		t.Fatal("AWS's endpoint is path-style by default")
	}
	// The default reaches the requests: the bucket in the path.
	countOnce(t, opened("--endpoint-url", srv))
	if path.Load() != "/bkt" || host.Load() != srv[len("http://"):] {
		t.Fatalf("request to %v%v", host.Load(), path.Load())
	}
}

func TestS3PathStyleFlag(t *testing.T) {
	for _, c := range []struct {
		args            []string
		value, set, bad bool
	}{
		{nil, false, false, false},
		{[]string{"--s3-path-style"}, true, true, false},
		{[]string{"--no-s3-path-style"}, false, true, false},
		{[]string{"--s3-path-style=true", "--no-s3-path-style"}, false, true, false},
		{[]string{"--no-s3-path-style", "--s3-path-style"}, true, true, false},
		{[]string{"--s3-path-style=invalid"}, false, false, true},
		{[]string{"--s3-path-style", "--s3-path-style=invalid"}, true, true, true},
	} {
		fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
		fs.SetOutput(io.Discard)
		opt := factory(t, "s3://bkt").RegisterFlags(fs, false).(*s3Flagged)
		err := fs.Parse(c.args)
		if (err != nil) != c.bad || opt.PathStyle != c.value || opt.pathStyleSet != c.set {
			t.Errorf("%q: value=%v set=%v err=%v", c.args, opt.PathStyle, opt.pathStyleSet, err)
		}
		// Both spellings are switches: the help shows neither =true nor
		// =false.
		for _, n := range []string{"s3-path-style", "no-s3-path-style"} {
			if fl := fs.Lookup(n); fl == nil || fl.NoOptDefVal != "true" || Described(fl).Metavar != "" {
				t.Fatalf("--%s is not a switch", n)
			}
		}
	}
}

func TestS3BearerFlagConflicts(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"--s3-bearer-token-refresh-interval=1m"}, "--s3-bearer-token-refresh-interval requires --s3-bearer-token-command"},
		{[]string{"--s3-bearer-token-command=unused", "--s3-profile=work"}, "--s3-bearer-token-command excludes --s3-profile"},
	} {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			opened, err := openS3Args(t, nil, tc.args...)
			if opened != nil || !errors.Is(err, source.ErrUnsupported) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("opened %v, error %v", opened, err)
			}
		})
	}
}

// TestS3EngineFlagsShared: S3 takes the store's and the range engine's
// flag groups the command registered once and passed to it, and opens
// with the parsed values (the engine takes its policy from the options:
// the provider's TestPolicyReachesEngine).
func TestS3EngineFlagsShared(t *testing.T) {
	s3test.NoCredentials(t)
	f := factory(t, "s3://bkt")
	fs := pflag.NewFlagSet("t", pflag.ContinueOnError)
	v := f.RegisterFlags(fs, true)
	if err := fs.Parse([]string{"--slow-split", "--anonymous", "--endpoint-url", "http://127.0.0.1:1", "--request-timeout", "7s"}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Open(context.Background(), registry.Request{Root: "s3://bkt", Options: v, Workers: 2}); err != nil {
		t.Fatal(err)
	}
	opt, err := v.(*s3Flagged).options()
	if err != nil || !opt.Seed || !opt.SlowSplit || !opt.Anonymous || opt.Endpoint != "http://127.0.0.1:1" || opt.Timeout.Seconds() != 7 {
		t.Fatalf("shared flags not applied: %+v (%v)", opt, err)
	}
	// Defaults (no flags parsed) hold the engine's defaults. Anonymous is set
	// so the client builds without credentials (none are configured here).
	o := s3Defaults().(*s3Flagged)
	o.store = &source.Store{Anonymous: true}
	if _, err := f.Open(context.Background(), registry.Request{Root: "s3://bkt", Options: o, Workers: 2}); err != nil {
		t.Fatalf("defaults: %v", err)
	}
	if opt, err := o.options(); err != nil || opt.Seed {
		t.Fatalf("defaults: %+v (%v)", opt, err)
	}
}
