// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"reflect"
	"testing"

	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/source/s3/client"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
	"github.com/varveio/swath/go/internal/source/s3/sdkclient"
)

// TestIdentitySameForBothClients: the run identity a journal records
// (Identity: source, route, region, owner columns) is the same, field for
// field, whether the hand-rolled or the SDK-backed client is built, for
// every endpoint form, so a journal written through one resumes through
// the other. The FIPS region aliases are the exception, recorded here: the
// SDK resolves them to their region with FIPS on, the hand-rolled client
// takes them for region names.
func TestIdentitySameForBothClients(t *testing.T) {
	s3test.NoCredentials(t)
	for _, c := range []struct {
		name    string
		opt     Options
		differs bool
	}{
		{"default endpoint", Options{Region: "eu-west-1"}, false},
		{"dotted bucket", Options{Bucket: "a.b", Region: "us-east-1"}, false},
		{"path-style", Options{Region: "us-east-1", PathStyle: true}, false},
		{"custom IP endpoint", Options{Endpoint: "http://127.0.0.1:9000"}, false},
		{"custom IPv6 endpoint", Options{Endpoint: "http://[::1]:9000"}, false},
		{"custom endpoint with a path", Options{Endpoint: "https://minio.example/s3/"}, false},
		{"custom endpoint with a path, path-style", Options{Endpoint: "https://minio.example/s3/", PathStyle: true}, false},
		{"custom endpoint with endpoint flags", Options{Endpoint: "https://minio.example/", PathStyle: true, FIPS: true, DualStack: true}, false},
		{"FIPS", Options{Region: "us-gov-west-1", FIPS: true}, false},
		{"dual-stack", Options{Region: "eu-west-1", DualStack: true}, false},
		{"cn-north-1", Options{Region: "cn-north-1"}, false},
		{"region from the environment", Options{}, false},
		{"FIPS alias fips-us-east-1", Options{Region: "fips-us-east-1"}, true},
		{"FIPS alias us-east-1-fips", Options{Region: "us-east-1-fips"}, true},
	} {
		t.Setenv("AWS_REGION", "ap-southeast-2")
		opt := c.opt
		if opt.Bucket == "" {
			opt.Bucket = "bkt"
		}
		opt.Prefix, opt.Anonymous, opt.FetchOwner = "p/", true, true
		cc := client.Config{Region: opt.Region, Endpoint: opt.Endpoint, PathStyle: opt.PathStyle, FIPS: opt.FIPS, DualStack: opt.DualStack,
			Profile: opt.Profile, Anonymous: opt.Anonymous, RequestTimeout: opt.Timeout, RequestPayer: opt.RequestPayer} // as connect
		hand, err := client.New(context.Background(), cc)
		if err != nil {
			t.Fatal(err)
		}
		sdk, err := sdkclient.New(context.Background(), cc)
		if err != nil {
			t.Fatal(err)
		}
		h, s := identityWith(hand, opt), identityWith(sdk, opt)
		if reflect.DeepEqual(h, s) == c.differs {
			t.Errorf("%s (differs %v):\nhand-rolled %+v\n        sdk %+v", c.name, c.differs, h, s)
		}
	}
}

// identityWith is the identity of a listing of opt through c, as connect
// builds it.
func identityWith(c built, opt Options) source.Identity {
	o := &opened[Object]{p: &provider[Object]{opt: opt, client: c, route: c.Route(opt.Bucket), region: c.Region()}}
	return o.Identity()
}
