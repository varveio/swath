// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package source_test

import (
	"context"
	"slices"
	"testing"

	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/source/azblob"
	"github.com/varveio/swath/go/internal/source/azblob/azblobtest"
	"github.com/varveio/swath/go/internal/source/gcs"
	"github.com/varveio/swath/go/internal/source/gcs/gcstest"
	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/s3"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
)

// routedS3 is a supplied S3 client that names its route (s3.Routed);
// unroutedS3 and suppliedGCS are supplied clients that count nothing.
// None is asked anything here.
type (
	routedS3    struct{ s3.API }
	unroutedS3  struct{ s3.API }
	suppliedGCS struct{ gcs.API }
)

func (routedS3) Route(bucket string) string { return "http://store.example/" + bucket }

// TestRegisteredSources opens each object-store provider as the command
// does, through registry.Lookup(root).Open, and checks the registered
// source: every optional interface the pipeline probes for is present
// (an absent one silently turns its capability off), the capabilities are
// the provider's (sequential, capped and supplied-client runs included),
// and a supplied client that counts nothing reports zero activity.
func TestRegisteredSources(t *testing.T) {
	s3test.NoCredentials(t)
	t.Setenv("AWS_REGION", "us-east-1")
	s3ep := s3test.Bucket(t, s3test.Keys(20, 3))
	gcsep := gcstest.Bucket(t, gcstest.Keys(20), 0)
	az := azblobtest.Serve(t, azblobtest.Keys()[:20])
	azRoot := "az://" + az.Account + "/" + az.Container
	azOptions := func(o azblob.Options) *azblob.Options {
		o.Endpoint, o.Anonymous = az.Endpoint[:len(az.Endpoint)-len("/"+az.Account)], true
		return &o
	}
	parallel := source.Capabilities{Resume: true, Seek: source.SeekFabricable, Order: source.OrderBytes}
	sequential := source.Capabilities{Resume: true, Seek: source.SeekTokenOnly, Order: source.OrderBytes}
	noResume := func(c source.Capabilities) source.Capabilities { c.Resume = false; return c }
	for _, c := range []struct {
		name    string
		root    string
		options any
		caps    source.Capabilities
		priced  bool
		counted bool // the built client: its activity is live
	}{
		{"s3", "s3://bkt", &s3.Options{Endpoint: s3ep, Anonymous: true}, parallel, true, true},
		{"s3 versions", "s3://bkt", &s3.Options{Endpoint: s3ep, Anonymous: true, AllVersions: true}, parallel, true, true},
		{"s3 sequential", "s3://bkt", &s3.Options{Endpoint: s3ep, Anonymous: true, Policy: keyrange.Policy{Sequential: true}}, sequential, true, true},
		{"s3 capped", "s3://bkt", &s3.Options{Endpoint: s3ep, Anonymous: true, Policy: keyrange.Policy{Sequential: true, MaxKeys: 5}}, noResume(sequential), true, true},
		{"s3 routed client", "s3://bkt", &s3.Options{Client: routedS3{}}, parallel, true, false},
		{"s3 unrouted client", "s3://bkt", &s3.Options{Client: unroutedS3{}}, noResume(parallel), true, false},
		{"gcs", "gs://bkt", &gcs.Options{Endpoint: gcsep, Anonymous: true}, parallel, true, true},
		{"gcs generations", "gs://bkt", &gcs.Options{Endpoint: gcsep, Anonymous: true, AllVersions: true}, parallel, true, true},
		{"gcs sequential", "gs://bkt", &gcs.Options{Endpoint: gcsep, Anonymous: true, Policy: keyrange.Policy{Sequential: true}}, sequential, true, true},
		{"gcs capped", "gs://bkt", &gcs.Options{Endpoint: gcsep, Anonymous: true, Policy: keyrange.Policy{Sequential: true, MaxKeys: 5}}, noResume(sequential), true, true},
		{"gcs supplied client", "gs://bkt", &gcs.Options{Client: suppliedGCS{}}, noResume(parallel), true, false},
		{"azblob", azRoot, azOptions(azblob.Options{}), parallel, false, true},
		{"azblob sequential", azRoot, azOptions(azblob.Options{Policy: keyrange.Policy{Sequential: true}}), sequential, false, true},
		{"azblob capped", azRoot, azOptions(azblob.Options{Policy: keyrange.Policy{Sequential: true, MaxKeys: 5}}), noResume(sequential), false, true},
		{"azblob supplied client", azRoot, &azblob.Options{Client: azblobtest.NewStore(nil)}, noResume(parallel), false, false},
	} {
		f, err := registry.Lookup(c.root)
		if err != nil {
			t.Fatal(err)
		}
		src, err := f.Open(context.Background(), registry.Request{Root: c.root, Options: c.options, Workers: 2, Warn: func(string) {}})
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		missing := func(name string, ok bool) {
			if !ok {
				t.Errorf("%s: the registered source is not %s", c.name, name)
			}
		}
		_, ok := src.(source.Identified)
		missing("Identified", ok)
		_, ok = src.(source.Reporter)
		missing("Reporter", ok)
		_, ok = src.(source.Live)
		missing("Live", ok)
		_, ok = src.(source.Advancing)
		missing("Advancing", ok)
		_, ok = src.(source.SeedTimed)
		missing("SeedTimed", ok)
		_, ok = src.(source.Contended)
		missing("Contended", ok)
		_, ok = src.(source.Resumable)
		missing("Resumable", ok)
		_, ok = src.(source.Seeded)
		missing("Seeded", ok)
		_, ok = src.(source.Reusing)
		missing("Reusing", ok)
		_, ok = src.(source.Fencing)
		missing("Fencing", ok)
		if _, ok := src.(source.Priced); ok != c.priced {
			t.Errorf("%s: Priced %v, want %v", c.name, ok, c.priced)
		}
		if cp, ok := src.(source.Capable); !ok {
			t.Errorf("%s: the registered source is not Capable", c.name)
		} else if got := cp.Capabilities(); got != c.caps {
			t.Errorf("%s: capabilities %+v, want %+v", c.name, got, c.caps)
		}
		rq, okRq := src.(source.Requested)
		missing("Requested", okRq)
		fa, okFa := src.(source.Faulted)
		missing("Faulted", okFa)
		if !c.counted && okRq && okFa {
			th, to := fa.Faults()
			if rq.Requests() != (source.Requests{}) || th != 0 || to != 0 || src.(source.Live).Answered() != 0 {
				t.Errorf("%s: a supplied client that counts nothing reports activity", c.name)
			}
		}
	}
}

// TestRegisteredRestart: Restart through source.Resumable reaches the
// provider's, which updates the engine's options (the listing continues
// after the key), and the row count and client activity the registered
// source reports are the provider's.
func TestRegisteredRestart(t *testing.T) {
	s3test.NoCredentials(t)
	t.Setenv("AWS_REGION", "us-east-1")
	s3keys, gcsKeys, azKeys := s3test.Keys(40, 3), gcstest.Keys(40), azblobtest.Keys()[:40]
	az := azblobtest.Serve(t, azKeys)
	for _, c := range []struct {
		name    string
		root    string
		options any
		keys    []string
	}{
		{"s3", "s3://bkt", &s3.Options{Endpoint: s3test.Bucket(t, s3keys), Anonymous: true}, s3keys},
		{"gcs", "gs://bkt", &gcs.Options{Endpoint: gcstest.Bucket(t, gcsKeys, 0), Anonymous: true}, gcsKeys},
		{"azblob", "az://" + az.Account + "/" + az.Container,
			&azblob.Options{Endpoint: az.Endpoint[:len(az.Endpoint)-len("/"+az.Account)], Anonymous: true}, azKeys},
	} {
		f, err := registry.Lookup(c.root)
		if err != nil {
			t.Fatal(err)
		}
		src, err := f.Open(context.Background(), registry.Request{Root: c.root, Options: c.options, Workers: 2, Warn: func(string) {}})
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		keys := slices.Sorted(slices.Values(c.keys))
		after := keys[len(keys)/2]
		if err := src.(source.Resumable).Restart(after); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		st, _, err := src.Run(context.Background(), registry.Env{Count: true, Order: order.Options{Workers: 2}})
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if want := int64(len(keys) - len(keys)/2 - 1); st.Rows != want || src.Listed() != want {
			t.Errorf("%s: %d rows counted, %d listed after %q, want %d", c.name, st.Rows, src.Listed(), after, want)
		}
		if src.(source.Live).Answered() == 0 || src.(source.Requested).Requests().Attempts == 0 {
			t.Errorf("%s: the client's activity does not reach the registered source", c.name)
		}
	}
}
