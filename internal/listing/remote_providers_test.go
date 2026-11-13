// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	azsdk "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/api/option"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/objfile"
	"github.com/varveio/swath/objfile/azblob"
	"github.com/varveio/swath/objfile/azblob/azfake"
	"github.com/varveio/swath/objfile/gcs"
	"github.com/varveio/swath/objfile/gcs/gcsfake"
	"github.com/varveio/swath/objfile/s3"
	"github.com/varveio/swath/objfile/s3/s3fake"
)

// The remote dataset tests run once per destination provider, each
// through its in-process fake. A provider gives the harness a bucket fake
// (bucketFake: the objects, another writer's puts and deletes, request
// counts and faults) and a RemoteOutput writing to it through the
// provider's objfile writer, with small upload buffers so that a part
// takes several requests.

const dstBucket = "dst"

// Fault steps every fake names alike.
const (
	opCreate = "create" // the request that creates an object
	opRead   = "read"   // a ranged read
	opDelete = "delete" // a delete
)

// bucketFake is a provider's fake bucket as the tests use it.
type bucketFake interface {
	http.Handler
	// Object is name's live bytes.
	Object(name string) ([]byte, bool)
	// Put creates or replaces name, as another writer would; Remove
	// deletes it.
	Put(name string, data []byte)
	Remove(name string)
	// Names are the live objects, sorted.
	Names() []string
	// Count is the requests of a fault step answered.
	Count(op string) int64
	// OpenUploads is the uploads started and neither completed nor
	// aborted (GCS: resumable sessions, which expire; S3: multipart
	// uploads, which do not; Azure: names holding uncommitted blocks,
	// which the service discards after a week).
	OpenUploads() int
	// SetFault decides a request's fate before (after false) the fake
	// acts on it, or after (its answer lost): a status other than 0 is
	// sent instead.
	SetFault(f func(op, name string, after bool) int)
}

// provider is a destination provider under test.
type provider struct {
	name string
	// root is the URI of the destination bucket or container, without a
	// prefix.
	root string
	fake func() bucketFake
	// output is a RemoteOutput writing SCHEME://dst/prefix through the
	// fake served at url.
	output func(url, prefix string) *RemoteOutput
	// uploading tells a request that carries an upload's bytes.
	uploading func(r *http.Request) bool
	// aborts: an upload that ends without its object ends its session at
	// the store (S3's AbortMultipartUpload); GCS's SDK only cancels, and
	// Azure's staged blocks need no call.
	aborts bool
}

// providers are the destinations every remote test runs against.
var providers = []provider{gcsProvider, s3Provider, azureProvider}

// eachProvider runs f once per provider, as a subtest named for it.
func eachProvider(t *testing.T, f func(t *testing.T, p provider)) {
	for _, p := range providers {
		t.Run(p.name, func(t *testing.T) { f(t, p) })
	}
}

// destination is a provider's fake bucket for remote datasets.
type destination struct {
	bucketFake
	p   provider
	url string
}

// newDestination serves a fake of p's through wrap (nil: as is).
func newDestination(t *testing.T, p provider, wrap func(http.Handler) http.Handler) *destination {
	t.Helper()
	f := p.fake()
	var h http.Handler = f
	if wrap != nil {
		h = wrap(f)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return &destination{bucketFake: f, p: p, url: srv.URL}
}

// output is a RemoteOutput writing the provider's dst/prefix through d.
func (d *destination) output(prefix string) *RemoteOutput { return d.p.output(d.url, prefix) }

// gcs is the GCS fake under d (nil for another provider's), for the tests
// of GCS behaviour.
func (d *destination) gcs() *gcsfake.Server {
	f, _ := d.bucketFake.(gcsFake)
	return f.Server
}

// uri is the destination's URI of prefix.
func (d *destination) uri(prefix string) string { return d.p.root + "/" + prefix }

// gcsProvider writes through objfile/gcs to gcsfake, in chunks of 256 KiB
// (the SDK's smallest).
var gcsProvider = provider{name: "gcs", root: "gs://" + dstBucket,
	fake: func() bucketFake { return gcsFake{gcsfake.New(dstBucket)} },
	output: func(url, prefix string) *RemoteOutput {
		loc := "gs://" + dstBucket + "/" + prefix
		return &RemoteOutput{Location: loc, ChunkSize: 256 << 10, Open: func(ctx context.Context, stats *dataset.UploadStats) (*dataset.Remote, func() error, error) {
			c, err := storage.NewClient(ctx, option.WithEndpoint(url+"/storage/v1/"), option.WithoutAuthentication())
			if err != nil {
				return nil, nil, err
			}
			l, err := objfile.ParseLocation(loc)
			if err != nil {
				return nil, nil, err
			}
			w := gcs.NewWriter(c, dstBucket, gcs.WriteOptions{ChunkSize: 256 << 10, Retried: stats.Retried, MaxAttempts: 2, ChunkRetryDeadline: 2 * time.Second})
			return &dataset.Remote{Location: l, Opener: objfile.Root(gcs.New(c, dstBucket), l.Prefix), Writer: objfile.RootWriter(w, l.Prefix)}, c.Close, nil
		}}
	},
	uploading: func(r *http.Request) bool {
		return r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/upload/")
	},
}

type gcsFake struct{ *gcsfake.Server }

func (f gcsFake) Object(name string) ([]byte, bool) {
	o, ok := f.Get(name)
	return o.Data, ok
}

func (f gcsFake) Put(name string, data []byte) { f.Server.Put(name, data) }

func (f gcsFake) OpenUploads() int { return f.OpenSessions() }

func (f gcsFake) SetFault(fn func(op, name string, after bool) int) { f.Server.SetFault(fn) }

// s3Part is the S3 harness's part size, the fake's minimum lowered to it:
// a part of the listing's (about 6 KiB) is a multipart upload of two.
const s3Part = 4 << 10

// s3Provider writes through objfile/s3 to s3fake, path-style, in parts of
// the output's ChunkSize (s3Part unless a test sets another); the SDK's
// retries do not wait.
var s3Provider = provider{name: "s3", root: "s3://" + dstBucket,
	fake: func() bucketFake {
		f := s3fake.New(dstBucket)
		f.MinPartSize = s3Part
		return s3Fake{f}
	},
	output: func(url, prefix string) *RemoteOutput {
		loc := "s3://" + dstBucket + "/" + prefix
		r := &RemoteOutput{Location: loc, ChunkSize: s3Part}
		r.Open = func(ctx context.Context, stats *dataset.UploadStats) (*dataset.Remote, func() error, error) {
			c := awss3.New(awss3.Options{BaseEndpoint: aws.String(url), UsePathStyle: true, Region: "us-east-1", Credentials: aws.AnonymousCredentials{},
				Retryer: retry.NewStandard(func(o *retry.StandardOptions) {
					o.Backoff = retry.BackoffDelayerFunc(func(int, error) (time.Duration, error) { return time.Millisecond, nil })
					o.RateLimiter = ratelimit.None
				})})
			l, err := objfile.ParseLocation(loc)
			if err != nil {
				return nil, nil, err
			}
			w, err := s3.NewWriter(c, dstBucket, s3.WriteOptions{PartSize: int64(r.ChunkSize), MinPartSize: s3Part, MaxAttempts: 2, Retried: stats.Retried})
			if err != nil {
				return nil, nil, err
			}
			return &dataset.Remote{Location: l, Region: "us-east-1", Opener: objfile.Root(s3.New(c, dstBucket), l.Prefix), Writer: objfile.RootWriter(w, l.Prefix)},
				func() error { return nil }, nil
		}
		return r
	},
	uploading: func(r *http.Request) bool {
		return r.Method == http.MethodPut || r.Method == http.MethodPost
	},
	aborts: true,
}

type s3Fake struct{ *s3fake.Server }

func (f s3Fake) Object(name string) ([]byte, bool) {
	o, ok := f.Get(name)
	return o.Data, ok
}

func (f s3Fake) Put(name string, data []byte) { f.Server.Put(name, data) }

func (f s3Fake) OpenUploads() int { return len(f.Uploads()) }

func (f s3Fake) SetFault(fn func(op, name string, after bool) int) { f.Server.SetFault(fn) }

// dstAccount is the Azure harness's storage account; azBlock its block
// size: a part of the listing's (about 6 KiB) is two blocks staged and a
// block list.
const (
	dstAccount = "acct"
	azBlock    = 4 << 10
)

// azureProvider writes through objfile/azblob to azfake, addressed as an
// emulator is (/ACCOUNT/CONTAINER/NAME), in blocks of the output's
// ChunkSize; the client's retries do not wait.
var azureProvider = provider{name: "azure", root: "az://" + dstAccount + "/" + dstBucket,
	fake: func() bucketFake { return azFake{azfake.New(dstAccount, dstBucket)} },
	output: func(url, prefix string) *RemoteOutput {
		loc := "az://" + dstAccount + "/" + dstBucket + "/" + prefix
		r := &RemoteOutput{Location: loc, ChunkSize: azBlock}
		r.Open = func(ctx context.Context, stats *dataset.UploadStats) (*dataset.Remote, func() error, error) {
			c, err := azsdk.NewClientWithNoCredential(url+"/"+dstAccount+"/", &azsdk.ClientOptions{ClientOptions: azcore.ClientOptions{
				Retry: policy.RetryOptions{MaxRetries: 1, RetryDelay: time.Millisecond, MaxRetryDelay: time.Millisecond}}})
			if err != nil {
				return nil, nil, err
			}
			l, err := objfile.ParseLocation(loc)
			if err != nil {
				return nil, nil, err
			}
			w, err := azblob.NewWriter(c, dstBucket, azblob.WriteOptions{BlockSize: int64(r.ChunkSize), MaxAttempts: 2,
				RetryDelay: time.Millisecond, MaxRetryDelay: time.Millisecond, Retried: stats.Retried})
			if err != nil {
				return nil, nil, err
			}
			return &dataset.Remote{Location: l, Opener: objfile.Root(azblob.New(c, dstBucket), l.Prefix), Writer: objfile.RootWriter(w, l.Prefix)},
				func() error { return nil }, nil
		}
		return r
	},
	uploading: func(r *http.Request) bool { return r.Method == http.MethodPut },
}

type azFake struct{ *azfake.Server }

func (f azFake) Object(name string) ([]byte, bool) {
	b, ok := f.Get(name)
	return b.Data, ok
}

func (f azFake) Put(name string, data []byte) { f.Server.Put(name, data) }

func (f azFake) OpenUploads() int { return len(f.Staging()) }

func (f azFake) SetFault(fn func(op, name string, after bool) int) { f.Server.SetFault(fn) }
