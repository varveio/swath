// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package remote opens object-store roots by URI and owns their SDK clients.
// Open provides reads with default credentials; OpenDestination provides
// reads and conditional writes with DestinationOptions. Both scope object
// names to the location's prefix using objfile.Root, and are
// objfile.ObjectReaders; neither bounds the requests in flight (wrap one in
// objfile.Limit for that). The clients Open builds send through a transport that
// reports every HTTP attempt, the SDKs' retries included, to the
// objfile.Observer of the request's context (ReportsIO).
//
// For anonymous access or a custom transport, construct the provider's SDK
// client and use objfile/s3, objfile/gcs or objfile/azblob directly. See the
// gcs package's range-validation requirements for caller-built clients.
package remote

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	azsdk "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"
	htransport "google.golang.org/api/transport/http"

	"github.com/varveio/swath/go/objfile"
	"github.com/varveio/swath/go/objfile/azblob"
	"github.com/varveio/swath/go/objfile/gcs"
	"github.com/varveio/swath/go/objfile/internal/ranged"
	"github.com/varveio/swath/go/objfile/s3"
)

// Store is a dataset root opened by Open: an objfile.Opener of the names
// under the location's prefix, through clients it owns. It is safe for
// concurrent use. It does not bound the requests in flight; wrap it with
// objfile.Limit for that. Its connection pool keeps idle connections for
// 64 requests to its host (see Open).
type Store struct {
	o      objfile.Opener
	scope  string
	close  func() error
	client any // the SDK client, recorded as observing (ranged.SetObserving)
}

// Open returns the Store of location, a URI ParseLocation accepts, with the
// provider's default credentials:
//
//   - s3: the AWS SDK's default configuration (environment, shared files,
//     container and instance roles), which also gives the region and an
//     endpoint set by AWS_ENDPOINT_URL_S3 or AWS_ENDPOINT_URL;
//   - gs: Application Default Credentials with the read-only scope, or none
//     when STORAGE_EMULATOR_HOST names an emulator;
//   - az: azidentity's DefaultAzureCredential, on the account's
//     blob.core.windows.net endpoint.
//
// The Store's connection pool keeps up to 64 idle connections to its host,
// so that the requests of four concurrent batches under
// objfile.DefaultBatchPolicy (16 each), or of an objfile.Limit of up to 64,
// find their connections open after the first burst; it does not bound the
// active connections. A client built by the caller keeps its own pool:
// size its idle connections per host to the requests it may have in
// flight (a Limit's n, or else BatchPolicy.MaxConcurrent times the batches
// run at once).
//
// Open makes no object-store request; a missing bucket or insufficient
// permissions fail the first operation on the Store. Credential setup may
// fail during Open. The Store outlives ctx: each request runs under
// the context of the Open or read that makes it, a token exchange the request
// waits for included (a refresh the library starts early, before a token
// expires, runs in the background with its own context), so a Store opened
// with one request's context may serve later ones.
func Open(ctx context.Context, location string) (*Store, error) {
	l, err := objfile.ParseLocation(location)
	if err != nil {
		return nil, err
	}
	pool := http.DefaultTransport.(*http.Transport).Clone()
	sizeTransport(pool, poolRequests)
	// Every attempt the SDKs make, their retries included, is observed
	// (objfile.Observer), below any check that may refuse a response.
	observed := ranged.Observe(pool)
	s := &Store{scope: l.String(), close: func() error { pool.CloseIdleConnections(); return nil }}
	var o objfile.Opener
	switch l.Scheme {
	case "s3":
		// As the SDK's own client: a redirect is the answer.
		hc := &http.Client{Transport: observed, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		cfg, err := config.LoadDefaultConfig(ctx, config.WithHTTPClient(hc))
		if err != nil {
			return nil, err
		}
		client := s3sdk.NewFromConfig(cfg)
		if err := s.at(aws.ToString(client.Options().BaseEndpoint)); err != nil {
			return nil, err
		}
		s.observing(client)
		o = s3.New(client, l.Container)
	case "gs":
		host := os.Getenv("STORAGE_EMULATOR_HOST")
		if err := s.at(host); err != nil {
			return nil, err
		}
		client, err := gcsClient(ctx, observed, host != "")
		if err != nil {
			return nil, err
		}
		s.observing(client)
		o = gcs.New(client, l.Container)
		s.close = func() error { err := client.Close(); pool.CloseIdleConnections(); return err }
	case "az":
		client, err := azureClient(l.Account, &http.Client{Transport: observed})
		if err != nil {
			return nil, err
		}
		s.observing(client)
		o = azblob.New(client, l.Container)
	}
	s.o = objfile.Root(o, l.Prefix)
	return s, nil
}

// poolRequests is how many requests to one host the connection pool of a
// Store keeps connections for between bursts: the idle connections it
// keeps per host (and in all) are at least this many. It is
// objfile.DefaultBatchPolicy's MaxConcurrent requests for each of four
// concurrent batches, and the bound for objfile.Limit a Store is wrapped
// in. The rule: keep idle connections for the requests a Store may have
// in flight to its host, a Limit's n or else MaxConcurrent times the
// batches run at once, so that a burst after the first finds its
// connections open. A Store's active connections are not bounded.
const poolRequests = 64

// sizeTransport raises idle connection capacity on an unused, exclusively
// owned transport, for requests in flight to one host: MaxIdleConnsPerHost
// and MaxIdleConns at least requests. It keeps active connection limits
// (MaxConnsPerHost), which may be intentional. Call before the transport's
// first use.
func sizeTransport(t *http.Transport, requests int) {
	t.MaxIdleConnsPerHost = max(t.MaxIdleConnsPerHost, requests)
	if t.MaxIdleConns != 0 { // 0 is no limit
		t.MaxIdleConns = max(t.MaxIdleConns, requests)
	}
}

// observing records client, whose transport is the Store's observing one,
// for the store package that wraps it (ReportsIO), until Close.
func (s *Store) observing(client any) {
	s.client = client
	ranged.SetObserving(client, true)
}

// at adds a custom endpoint to the Store's scope. One that carries user
// information, a query or a fragment, where credentials go, is refused, and
// is quoted in no error.
func (s *Store) at(endpoint string) error {
	if endpoint == "" {
		return nil
	}
	raw := endpoint
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw // STORAGE_EMULATOR_HOST may be a bare host, as the SDK takes it
	}
	u, err := url.Parse(raw)
	// A path or host whose escapes decode to a query, a fragment, a percent
	// sign or a control character is refused too: the S3 resolver decodes the
	// path, appends the bucket and parses again, quoting the result.
	if err != nil || u.Host == "" || u.User != nil || strings.ContainsAny(endpoint, "?#") ||
		strings.ContainsFunc(u.Path, func(r rune) bool { return r == '?' || r == '#' || r == '%' || r < 0x20 || r == 0x7f }) ||
		strings.ContainsFunc(u.Host, func(r rune) bool { return r <= 0x20 || r == 0x7f }) {
		return errors.New("remote: the endpoint the environment sets is not a URL without user information, query or fragment; build the client with its own settings instead")
	}
	s.scope += " at " + endpoint
	return nil
}

// gcsClient returns a read-only GCS client over pool whose transport checks
// every ranged response (ranged.Transport); with emulator set, the SDK's
// emulator client, without credentials.
//
// The credentials are the new auth library's (cloud.google.com/go/auth),
// which fetches each token with the context of the request that needs it.
// The older path, with user credentials, keeps the context the transport was
// built with for every refresh, so a Store opened with a request's context
// would fail to refresh its token once that request ended.
func gcsClient(ctx context.Context, pool http.RoundTripper, emulator bool) (*storage.Client, error) {
	tr := ranged.Transport(pool)
	if !emulator {
		var err error
		if tr, err = htransport.NewTransport(ctx, tr, option.WithScopes(storage.ScopeReadOnly), internaloption.EnableNewAuthLibrary(), option.WithLogger(quietLog)); err != nil {
			return nil, err
		}
	}
	return storage.NewClient(ctx, append([]option.ClientOption{option.WithHTTPClient(&http.Client{Transport: tr}), option.WithLogger(quietLog)}, gcsEndpoint...)...)
}

// quietLog is the GCS clients' logger: it emits nothing. The SDK's own,
// switched on by GOOGLE_SDK_GO_LOGGING_LEVEL, writes request headers and
// token exchanges unredacted.
var quietLog = slog.New(slog.DiscardHandler)

// gcsEndpoint is the GCS client's endpoint option; a test sets it.
var gcsEndpoint []option.ClientOption

// azureClient returns the client of account over hc; a test replaces it.
var azureClient = func(account string, hc *http.Client) (*azsdk.Client, error) {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, err
	}
	return azsdk.NewClient("https://"+account+".blob.core.windows.net/", cred,
		&azsdk.ClientOptions{ClientOptions: azcore.ClientOptions{Transport: hc}})
}

// Open opens name, a path objfile.CheckPath accepts, under the location's
// prefix.
func (s *Store) Open(ctx context.Context, name string) (objfile.Object, error) {
	return s.o.Open(ctx, name)
}

// OpenSized opens name, of size bytes, under the location's prefix.
func (s *Store) OpenSized(ctx context.Context, name string, size int64) (objfile.Object, error) {
	return s.o.OpenSized(ctx, name, size)
}

// ReadObject reads name, a path objfile.CheckPath accepts, under the
// location's prefix, whole in one request (objfile.ObjectReader).
func (s *Store) ReadObject(ctx context.Context, name string, limit int64) ([]byte, string, error) {
	return objfile.ReadObject(ctx, s.o, name, limit)
}

// Scope names the Store's storage namespace for objfile.Pin and
// objfile.Restore: the location's canonical URI, followed by " at " and the
// endpoint when one is set by the environment. It holds no credentials.
func (s *Store) Scope() string { return s.scope }

var _ interface {
	objfile.Opener
	objfile.ObjectReader
	io.Closer
	objfile.IOReporter
} = (*Store)(nil)

// Close releases the clients the Store owns and closes their idle
// connections. Read the Objects it opened before Close.
func (s *Store) Close() error {
	if s.client != nil {
		ranged.SetObserving(s.client, false)
	}
	return s.close()
}

// ReportsIO reports true (objfile.IOReporter): the Store's transport
// observes every attempt its SDK client makes, retries and reopened bodies
// included.
func (s *Store) ReportsIO() bool {
	r, ok := s.o.(objfile.IOReporter)
	return ok && r.ReportsIO()
}
