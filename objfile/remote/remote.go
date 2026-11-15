// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package remote opens object-store roots by URI and owns their SDK clients.
// OpenWith provides reads, and OpenDestination reads and conditional
// writes, with the credentials and endpoint Options give; Open is OpenWith
// with the zero Options, the environment's and SDKs' defaults. Both scope
// object names to the location's prefix using objfile.Root, and are
// objfile.ObjectReaders; neither bounds the requests in flight (wrap one in
// objfile.Limit for that). The clients OpenWith builds send through a
// transport that reports every HTTP attempt, the SDKs' retries included, to
// the objfile.Observer of the request's context (ReportsIO).
//
// For anonymous access or a custom transport, construct the provider's SDK
// client and use objfile/s3, objfile/gcs or objfile/azblob directly. See the
// gcs package's range-validation requirements for caller-built clients.
package remote

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/auth/credentials"
	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/api/option"
	htransport "google.golang.org/api/transport/http"

	"github.com/varveio/swath/objfile"
	"github.com/varveio/swath/objfile/azblob"
	"github.com/varveio/swath/objfile/gcs"
	"github.com/varveio/swath/objfile/internal/ranged"
	"github.com/varveio/swath/objfile/s3"
)

// Store is a dataset root opened by OpenWith: an objfile.Opener of the
// names under the location's prefix, through clients it owns. It is safe
// for concurrent use. It does not bound the requests in flight; wrap it
// with objfile.Limit for that. Its connection pool keeps idle connections
// for 64 requests to its host (see OpenWith).
type Store struct {
	o      objfile.Opener
	scope  string
	close  func() error
	client any // the SDK client, recorded as observing (ranged.SetObserving)
}

// Open is OpenWith with the zero Options: the provider's default
// credentials and the environment's endpoint,
//
//   - s3: the AWS SDK's default configuration (environment, shared files,
//     container and instance roles), which also gives the region and an
//     endpoint set by AWS_ENDPOINT_URL_S3 or AWS_ENDPOINT_URL;
//   - gs: Application Default Credentials with the read-only scope, or none
//     when STORAGE_EMULATOR_HOST names an emulator;
//   - az: azidentity's DefaultAzureCredential, on the account's
//     blob.core.windows.net endpoint.
func Open(ctx context.Context, location string) (*Store, error) {
	return OpenWith(ctx, location, Options{})
}

// OpenWith returns the Store of location, a URI ParseLocation accepts,
// with the credentials, endpoint and request timeout o gives, the defaults
// (Open) for those it leaves unset. The Store reads: GCS credentials have
// the read-only scope, and o's write options are not used.
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
// OpenWith makes no object-store request; a missing bucket or insufficient
// permissions fail the first operation on the Store. Credential setup may
// fail during OpenWith. The Store outlives ctx: each request runs under
// the context of the open or read that makes it, a token exchange the request
// waits for included (a refresh the library starts early, before a token
// expires, runs in the background with its own context), so a Store opened
// with one request's context may serve later ones.
func OpenWith(ctx context.Context, location string, o Options) (*Store, error) {
	l, err := objfile.ParseLocation(location)
	if err != nil {
		return nil, err
	}
	pool := http.DefaultTransport.(*http.Transport).Clone()
	sizeTransport(pool, poolRequests)
	pool.ResponseHeaderTimeout = o.timeout()
	// Every attempt the SDKs make, their retries included, is observed
	// (objfile.Observer), below any check that may refuse a response.
	observed := ranged.Observe(pool)
	s := &Store{scope: l.String(), close: func() error { pool.CloseIdleConnections(); return nil }}
	var opener objfile.Opener
	switch l.Scheme {
	case "s3":
		// As the SDK's own client: a redirect is the answer.
		hc := &http.Client{Transport: observed, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		opts := []func(*config.LoadOptions) error{config.WithHTTPClient(hc)}
		if o.Profile != "" {
			opts = append(opts, config.WithSharedConfigProfile(o.Profile))
		}
		if o.Region != "" {
			opts = append(opts, config.WithRegion(o.Region))
		}
		cfg, err := config.LoadDefaultConfig(ctx, opts...)
		if err != nil {
			return nil, err
		}
		client := s3sdk.NewFromConfig(cfg, func(so *s3sdk.Options) {
			if o.Endpoint != "" {
				so.BaseEndpoint, so.UsePathStyle = aws.String(o.Endpoint), true
			}
		})
		if err := s.at(aws.ToString(client.Options().BaseEndpoint)); err != nil {
			return nil, err
		}
		s.observing(client)
		opener = s3.New(client, l.Container)
	case "gs":
		if o.Endpoint == "" && !o.Emulator {
			o.Endpoint, o.Emulator = ResolveEndpoint("")
		}
		if err := s.at(o.Endpoint); err != nil {
			return nil, err
		}
		client, err := gcsClient(ctx, observed, o, storage.ScopeReadOnly)
		if err != nil {
			return nil, err
		}
		s.observing(client)
		opener = gcs.New(client, l.Container)
		s.close = func() error { err := client.Close(); pool.CloseIdleConnections(); return err }
	case "az":
		if err := s.at(o.Endpoint); err != nil {
			return nil, err
		}
		client, err := azureClient(l.Account, o, observed)
		if err != nil {
			return nil, err
		}
		s.observing(client)
		opener = azblob.New(client, l.Container)
	}
	s.o = objfile.Root(opener, l.Prefix)
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
		return errors.New("remote: the endpoint is not a URL without user information, query or fragment")
	}
	s.scope += " at " + endpoint
	return nil
}

// gcsClient returns a GCS client over pool for o, whose transport checks
// every ranged response (ranged.Transport): with o.Emulator, the emulator's
// client, without credentials; else one with o's credentials and scope
// (gcsCredentials). The SDK's logger is silenced: its debug output,
// switched on by the environment, writes request headers unredacted, the
// bearer token included.
func gcsClient(ctx context.Context, pool http.RoundTripper, o Options, scope string) (*storage.Client, error) {
	tr := ranged.Transport(pool)
	if !o.Emulator {
		creds, err := gcsCredentials(o, scope)
		if err != nil {
			return nil, err
		}
		if tr, err = htransport.NewTransport(ctx, tr, option.WithAuthCredentials(creds), option.WithLogger(quietLog)); err != nil {
			return nil, err
		}
	}
	opts := []option.ClientOption{option.WithLogger(quietLog), option.WithHTTPClient(&http.Client{Transport: tr})}
	if ep := o.Endpoint; ep != "" {
		if o.Emulator {
			// The emulator's JSON API base, as the SDK makes it from
			// STORAGE_EMULATOR_HOST, given explicitly: the environment
			// is not read again.
			if !strings.Contains(ep, "://") {
				ep = "http://" + ep
			}
			ep = strings.TrimSuffix(ep, "/") + "/storage/v1/"
		} else if ep[len(ep)-1] != '/' {
			ep += "/"
		}
		opts = append(opts, option.WithEndpoint(ep))
	}
	return storage.NewClient(ctx, opts...)
}

// gcsCredentials loads o's credentials with scope: the file o names (a
// service account key or authorized-user credentials, refused otherwise),
// else Application Default Credentials. They are the new auth library's
// (cloud.google.com/go/auth), which fetches each token with the context of
// the request that needs it, so a Store opened with a request's context
// refreshes its token after that request ends. Their token exchanges go
// through a client of their own, bounded by the request timeout (headers
// and body); their debug logging is off whatever the environment says (it
// would print tokens and signed assertions).
func gcsCredentials(o Options, scope string) (*auth.Credentials, error) {
	dopts := &credentials.DetectOptions{Scopes: []string{scope}, Logger: quietLog,
		Client: &http.Client{Timeout: o.timeout(), Transport: http.DefaultTransport.(*http.Transport).Clone()}}
	if o.CredentialsFile == "" {
		creds, err := credentials.DetectDefault(dopts)
		if err != nil {
			return nil, fmt.Errorf("remote: credentials: %w", err)
		}
		return creds, nil
	}
	b, err := os.ReadFile(o.CredentialsFile)
	if err != nil {
		return nil, fmt.Errorf("remote: credentials: %w", err)
	}
	var kind struct {
		Type credentials.CredType `json:"type"`
	}
	if err := json.Unmarshal(b, &kind); err != nil || kind.Type != credentials.ServiceAccount && kind.Type != credentials.AuthorizedUser {
		return nil, fmt.Errorf("remote: credentials %s: want a service account key or authorized-user credentials", o.CredentialsFile)
	}
	creds, err := credentials.NewCredentialsFromJSON(kind.Type, b, dopts)
	if err != nil {
		return nil, fmt.Errorf("remote: credentials %s: %w", o.CredentialsFile, err)
	}
	return creds, nil
}

// quietLog is the GCS clients' and credentials' logger: it emits nothing.
// The SDK's own, switched on by GOOGLE_SDK_GO_LOGGING_LEVEL, writes request
// headers and token exchanges unredacted.
var quietLog = slog.New(slog.DiscardHandler)

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
// endpoint when one is set (Options.Endpoint, or the environment's). It
// holds no credentials.
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
