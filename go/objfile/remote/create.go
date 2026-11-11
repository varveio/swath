// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package remote

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"
	htransport "google.golang.org/api/transport/http"

	"github.com/varveio/swath/go/objfile"
	"github.com/varveio/swath/go/objfile/gcs"
	"github.com/varveio/swath/go/objfile/internal/ranged"
)

// CreateOptions configure a Destination. Nothing in them is shared with
// any client the caller reads with: a destination's credentials are its
// own.
type CreateOptions struct {
	// CredentialsFile is a Google credentials JSON file (a service
	// account's key, or authorized-user credentials) read for the
	// destination; "" uses Application Default Credentials. Either way the
	// read-write scope is requested.
	CredentialsFile string
	// Endpoint is the JSON API base the client sends to
	// (https://storage.googleapis.com/storage/v1/ by default), as the SDK
	// takes it; "" is the default, or the emulator STORAGE_EMULATOR_HOST
	// names (then without credentials: ResolveEndpoint). It may not carry
	// user information, a query or a fragment.
	Endpoint string
	// Emulator says Endpoint is an emulator's host or URL, reached without
	// credentials (what ResolveEndpoint returns for STORAGE_EMULATOR_HOST).
	// A caller that freezes the route resolves it once and passes both, so
	// the environment is not read again.
	Emulator bool
	// RequestTimeout bounds the wait for a response's headers once a
	// request is sent, so a stalled request fails (and is retried) instead
	// of hanging (0: DefaultRequestTimeout).
	RequestTimeout time.Duration
	// Write configures the uploads (chunk size, retries, their account).
	Write gcs.WriteOptions
}

// Destination is a dataset root opened for writing by Create: a Writer and
// an Opener (for reading back what it wrote) of the names under the
// location's prefix, through one client it owns. It is safe for concurrent
// use.
type Destination struct {
	o     objfile.Opener
	w     objfile.Writer
	scope string
	close func() error
}

// Create returns the Destination of location, a URI ParseLocation accepts.
// Only gs:// is supported for writing; s3:// and az:// are refused. It makes
// no request: a missing bucket or a refused credential fails the first
// request. Credentials are loaded now (a file that cannot be read fails
// Create); the Destination outlives ctx.
func Create(ctx context.Context, location string, o CreateOptions) (*Destination, error) {
	l, err := objfile.ParseLocation(location)
	if err != nil {
		return nil, err
	}
	if l.Scheme != "gs" {
		return nil, fmt.Errorf("remote: writing to %s:// is not supported (gs:// only)", l.Scheme)
	}
	pool := http.DefaultTransport.(*http.Transport).Clone()
	pool.ResponseHeaderTimeout = o.RequestTimeout
	if pool.ResponseHeaderTimeout <= 0 {
		pool.ResponseHeaderTimeout = DefaultRequestTimeout
	}
	d := &Destination{scope: l.String()}
	if o.Endpoint == "" && !o.Emulator {
		o.Endpoint, o.Emulator = ResolveEndpoint("")
	}
	s := &Store{scope: d.scope}
	if err := s.at(o.Endpoint); err != nil {
		return nil, errors.New("remote: the destination endpoint is not a URL without user information, query or fragment")
	}
	d.scope = s.scope
	client, err := writeClient(ctx, pool, o)
	if err != nil {
		return nil, err
	}
	d.o = objfile.Root(gcs.New(client, l.Container), l.Prefix)
	d.w = objfile.RootWriter(gcs.NewWriter(client, l.Container, o.Write), l.Prefix)
	d.close = func() error { err := client.Close(); pool.CloseIdleConnections(); return err }
	return d, nil
}

// writeClient is a read-write GCS client over pool for o (emulator: the
// SDK's emulator client, without credentials). Reads go through
// ranged.Transport, as Open's do. The SDK's logger is silenced: its debug
// output, switched on by the environment, writes request headers
// unredacted, the bearer token included.
func writeClient(ctx context.Context, pool http.RoundTripper, o CreateOptions) (*storage.Client, error) {
	tr := ranged.Transport(pool)
	opts := []option.ClientOption{option.WithLogger(slog.New(slog.DiscardHandler))}
	switch {
	case o.Emulator:
	case o.CredentialsFile != "":
		b, err := os.ReadFile(o.CredentialsFile)
		if err != nil {
			return nil, fmt.Errorf("remote: destination credentials: %w", err)
		}
		// The token source keeps this context for every refresh.
		creds, err := google.CredentialsFromJSON(context.WithoutCancel(ctx), b, storage.ScopeReadWrite)
		if err != nil {
			return nil, fmt.Errorf("remote: destination credentials %s: %w", o.CredentialsFile, err)
		}
		if tr, err = htransport.NewTransport(ctx, tr, option.WithTokenSource(creds.TokenSource)); err != nil {
			return nil, err
		}
	default:
		var err error
		if tr, err = htransport.NewTransport(ctx, tr, option.WithScopes(storage.ScopeReadWrite), internaloption.EnableNewAuthLibrary()); err != nil {
			return nil, err
		}
	}
	opts = append(opts, option.WithHTTPClient(&http.Client{Transport: tr}))
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
	c, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, err
	}
	// Reads (metadata, ranges) are bounded as uploads are: the SDK's
	// default retries them until the context ends.
	c.SetRetry(storage.WithMaxAttempts(gcs.DefaultMaxAttempts))
	return c, nil
}

// DefaultRequestTimeout bounds the wait for a response's headers.
const DefaultRequestTimeout = 2 * time.Minute

// ResolveEndpoint is the destination endpoint the options' Endpoint
// resolves to: endpoint itself when set, else the emulator
// STORAGE_EMULATOR_HOST names (emulator true), else "" (Google's). A
// caller that freezes a destination's route resolves it once, records it,
// and passes the result to Create.
func ResolveEndpoint(endpoint string) (string, bool) {
	if endpoint != "" {
		return endpoint, false
	}
	if host := os.Getenv("STORAGE_EMULATOR_HOST"); host != "" {
		return host, true
	}
	return "", false
}

// Writer is the Destination's writer of names under the location's
// prefix.
func (d *Destination) Writer() objfile.Writer { return d.w }

// Open opens name under the location's prefix (reading back a write).
func (d *Destination) Open(ctx context.Context, name string) (objfile.Object, error) {
	return d.o.Open(ctx, name)
}

// OpenSized opens name, of size bytes, under the location's prefix.
func (d *Destination) OpenSized(ctx context.Context, name string, size int64) (objfile.Object, error) {
	return d.o.OpenSized(ctx, name, size)
}

// Scope names the destination's namespace: the location's canonical URI,
// and " at " the endpoint when one is set. It holds no credentials.
func (d *Destination) Scope() string { return d.scope }

// Close releases the client and its idle connections.
func (d *Destination) Close() error { return d.close() }

var _ interface {
	objfile.Opener
	io.Closer
} = (*Destination)(nil)
