// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/logging"

	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/internal/source/tokensource"
)

// Credentials come from the AWS SDK's config module: its default chain
// (environment, shared config and credentials files and profiles, SSO, web
// identity, credential processes, assume-role through stscreds, container
// credentials, IMDSv2). The SDK's service client signs with them (SigV4).

// credentialWait bounds the first credential lookup: a slow token exchange
// is waited for, never taken for absence.
const credentialWait = 30 * time.Second

// auth is the resolved region and credentials a client signs with.
type auth struct {
	credentials aws.CredentialsProvider // nil: anonymous or bearer; cached by the SDK until expiry
	source      string                  // where the credentials come from (never the secret)
	region      string                  // the signing region
	anonymous   bool
	bearer      *tokensource.Shared // --s3-bearer-token-command: sent instead of a signature
	note        string              // a one-time warning for the user, or ""
}

// loadAuth resolves the region and credentials. With Anonymous (and only
// then) no credentials are looked up and requests go unsigned. Otherwise
// the SDK's default chain runs once, under credentialWait: credentials it
// cannot give (none configured, a refused grant, a token service that does
// not answer) are a *objhttp.CredentialError (class auth) and nothing is
// sent. Once a lookup has succeeded, each request's signing refreshes them,
// a transient refresh failure retried (then the range requeued).
func loadAuth(ctx context.Context, cfg Config) (*auth, error) {
	// The SDK's own log lines (an IMDS fallback warning on every host
	// without instance metadata) are not for swath's users.
	opts := []func(*config.LoadOptions) error{config.WithLogger(logging.Nop{})}
	if cfg.Profile != "" {
		opts = append(opts, config.WithSharedConfigProfile(cfg.Profile))
	}
	// The trust the listing has (--ca-bundle over AWS_CA_BUNDLE,
	// --no-verify-ssl) is the credential chain's too: STS, SSO, web
	// identity.
	tc, err := cfg.Trust.Config()
	if err != nil {
		return nil, err
	}
	if tc != nil {
		opts = append(opts, config.WithHTTPClient(awshttp.NewBuildableClient().WithTransportOptions(func(t *http.Transport) { t.TLSClientConfig = tc })))
	}
	if cfg.Trust.CABundle != "" && !cfg.Trust.NoVerify {
		// Given, it replaces the SDK's own read of AWS_CA_BUNDLE.
		pem, err := os.ReadFile(cfg.Trust.CABundle)
		if err != nil {
			return nil, source.Unsupported(fmt.Errorf("CA bundle: %w", err))
		}
		opts = append(opts, config.WithCustomCABundle(bytes.NewReader(pem)))
	}
	// The bucket's region (--s3-region) is the signing region only. The
	// credential chain (STS for web identity and assumed roles) keeps the
	// environment's or the profile's region, else us-east-1, so a bucket in
	// another region never moves the STS endpoint.
	opts = append(opts, config.WithDefaultRegion("us-east-1"))
	if cfg.Anonymous || cfg.Bearer != nil {
		opts = append(opts, config.WithCredentialsProvider(aws.AnonymousCredentials{}))
	} else if why := partialEnvCredentials(); why != "" {
		// Credential intent the SDK drops without a word (one key of a
		// pair, a role without its token) would leave another source of
		// the chain to sign: it is configured, and broken.
		return nil, &objhttp.CredentialError{Source: "environment", Err: errors.New(why)}
	}
	ac, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		// A profile that does not exist, a role without its token: the
		// configuration names credentials it cannot give (class auth).
		return nil, &objhttp.CredentialError{Source: "AWS configuration", Err: err}
	}
	a := &auth{region: cfg.Region, note: anonymousNote(cfg)}
	if a.region == "" {
		a.region = ac.Region // the environment's or the profile's, else us-east-1
	}
	switch {
	case cfg.Bearer != nil:
		// The token replaces the credential chain: nothing else is looked up.
		a.bearer, a.source = cfg.Bearer, "bearer token command"
		return a, nil
	case cfg.Anonymous:
		a.anonymous, a.source = true, "anonymous"
		return a, nil
	case ac.Credentials == nil:
		return nil, &objhttp.CredentialError{Source: "AWS configuration", Err: errors.New("no credential provider")}
	}
	src, tf := "AWS default chain", tokenFile(ac)
	if cfg.Profile != "" {
		src = "profile " + cfg.Profile
	}
	if tf != "" {
		src += ", web identity"
	}
	lookup, cancel := context.WithTimeout(ctx, credentialWait)
	defer cancel()
	c, err := retrieve(lookup, ac.Credentials, tf)
	switch {
	case ctx.Err() != nil:
		// The caller gave up (a signal, its deadline): not a verdict on
		// the credentials.
		return nil, context.Cause(ctx)
	case err != nil:
		return nil, &objhttp.CredentialError{Source: src, Err: err}
	}
	a.credentials, a.source = ac.Credentials, c.Source
	return a, nil
}

// retrieve is the first lookup. A web-identity token file that is not
// there yet (mounted after the process started) is waited for until ctx
// ends, then looked up once more. Whether the file is there is read
// before the lookup, not after a failed one: a file that appears between
// the two would otherwise make its own absence the final answer.
func retrieve(ctx context.Context, p aws.CredentialsProvider, tokenFile string) (aws.Credentials, error) {
	for {
		present := tokenFile == ""
		if !present {
			_, serr := os.Stat(tokenFile)
			present = !errors.Is(serr, fs.ErrNotExist)
		}
		c, err := p.Retrieve(ctx)
		if err == nil || present {
			return c, err
		}
		select {
		case <-ctx.Done():
			return c, err
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// tokenFile is the web-identity token file the environment or the shared
// config names ("" for none): the first lookup waits for it (retrieve).
func tokenFile(ac aws.Config) string {
	for _, s := range ac.ConfigSources {
		var f string
		switch v := s.(type) {
		case config.EnvConfig:
			f = v.WebIdentityTokenFilePath
		case *config.EnvConfig:
			f = v.WebIdentityTokenFilePath
		case config.SharedConfig:
			f = v.WebIdentityTokenFile
		case *config.SharedConfig:
			f = v.WebIdentityTokenFile
		}
		if f != "" {
			return f
		}
	}
	return ""
}

// partialEnvCredentials names credential intent in the environment that
// the SDK's chain ignores ("" when there is none): an access key without
// its secret or the reverse (either spelling), a session token without
// both keys, a role ARN without a web-identity token file. The SDK keeps
// environment keys only as a pair, and builds web identity only from both
// variables; the rest of the chain then sees nothing.
func partialEnvCredentials() string {
	get := func(names ...string) string {
		for _, n := range names {
			if v := os.Getenv(n); v != "" {
				return v
			}
		}
		return ""
	}
	id, secret := get("AWS_ACCESS_KEY_ID", "AWS_ACCESS_KEY"), get("AWS_SECRET_ACCESS_KEY", "AWS_SECRET_KEY")
	switch {
	case id != "" && secret == "":
		return "AWS_ACCESS_KEY_ID is set without AWS_SECRET_ACCESS_KEY"
	case id == "" && secret != "":
		return "AWS_SECRET_ACCESS_KEY is set without AWS_ACCESS_KEY_ID"
	case id == "" && os.Getenv("AWS_SESSION_TOKEN") != "":
		return "AWS_SESSION_TOKEN is set without AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY"
	case os.Getenv("AWS_ROLE_ARN") != "" && os.Getenv("AWS_WEB_IDENTITY_TOKEN_FILE") == "":
		return "AWS_ROLE_ARN is set without AWS_WEB_IDENTITY_TOKEN_FILE"
	}
	return ""
}

// credentialVars are the environment variables that give the AWS SDK's
// chain a credential: Anonymous ignores them (anonymousNote).
var credentialVars = []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE",
	"AWS_ROLE_ARN", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI"}

// anonymousNote warns once when Anonymous hides credentials the
// environment holds.
func anonymousNote(cfg Config) string {
	if !cfg.Anonymous {
		return ""
	}
	return source.AnonymousNote("S3", credentialVars...)
}

// ready fetches what a request needs before it is sent: the bearer token
// (token), or the credentials (cached by the SDK until expiry; the zero
// value when anonymous). A failure is a *objhttp.CredentialError: a request
// is never sent without them.
func (a *auth) ready(ctx context.Context) (token string, creds aws.Credentials, err error) {
	switch {
	case a.bearer != nil:
		tok, err := a.bearer.Token(ctx)
		if err != nil {
			return "", creds, &objhttp.CredentialError{Source: "--s3-bearer-token-command", Err: err}
		}
		return tok.AccessToken, creds, nil
	case a.anonymous:
		return "", creds, nil
	}
	creds, err = a.credentials.Retrieve(ctx)
	if err != nil {
		return "", creds, &objhttp.CredentialError{Source: a.source, Transient: transientRefresh(err),
			Timeout: errors.Is(err, context.DeadlineExceeded), Err: err}
	}
	return "", creds, nil
}

// transientRefresh: a credential service's (STS, IMDS, SSO, container
// endpoint) 5xx, 408 and 429 or throttling codes, transport failures and a
// refresh past its deadline may pass and are retried, then requeued; a
// refused or expired grant, a missing role or an unusable profile will not.
func transientRefresh(err error) bool {
	var hs interface{ HTTPStatusCode() int }
	if errors.As(err, &hs) {
		if objhttp.TransientStatus(hs.HTTPStatusCode()) {
			return true
		}
	}
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "Throttling", "ThrottlingException", "RequestLimitExceeded", "TooManyRequestsException",
			"IDPCommunicationError", "InternalError", "InternalFailure", "ServiceUnavailable":
			return true
		}
		return false
	}
	return objhttp.Transport(err) || errors.Is(err, context.DeadlineExceeded)
}
