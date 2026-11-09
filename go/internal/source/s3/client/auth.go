// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/logging"

	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/internal/source/tokensource"
)

// Credentials come from the AWS SDK's config module: its default chain
// (environment, shared config and credentials files and profiles, SSO, web
// identity, credential processes, assume-role through stscreds, container
// credentials, IMDSv2). The SDK's service client signs with them (SigV4).

// credentialProbe bounds the first credential lookup of an empty chain
// (the environment names no source, so only the instance metadata service
// is left), so a host without credentials falls back to anonymous quickly.
// credentialWait bounds the first lookup of a configured source: a slow
// token exchange is waited for, never taken for absence.
const (
	credentialProbe = 3 * time.Second
	credentialWait  = 30 * time.Second
)

// auth is the resolved region and credentials a client signs with.
type auth struct {
	credentials aws.CredentialsProvider // nil: anonymous or bearer; cached by the SDK until expiry
	source      string                  // where the credentials come from (never the secret)
	region      string                  // the signing region
	anonymous   bool
	bearer      *tokensource.Shared // --bearer-token-command: sent instead of a signature
	note        string              // a one-time warning for the user, or ""
}

// loadAuth resolves the region and credentials. With -anonymous no
// credentials are looked up. Otherwise the default chain runs once. Only an
// empty chain (no source configured anywhere: no environment keys, profile,
// web identity, credential process, SSO, role or container endpoint, and no
// sign of an instance metadata service) whose lookup finds nothing goes
// unsigned. A configured source fails closed: a lookup that fails for good
// is a *objhttp.CredentialError (class auth); one that fails transiently (a
// token service's 5xx or throttle, a cut connection, past its wait) keeps
// the source, and each request's signing retries it (then the range is
// requeued), as a refresh failure mid-run would.
func loadAuth(ctx context.Context, cfg Config) (*auth, error) {
	// The SDK's own log lines (an IMDS fallback warning on every host
	// without instance metadata) are not for swath's users.
	opts := []func(*config.LoadOptions) error{config.WithLogger(logging.Nop{})}
	if cfg.Profile != "" {
		opts = append(opts, config.WithSharedConfigProfile(cfg.Profile))
	}
	// The bucket's region (--region) is the signing region only. The
	// credential chain (STS for web identity and assumed roles) keeps the
	// environment's or the profile's region, else us-east-1, so a bucket in
	// another region never moves the STS endpoint.
	opts = append(opts, config.WithDefaultRegion("us-east-1"))
	if cfg.Anonymous || cfg.Bearer != nil {
		opts = append(opts, config.WithCredentialsProvider(aws.AnonymousCredentials{}))
	}
	if !cfg.Anonymous && cfg.Bearer == nil {
		// Credential intent the SDK drops without a word (one key of a
		// pair, a role without its token) would read as an empty chain
		// and list unsigned: it is configured, and broken.
		if why := partialEnvCredentials(); why != "" {
			return nil, &objhttp.CredentialError{Source: "environment", Err: errors.New(why)}
		}
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
	if cfg.Bearer != nil {
		// The token replaces the credential chain: nothing else is looked up.
		a.bearer, a.source = cfg.Bearer, "bearer token command"
		return a, nil
	}
	if cfg.Anonymous || ac.Credentials == nil {
		a.anonymous, a.source = true, "anonymous"
		return a, nil
	}
	src, tokenFile := configuredSource(ac)
	if src == "" && cfg.Profile != "" {
		src = "profile " + cfg.Profile
	}
	wait := credentialProbe
	if src != "" {
		wait = credentialWait
	}
	lookup, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	c, err := retrieve(lookup, ac.Credentials, tokenFile)
	switch {
	case ctx.Err() != nil:
		// The caller gave up (a signal, its deadline): not a verdict on
		// the credentials.
		return nil, context.Cause(ctx)
	case err == nil:
		a.credentials, a.source = ac.Credentials, c.Source
	case src == "":
		a.anonymous, a.source = true, "anonymous (no credentials found)"
	case transientRefresh(err):
		a.credentials, a.source = ac.Credentials, src
		a.note = joinNotes(a.note, fmt.Sprintf("credentials (%s) not available yet: %v; requests retry them", src, err))
	default:
		return nil, &objhttp.CredentialError{Source: src, Err: err}
	}
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

// configuredSource names the credential source the environment or the
// shared files configure ("" for an empty chain), and the web-identity
// token file if one is named.
func configuredSource(ac aws.Config) (src, tokenFile string) {
	imdsOn := true
	for _, s := range ac.ConfigSources {
		var e *config.EnvConfig
		var sc *config.SharedConfig
		switch v := s.(type) {
		case config.EnvConfig:
			e = &v
		case *config.EnvConfig:
			e = v
		case config.SharedConfig:
			sc = &v
		case *config.SharedConfig:
			sc = v
		}
		switch {
		case e != nil:
			if e.EC2IMDSClientEnableState == imds.ClientDisabled {
				imdsOn = false
			}
			if e.EC2IMDSEndpoint != "" && src == "" {
				src = "instance metadata (AWS_EC2_METADATA_SERVICE_ENDPOINT)"
			}
			switch {
			case e.Credentials.AccessKeyID != "":
				return "environment keys", ""
			case e.WebIdentityTokenFilePath != "":
				return "web identity (AWS_WEB_IDENTITY_TOKEN_FILE)", e.WebIdentityTokenFilePath
			case e.ContainerCredentialsEndpoint != "" || e.ContainerCredentialsRelativePath != "":
				return "container credentials", ""
			case e.SharedConfigProfile != "":
				src = "profile " + e.SharedConfigProfile
			}
		case sc != nil:
			switch {
			case sc.WebIdentityTokenFile != "":
				return "profile " + sc.Profile + " (web identity)", sc.WebIdentityTokenFile
			case sc.Credentials.AccessKeyID != "", sc.RoleARN != "", sc.CredentialProcess != "",
				sc.CredentialSource != "", sc.SSOSession != nil, sc.SSOStartURL != "", sc.LoginSession != "":
				return "profile " + sc.Profile, ""
			}
			if sc.EC2IMDSEndpoint != "" && src == "" {
				src = "instance metadata (profile " + sc.Profile + ")"
			}
		}
	}
	if src == "" && imdsOn && onEC2() {
		src = "instance metadata"
	}
	return src, ""
}

// dmiRoot is where onEC2 reads the machine's identity (tests move it).
var dmiRoot = "/sys"

// onEC2 reports the signs Linux gives of an EC2 instance (where the
// instance metadata service holds a role's credentials): the DMI vendor
// or asset tag, or a Xen hypervisor id. Elsewhere it is false.
func onEC2() bool {
	read := func(p string) string {
		b, _ := os.ReadFile(filepath.Join(dmiRoot, p))
		return strings.TrimSpace(string(b))
	}
	return strings.Contains(read("devices/virtual/dmi/id/sys_vendor"), "Amazon") ||
		strings.HasPrefix(read("devices/virtual/dmi/id/board_asset_tag"), "i-") ||
		strings.HasPrefix(strings.ToLower(read("hypervisor/uuid")), "ec2")
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

// anonymousNote warns once when -anonymous hides credentials the
// environment holds.
func anonymousNote(cfg Config) string {
	if cfg.Anonymous && (os.Getenv("AWS_ACCESS_KEY_ID") != "" || os.Getenv("AWS_PROFILE") != "" || os.Getenv("AWS_SESSION_TOKEN") != "") {
		return "-anonymous: ignoring the AWS_* credentials in the environment"
	}
	return ""
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
			return "", creds, &objhttp.CredentialError{Source: "--bearer-token-command", Err: err}
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
		if s := hs.HTTPStatusCode(); s >= 500 || s == 408 || s == 429 {
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

func joinNotes(a, b string) string {
	if a == "" || b == "" {
		return a + b
	}
	return a + "; " + b
}
