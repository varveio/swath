// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objhttp

import (
	"context"
	"errors"
	"fmt"
	"time"

	"golang.org/x/oauth2"

	"github.com/varveio/swath/go/internal/source/tokensource"
)

// WaitToken waits for a shared token under the attempt's deadline. A nil
// source needs no token. A failed wait is a credential failure from source
// (discovered: CredentialError.Discovered), before any listing request is
// sent or counted.
func WaitToken(ctx context.Context, ts *tokensource.Shared, source string, discovered bool, timeout time.Duration, transient func(error) bool) (*oauth2.Token, error) {
	if ts == nil {
		return nil, nil
	}
	t, err := ts.Token(ctx)
	switch {
	case err == nil:
		return t, nil
	case ctx.Err() != nil:
		return nil, &CredentialError{Source: source, Discovered: discovered, Transient: true, Timeout: true,
			Err: fmt.Errorf("no token within %s: %v", timeout, ctx.Err())}
	}
	return nil, &CredentialError{Source: source, Discovered: discovered, Transient: transient(err),
		Timeout: errors.Is(err, context.DeadlineExceeded), Err: err}
}

// TransientStatus reports whether a credential endpoint's HTTP status may pass.
func TransientStatus(status int) bool {
	return status >= 500 || status == 408 || status == 429
}

// CredentialRefused reports a credential failure that will not pass: no
// credentials found, or configured ones that do not load or are refused.
// The command matches it by this method, CredentialSource and
// CredentialDiscovered (it imports providers only to register them) to show
// it in one line: the source, the cause's first line, and --anonymous where
// nothing was configured; the rest of the cause (an SDK's chain report, a
// token endpoint's body) is left to -v.
func (e *CredentialError) CredentialRefused() bool { return !e.Transient }

// CredentialSource is where the credentials came from (Source).
func (e *CredentialError) CredentialSource() string { return e.Source }

// CredentialDiscovered reports that the SDK's own search failed, nothing
// having been configured (Discovered).
func (e *CredentialError) CredentialDiscovered() bool { return e.Discovered }
