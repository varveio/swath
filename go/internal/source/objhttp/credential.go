// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objhttp

// CredentialRefused reports a credential failure that will not pass: no
// credentials found, or configured ones that do not load or are refused.
// The command matches it by this method (it imports providers only to
// register them) to say that a public bucket needs --anonymous.
func (e *CredentialError) CredentialRefused() bool { return !e.Transient }
