// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"errors"
	"syscall"
)

// connReset reports a connection the peer reset or aborted. The SDK's
// policy (storage.ShouldRetry) matches "connection reset" in the message,
// which Windows words "An existing connection was forcibly closed by the
// remote host": matched here by its error number.
func connReset(err error) bool {
	return errors.Is(err, syscall.WSAECONNRESET) || errors.Is(err, syscall.WSAECONNABORTED)
}
