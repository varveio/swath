// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"net"
	"net/url"
	"os"
	"syscall"
	"testing"
)

// TestResetRetriedOnWindows: a chunk whose connection the peer reset is
// retried. Windows words the reset "An existing connection was forcibly
// closed", which the SDK's string match missed: the upload failed
// (listing's TestRemoteMultiChunk on windows-latest).
func TestResetRetriedOnWindows(t *testing.T) {
	for _, errno := range []syscall.Errno{syscall.WSAECONNRESET, syscall.WSAECONNABORTED} {
		err := &url.Error{Op: "Post", URL: "http://127.0.0.1/upload", Err: &net.OpError{Op: "write", Net: "tcp", Err: os.NewSyscallError("wsasend", errno)}}
		if !retryable(err) {
			t.Errorf("%v: not retried", err)
		}
	}
}
