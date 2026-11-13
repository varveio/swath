// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package source

import (
	"errors"
	"net"
	"time"
)

// UnreachableGrace is how long an object-store client retries an endpoint
// that has never answered while every attempt fails to connect
// deterministically (Unreachable): long enough for an emulator that is
// still starting, short of the stall timeout a mistyped address would
// otherwise wait out (objhttp's fail-fast; the command's warning names
// it). A constant but for tests, which shorten it.
var UnreachableGrace = 15 * time.Second

// Unreachable reports a connection that failed deterministically before
// any HTTP exchange: refused, the host not found, or no route to the
// network or host. Repeating it gives the same answer until someone starts
// the service or fixes the address. A timeout, a reset or a temporary DNS
// failure is not one; a refused certificate is already not retried
// (objhttp.Transport).
func Unreachable(err error) bool {
	if err == nil {
		return false
	}
	var dns *net.DNSError
	if errors.As(err, &dns) && dns.IsNotFound {
		return true
	}
	for _, e := range unreachableErrnos {
		if errors.Is(err, e) {
			return true
		}
	}
	return false
}
