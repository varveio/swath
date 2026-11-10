// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package source

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// Trust is how an object store client verifies the service's TLS
// certificate (--ca-bundle, --no-verify-ssl): the system roots, with
// CABundle's certificates added, or with NoVerify not at all.
type Trust struct {
	CABundle string // a PEM file of certificates trusted besides the system roots
	NoVerify bool   // accept any certificate and host name
}

// NoVerifyWarning is the warning a provider gives once when --no-verify-ssl
// is in force, a resume's replay of it included.
const NoVerifyWarning = "--no-verify-ssl: TLS certificates are not verified; anyone on the network path can read and alter the listing"

// Config is the TLS configuration t asks for: nil for the system's own
// (neither set). A CA bundle that cannot be read or holds no PEM
// certificate is refused, never ignored.
func (t Trust) Config() (*tls.Config, error) {
	if t.NoVerify {
		return &tls.Config{InsecureSkipVerify: true}, nil
	}
	if t.CABundle == "" {
		return nil, nil
	}
	pem, err := os.ReadFile(t.CABundle)
	if err != nil {
		return nil, Unsupported(fmt.Errorf("CA bundle: %w", err))
	}
	roots, err := x509.SystemCertPool()
	if err != nil {
		roots = x509.NewCertPool() // no system roots to add to: the bundle's alone
	}
	if !roots.AppendCertsFromPEM(pem) {
		return nil, Unsupported(fmt.Errorf("CA bundle %s: no PEM certificate in the file", t.CABundle))
	}
	return &tls.Config{RootCAs: roots}, nil
}
