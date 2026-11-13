// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package source

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"
)

// TestUnreachable: a real refused connection and a host not found are
// deterministic connect failures; a temporary DNS failure, a timeout, a
// cut connection and a refused certificate are not.
func TestUnreachable(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	host := ln.Addr().String()
	ln.Close()
	_, refused := http.Get("http://" + host + "/") // the platform's own error, wrapped as a client sees it
	if refused == nil {
		t.Skip("a closed port answered")
	}
	dial := func(err error) error {
		return &url.Error{Op: "Get", URL: "http://h/", Err: &net.OpError{Op: "dial", Net: "tcp", Err: err}}
	}
	for _, c := range []struct {
		name string
		err  error
		want bool
	}{
		{"refused", refused, true},
		{"host not found", dial(&net.DNSError{Err: "no such host", Name: "nohost.invalid", IsNotFound: true}), true},
		{"dns timeout", dial(&net.DNSError{Err: "i/o timeout", Name: "h", IsTimeout: true}), false},
		{"dns temporary", dial(&net.DNSError{Err: "server misbehaving", Name: "h", IsTemporary: true}), false},
		{"deadline", dial(context.DeadlineExceeded), false},
		{"cut", dial(io.ErrUnexpectedEOF), false},
		{"certificate", dial(&tls.CertificateVerificationError{Err: errors.New("x509: unknown authority")}), false},
		{"nil", nil, false},
	} {
		if got := Unreachable(c.err); got != c.want {
			t.Errorf("%s: Unreachable(%v) = %v", c.name, c.err, got)
		}
	}
}

// TestUnreachableStates: armed, the first deterministic connect failure
// starts the grace and one past it expires the run; an answer before
// that turns it off for good; disarmed (or never armed) it never expires;
// the grace is at most half an explicit stall timeout.
func TestUnreachableStates(t *testing.T) {
	g := UnreachableGrace
	UnreachableGrace = time.Second
	t.Cleanup(func() { UnreachableGrace = g })
	t0 := time.Unix(1_000_000, 0)
	grace, disarm := ArmUnreachable(time.Second)
	disarm()
	if grace != 500*time.Millisecond {
		t.Fatalf("grace %s under a 1s stall timeout", grace)
	}
	if expired, _ := ConnectFailed(t0.Add(time.Hour)); expired {
		t.Fatal("expired while not armed")
	}
	grace, disarm = ArmUnreachable(0)
	defer disarm()
	if expired, _ := ConnectFailed(t0); expired {
		t.Fatal("expired at the first failure")
	}
	if expired, _ := ConnectFailed(t0.Add(grace - 1)); expired {
		t.Fatal("expired inside the grace")
	}
	if expired, since := ConnectFailed(t0.Add(grace)); !expired || since != grace {
		t.Fatalf("not expired at the grace (%v, %s)", expired, since)
	}
	if expired, _ := ConnectFailed(t0.Add(grace + 1)); !expired {
		t.Fatal("an expired run un-expired")
	}
	ArmUnreachable(0)
	ConnectFailed(t0)
	Contacted()
	if _, armed := UnreachableArmed(); armed {
		t.Fatal("armed after an answer")
	}
	if expired, _ := ConnectFailed(t0.Add(time.Hour)); expired {
		t.Fatal("expired after an answer")
	}
}

// TestUnreachableAnswerRace: a caller that has read the run as armed and
// past its grace, and pauses, while another records an answer, does not
// stop the run: the expiry is a compare-and-swap the answer defeats (the
// interleaving the review reproduced: answered, yet unreachable).
func TestUnreachableAnswerRace(t *testing.T) {
	grace, disarm := ArmUnreachable(0)
	defer disarm()
	t0 := time.Unix(1_000_000, 0)
	ConnectFailed(t0)
	paused := 0
	reach.hook = func(int64) {
		paused++
		Contacted() // another worker's answer, between the load and the swap
	}
	t.Cleanup(func() { reach.hook = nil })
	if expired, _ := ConnectFailed(t0.Add(grace)); expired || paused != 1 {
		t.Fatalf("expired %v after an answer recorded mid-decision (hook ran %d times)", expired, paused)
	}
	if _, armed := UnreachableArmed(); armed {
		t.Fatal("armed after the answer")
	}
}
