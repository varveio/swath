// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"cloud.google.com/go/compute/metadata"
	"golang.org/x/oauth2"

	"github.com/varveio/swath/internal/source/objhttp"
)

type failingTokens struct{ err error }

func (f failingTokens) Token() (*oauth2.Token, error) { return nil, f.err }

// TestClassify: the reason-sensitive and credential classes, through the
// client as a run meets them (one attempt, so nothing is retried away);
// transient classes come back wrapped in objhttp.ExhaustedError. A 403 is a
// throttle when its reason says userRateLimitExceeded. The status classes
// are TestAnswers'.
func TestClassify(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(403)
		fmt.Fprint(w, `{"error":{"code":403,"message":"m","errors":[{"reason":"userRateLimitExceeded"}]}}`)
	}))
	defer srv.Close()
	c, err := New(context.Background(), Config{Endpoint: srv.URL + "/storage/v1/", Anonymous: true, Retry: objhttp.Retry{MaxAttempts: 1}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.List(context.Background(), &ListInput{Bucket: "b"})
	var f *objhttp.Failure
	var se *Error
	var ex *objhttp.ExhaustedError
	if !errors.As(err, &f) || !f.Transient || !errors.As(err, &se) || f.Status != se.Status || f.Code != se.Reason ||
		objhttp.Classify(err) != objhttp.ClassThrottle || !errors.As(err, &ex) {
		t.Fatalf("403 userRateLimitExceeded: class %q, failure %+v (%v)", objhttp.Classify(err), f, err)
	}
	// Credentials: a refused grant, and a token source that fails in transit.
	for _, x := range []struct {
		name      string
		err       error
		exhausted bool
		class     objhttp.Class // when not the default for exhausted
	}{
		{"refused grant", &oauth2.RetrieveError{Response: &http.Response{StatusCode: 400}, ErrorCode: "invalid_grant"}, false, ""},
		{"token endpoint 503", &oauth2.RetrieveError{Response: &http.Response{StatusCode: 503}}, true, ""},
		{"token endpoint unreachable", &net.OpError{Op: "dial", Err: errors.New("connection refused")}, true, ""},
		{"metadata server 503", &metadata.Error{Code: 503, Message: "unavailable"}, true, ""},
		{"metadata server 429", fmt.Errorf("compute: %w", &metadata.Error{Code: 429}), true, ""},
		{"metadata server 404", &metadata.Error{Code: 404, Message: "no service account"}, false, ""},
		{"token exchange timeout", fmt.Errorf("oauth2: %w", context.DeadlineExceeded), true, objhttp.ClassTimeout},
	} {
		e, _ := withTokens(Config{Endpoint: srv.URL + "/storage/v1/", Retry: objhttp.Retry{MaxAttempts: 1}}, failingTokens{x.err})
		_, err := e.List(context.Background(), &ListInput{Bucket: "b"})
		var ex *objhttp.ExhaustedError
		want := objhttp.ClassAuth // a refusal stops the run; an outage of the token endpoint is transient
		if x.exhausted {
			want = objhttp.ClassOther
		}
		if x.class != "" {
			want = x.class
		}
		var ce *objhttp.CredentialError
		if objhttp.Classify(err) != want || errors.As(err, &ex) != x.exhausted || !errors.As(err, &ce) {
			t.Errorf("%s: class %q exhausted=%v (%v)", x.name, objhttp.Classify(err), errors.As(err, &ex), err)
		}
	}
}
