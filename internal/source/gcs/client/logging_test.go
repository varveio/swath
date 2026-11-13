// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"golang.org/x/oauth2"
)

// secretToken is a synthetic bearer token that must reach the server and
// no log.
const secretToken = "ya29.synthetic-bearer-must-not-be-logged"

type secretTokens struct{}

func (secretTokens) Token() (*oauth2.Token, error) {
	return &oauth2.Token{AccessToken: secretToken, TokenType: "Bearer", Expiry: time.Now().Add(time.Hour)}, nil
}

// TestSDKLogsNothing: with the generated client's debug logging switched
// on in the environment (GOOGLE_SDK_GO_LOGGING_LEVEL=debug, which by
// default logs every request's headers, Authorization included, and every
// response body to stderr), a listing writes nothing to stderr: the
// service is built with a logger that emits nothing, so a bearer token
// never reaches a log.
func TestSDKLogsNothing(t *testing.T) {
	t.Setenv("GOOGLE_SDK_GO_LOGGING_LEVEL", "debug")
	var auth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = r.Header.Get("Authorization")
		io.WriteString(w, `{"kind":"storage#objects","items":[{"name":"listed-name"}]}`)
	}))
	defer srv.Close()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	out := make(chan []byte)
	go func() {
		b, _ := io.ReadAll(r)
		out <- b
	}()
	stderr := os.Stderr
	os.Stderr = w
	c, err := withTokens(Config{Endpoint: srv.URL + "/storage/v1/"}, secretTokens{})
	if err == nil {
		_, err = c.List(context.Background(), &ListInput{Bucket: "b"})
	}
	os.Stderr = stderr
	w.Close()
	logged := <-out
	if err != nil {
		t.Fatal(err)
	}
	if auth != "Bearer "+secretToken {
		t.Fatalf("Authorization %q: the token was not sent", auth)
	}
	for _, s := range []string{secretToken, "listed-name", "api request"} {
		if bytes.Contains(logged, []byte(s)) {
			t.Fatalf("the SDK logged %q:\n%s", s, logged)
		}
	}
}
