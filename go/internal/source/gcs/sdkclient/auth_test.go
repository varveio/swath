// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sdkclient

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"golang.org/x/oauth2"

	"github.com/varveio/swath/go/internal/buildinfo"
	"github.com/varveio/swath/go/internal/source/gcs/client"
	"github.com/varveio/swath/go/internal/source/objhttp"
)

type tokens struct{ err error }

func (s tokens) Token() (*oauth2.Token, error) {
	if s.err != nil {
		return nil, s.err
	}
	return &oauth2.Token{AccessToken: "tok", TokenType: "bearer", Expiry: time.Now().Add(time.Hour)}, nil
}

// TestCredentialsAndHeaders: each attempt carries the shared token source's
// bearer token and swath's User-Agent (as the hand-rolled client sends
// them); a token the source will not issue is a credential failure before
// any request (no attempt counted).
func TestCredentialsAndHeaders(t *testing.T) {
	var auth, agent string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth, agent = r.Header.Get("Authorization"), r.Header.Get("User-Agent")
		io.WriteString(w, `{"kind":"storage#objects"}`)
	}))
	defer srv.Close()
	for _, sdk := range []bool{false, true} {
		hand, s := pair(t, client.Config{Endpoint: srv.URL + "/storage/v1/", TokenSource: tokens{}})
		c := hand
		if sdk {
			c = s
		}
		if _, err := c.List(context.Background(), &client.ListInput{Bucket: "b"}); err != nil {
			t.Fatal(err)
		}
		if auth != "Bearer tok" || agent != buildinfo.Tool {
			t.Fatalf("sdk %v: Authorization %q, User-Agent %q", sdk, auth, agent)
		}
	}
	c, err := New(context.Background(), client.Config{Endpoint: srv.URL + "/storage/v1/", TokenSource: tokens{errors.New("refused")}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.List(context.Background(), &client.ListInput{Bucket: "b"})
	if client.Classify(err) != objhttp.ClassAuth || c.Counters().Attempts != 0 {
		t.Fatalf("err %v, counters %+v", err, c.Counters())
	}
	if source, anonymous := c.Credentials(); source != "token source" || anonymous {
		t.Fatalf("credentials %q %v", source, anonymous)
	}
}
