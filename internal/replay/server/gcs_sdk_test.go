// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"

	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	storage "google.golang.org/api/storage/v1"

	"github.com/varveio/swath/internal/replay/store"
	"github.com/varveio/swath/internal/replay/store/memstore"
)

// An official client library (the Go one) against the GCS route: its
// endpoint override, unmodified, auto-paginating with Pages.

// queries records each request's query on its way to the server.
type queries struct {
	mu   sync.Mutex
	seen []string
}

func (q *queries) RoundTrip(r *http.Request) (*http.Response, error) {
	q.mu.Lock()
	q.seen = append(q.seen, r.URL.RawQuery)
	q.mu.Unlock()
	return (&http.Transport{}).RoundTrip(r)
}

func sdkService(t *testing.T, s *Server, q *queries) *storage.Service {
	t.Helper()
	svc, err := storage.NewService(context.Background(), option.WithEndpoint(fmt.Sprintf("http://127.0.0.1:%d/storage/v1/", s.Port())),
		option.WithoutAuthentication(), option.WithHTTPClient(&http.Client{Transport: q}))
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

func sdkWalk(t *testing.T, call *storage.ObjectsListCall) (names []string) {
	t.Helper()
	err := call.Pages(context.Background(), func(p *storage.Objects) error {
		for _, it := range p.Items {
			if it.Generation != 1 || it.ContentType != "application/octet-stream" || it.Updated == "" {
				return fmt.Errorf("item %+v", it)
			}
			names = append(names, it.Name)
		}
		names = append(names, p.Prefixes...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return names
}

func TestOfficialClientUsesHierarchyOffsetsAndUnicode(t *testing.T) {
	keys := []string{"a/1", "a/2", "a/sub/1", "a/sub/2", "b", "é", "", "\U0001F600"}
	cfg := gcsConfig(memstore.Keys(memstore.Options{Rollup: true}, keys...), ProtocolGCS)
	s := start(t, cfg)
	q := &queries{}
	svc := sdkService(t, s, q)
	if got := sdkWalk(t, svc.Objects.List("bucket").MaxResults(1).Prefix("a/").StartOffset("a/2").EndOffset("b")); !slices.Equal(got, []string{"a/2", "a/sub/1", "a/sub/2"}) {
		t.Errorf("bounded: %v", got)
	}
	if len(q.seen) != 3 || !strings.Contains(q.seen[1], "pageToken=") || !strings.Contains(q.seen[0], "startOffset=a%2F2") || !strings.Contains(q.seen[0], "endOffset=b") {
		t.Errorf("bounded requests: %q", q.seen)
	}
	if got := sdkWalk(t, svc.Objects.List("bucket").MaxResults(1).Prefix("a/").Delimiter("/")); !slices.Equal(got, []string{"a/1", "a/2", "a/sub/"}) {
		t.Errorf("hierarchy: %v", got)
	}
	if got := sdkWalk(t, svc.Objects.List("bucket").MaxResults(1).Projection("full")); !slices.Equal(got, keys) {
		t.Errorf("ordered: %v", got)
	}
	if len(q.seen) != 3+3+8 {
		t.Errorf("%d requests", len(q.seen))
	}
}

// TestOfficialClientDecodesAReplayFault: a fixture row the API cannot
// represent reaches the client as the JSON API error it decodes, with the
// replay's reason.
func TestOfficialClientDecodesAReplayFault(t *testing.T) {
	size := int64(-1)
	st, err := memstore.New([]store.Object{{Key: []byte("bad"), Size: &size}}, memstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	s := start(t, gcsConfig(st, ProtocolGCS))
	q := &queries{}
	err = sdkService(t, s, q).Objects.List("bucket").Pages(context.Background(), func(*storage.Objects) error { return nil })
	var g *googleapi.Error
	if !errors.As(err, &g) || g.Code != 500 || len(g.Errors) != 1 || g.Errors[0].Reason != "internalError" ||
		g.Header.Get("x-swath-replay-error") != "negative_size" || len(q.seen) != 1 {
		t.Errorf("%v (%d requests)", err, len(q.seen))
	}
}
