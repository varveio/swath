// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	azsdk "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/objfile"
	"github.com/varveio/swath/objfile/azblob"
	"github.com/varveio/swath/objfile/azblob/azfake"
)

// TestRemoteAzureOverlap: an Azure destination overlaps a listed root of
// the same account and container whose prefix holds it or is held by it;
// another account or container never does.
func TestRemoteAzureOverlap(t *testing.T) {
	src := source.Locator{Kind: "azblob", Root: "az://acct/ctr/a", Container: "ctr"}
	for dst, overlaps := range map[string]bool{
		"az://acct/ctr": true, "az://acct/ctr/a": true, "az://acct/ctr/ab/c": true, "az://acct/ctr/out": false,
		"az://other/ctr/a": false, "az://acct/ctr2/a": false, "gs://ctr/a": false,
	} {
		l, err := ParseDestination(dst)
		if err != nil {
			t.Fatal(err)
		}
		if err := checkOverlap(src, l); errors.Is(err, ErrOverlap) != overlaps {
			t.Errorf("source %s, destination %s: %v", src.Root, dst, err)
		}
	}
}

// TestRemoteAzureSecrecy: a block's upload fails in transport, and the
// transport error quotes the request URL with the SAS and the block id;
// neither the run's error, its warnings, its report nor the work
// directory's error file shows the signature or a block id.
func TestRemoteAzureSecrecy(t *testing.T) {
	const sig = "SYNTHETICSIGNATURE0123456789"
	url0 := bigBucket(t)
	f := azfake.New(dstAccount, dstBucket)
	var ids sync.Map
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("blockid")
		if id != "" {
			ids.Store(id, true)
		}
		if raw, _ := base64.StdEncoding.DecodeString(id); strings.HasSuffix(string(raw), "000001") {
			conn, _, _ := w.(http.Hijacker).Hijack()
			_ = conn.Close()
			return
		}
		f.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	loc := "az://" + dstAccount + "/" + dstBucket + "/s"
	out := &RemoteOutput{Location: loc, ChunkSize: azBlock, Open: func(ctx context.Context, st *dataset.UploadStats) (*dataset.Remote, func() error, error) {
		c, err := azsdk.NewClientWithNoCredential(srv.URL+"/"+dstAccount+"/?sv=2026-06-06&sp=rcwd&sig="+sig, nil)
		if err != nil {
			return nil, nil, err
		}
		l, _ := objfile.ParseLocation(loc)
		w, err := azblob.NewWriter(c, dstBucket, azblob.WriteOptions{BlockSize: azBlock, MaxAttempts: 1, Retried: st.Retried})
		if err != nil {
			return nil, nil, err
		}
		return &dataset.Remote{Location: l, Opener: objfile.Root(azblob.New(c, dstBucket), "s"), Writer: objfile.RootWriter(w, "s")}, func() error { return nil }, nil
	}}
	ws := filepath.Join(t.TempDir(), "work")
	cfg := remoteConfig(url0, ws, false, out)
	var said []string
	var mu sync.Mutex
	cfg.Warn = func(m string) { mu.Lock(); said = append(said, m); mu.Unlock() }
	cfg.Settle = func(r Report, err error) {
		mu.Lock()
		said = append(said, r.Outcome.Error, fmt.Sprint(err))
		mu.Unlock()
	}
	rep, err := Run(context.Background(), cfg)
	if err == nil {
		t.Fatal("a run whose blocks fail succeeded")
	}
	b, _ := json.Marshal(rep)
	errFile, _ := os.ReadFile(filepath.Join(ws, dataset.ErrorName))
	n := 0
	for _, text := range append(said, err.Error(), string(b), string(errFile)) {
		if strings.Contains(text, sig) || strings.Contains(text, "blockid") {
			t.Fatalf("the signature or a block id escaped: %s", text)
		}
		ids.Range(func(k, _ any) bool {
			n++
			if id := k.(string); strings.Contains(text, id[:32]) {
				t.Fatalf("block id %s escaped: %s", id, text)
			}
			return true
		})
	}
	if n == 0 {
		t.Fatal("no block staged")
	}
}
