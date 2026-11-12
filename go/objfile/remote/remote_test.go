// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package remote

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	azsdk "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"google.golang.org/api/option"

	"github.com/varveio/swath/go/objfile"
	"github.com/varveio/swath/go/objfile/internal/objfiletest"
)

const secret = "secret-key-never-recorded"

var data = bytes.Repeat([]byte("0123456789abcdef"), 512)

// store serves the object ds/obj of bucket (or container) bkt on the S3,
// GCS (JSON metadata and XML reads) and Azure Blob APIs. With shiftRest set,
// a ranged read's body is cut half way and every later read shifted by one
// byte.
type store struct {
	shiftRest bool
	reads     atomic.Int32
}

func (s *store) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if name, ok := strings.CutPrefix(r.URL.Path, "/storage/v1/b/bkt/o/"); ok {
		if name != "ds/obj" {
			http.Error(w, `{"error":{"code":404}}`, http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"bucket": "bkt", "name": name, "size": strconv.Itoa(len(data)), "generation": "7"})
		return
	}
	if r.URL.Path != "/bkt/ds/obj" {
		w.Header().Set("x-ms-error-code", "BlobNotFound")
		http.Error(w, "NoSuchKey", http.StatusNotFound)
		return
	}
	w.Header().Set("ETag", `"e1"`)
	w.Header().Set("X-Goog-Generation", "7")
	w.Header().Set("x-ms-blob-type", "BlockBlob")
	w.Header().Set("Last-Modified", "Thu, 01 Jan 1970 00:00:00 GMT")
	if r.Method == http.MethodHead {
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		return
	}
	rng := r.Header.Get("Range")
	if rng == "" {
		rng = r.Header.Get("x-ms-range")
	}
	var start, end int
	if _, err := fmt.Sscanf(rng, "bytes=%d-%d", &start, &end); err != nil || end >= len(data) {
		http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
		return
	}
	read := s.reads.Add(1)
	if s.shiftRest && read > 1 {
		start, end = start+1, end+1
	}
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
	w.Header().Set("Content-Length", strconv.Itoa(end-start+1))
	w.WriteHeader(http.StatusPartialContent)
	if s.shiftRest && read == 1 {
		_, _ = w.Write(data[start : start+(end-start+1)/2])
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler)
	}
	_, _ = w.Write(data[start : end+1])
}

// isolate points every provider's default configuration at nothing on this
// machine.
func isolate(t *testing.T) {
	for _, v := range []string{"AWS_CONFIG_FILE", "AWS_SHARED_CREDENTIALS_FILE"} {
		t.Setenv(v, t.TempDir()+"/none")
	}
	for _, v := range []string{"AWS_PROFILE", "AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3", "STORAGE_EMULATOR_HOST", "GOOGLE_APPLICATION_CREDENTIALS"} {
		t.Setenv(v, "")
	}
}

// server serves st, counting the connections open to it.
type server struct {
	*httptest.Server
	open atomic.Int32
}

func serve(t *testing.T, st *store) *server {
	srv := &server{Server: httptest.NewUnstartedServer(st)}
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		switch state {
		case http.StateNew:
			srv.open.Add(1)
		case http.StateClosed, http.StateHijacked:
			srv.open.Add(-1)
		}
	}
	srv.Start()
	t.Cleanup(srv.Close)
	return srv
}

// check opens ds/obj through location's Store, pinned under its scope,
// reads it, closes the Store, which must close its connections to srv, and
// returns the Store's scope and the snapshot.
func check(t *testing.T, srv *server, location string) (string, []byte) {
	t.Helper()
	ctx := context.Background()
	s, err := Open(ctx, location)
	if err != nil {
		t.Fatal(err)
	}
	p := objfile.Pin(objfile.Limit(s, 4), s.Scope())
	f, err := p.Open(ctx, "obj")
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 1000)
	if _, err := f.ReadAt(got, 100); err != nil || !bytes.Equal(got, data[100:1100]) {
		t.Fatalf("ReadAt: %v", err)
	}
	if _, err := p.Open(ctx, "missing"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Open(missing) = %v, want fs.ErrNotExist", err)
	}
	// Every attempt is observed, through the wrappers, with the store's
	// name for it (the prefix included), and the Store says so.
	if !s.ReportsIO() || !p.ReportsIO() || !f.(objfile.IOReporter).ReportsIO() {
		t.Fatal("the Store, its pinned Opener or its Object does not report complete observation")
	}
	obs := &objfiletest.Observer{}
	octx := objfile.WithObserver(ctx, obs)
	g, err := p.Open(octx, "obj")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.WithContext(octx).ReadAt(got, 100); err != nil {
		t.Fatal(err)
	}
	reqs, ends := obs.Events()
	if len(reqs) != 2 || len(ends) != 2 || reqs[0].Kind != objfile.MetadataRequest || reqs[1] != (objfile.Request{Kind: objfile.RangeRequest, Name: "ds/obj", Off: 100, Len: 1000}) ||
		ends[1] != (objfile.RequestResult{Bytes: 1000}) || reqs[0].Name != "ds/obj" {
		t.Fatalf("observed %+v, %+v; want the metadata request and the range", reqs, ends)
	}
	snap, err := p.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(snap, []byte(secret)) || strings.Contains(s.Scope(), secret) {
		t.Fatalf("a credential is in the scope or snapshot: %s", snap)
	}
	if srv.open.Load() == 0 {
		t.Fatal("no connection to the server")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(5 * time.Second); srv.open.Load() > 0; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("%d connections open after Close", srv.open.Load())
		}
	}
	return s.Scope(), snap
}

func TestOpenS3(t *testing.T) {
	isolate(t)
	srv := serve(t, &store{})
	t.Setenv("AWS_ENDPOINT_URL_S3", srv.URL)
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIDTEST")
	t.Setenv("AWS_SECRET_ACCESS_KEY", secret)
	scope, snap := check(t, srv, "s3://bkt/ds")
	if want := "s3://bkt/ds/ at " + srv.URL; scope != want {
		t.Errorf("Scope = %q, want %q", scope, want)
	}
	want := `{"version":1,"scope":"s3://bkt/ds/ at ` + srv.URL + `","objects":{"obj":{"size":8192,"identity":"\"e1\""}}}`
	if string(snap) != want {
		t.Errorf("Snapshot = %s, want %s", snap, want)
	}
}

func TestOpenGCS(t *testing.T) {
	isolate(t)
	srv := serve(t, &store{})
	u, _ := url.Parse(srv.URL)
	t.Setenv("STORAGE_EMULATOR_HOST", u.Host)
	if scope, _ := check(t, srv, "gs://bkt/ds/"); scope != "gs://bkt/ds/ at "+u.Host {
		t.Errorf("Scope = %q", scope)
	}

	// The Store's client checks every ranged answer (ranged.Transport): the
	// rest of a body cut short, requested again, is shifted.
	shifted := serve(t, &store{shiftRest: true})
	u, _ = url.Parse(shifted.URL)
	t.Setenv("STORAGE_EMULATOR_HOST", u.Host)
	s, err := Open(context.Background(), "gs://bkt/ds/")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	f, err := s.Open(context.Background(), "obj")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.ReadAt(make([]byte, 1000), 100); err == nil {
		t.Error("a shifted range was accepted")
	}
}

func TestOpenAzure(t *testing.T) {
	isolate(t)
	srv := serve(t, &store{})
	var account string
	defer func(f func(string, *http.Client) (*azsdk.Client, error)) { azureClient = f }(azureClient)
	azureClient = func(a string, hc *http.Client) (*azsdk.Client, error) {
		account = a
		return azsdk.NewClientWithNoCredential(srv.URL+"/", &azsdk.ClientOptions{ClientOptions: azcore.ClientOptions{Transport: hc}})
	}
	if scope, _ := check(t, srv, "az://acct/bkt/ds"); scope != "az://acct/bkt/ds/" || account != "acct" {
		t.Errorf("Scope = %q, account %q", scope, account)
	}
}

func TestOpenRefusesLocation(t *testing.T) {
	for _, bad := range []string{"s3://bkt/ds?x=1", "file:///tmp/ds", "az://acct"} {
		if s, err := Open(context.Background(), bad); err == nil {
			s.Close()
			t.Errorf("Open(%q) succeeded", bad)
		}
	}
}

// TestOpenRefusesCredentialEndpoint: an endpoint the environment sets with
// user information or a query is refused, and not quoted in the error.
func TestOpenRefusesCredentialEndpoint(t *testing.T) {
	isolate(t)
	t.Setenv("AWS_REGION", "us-east-1")
	for _, c := range []struct{ env, location, endpoint string }{
		{"AWS_ENDPOINT_URL_S3", "s3://bkt/ds", "https://user:" + secret + "@s3.example.com"},
		{"AWS_ENDPOINT_URL_S3", "s3://bkt/ds", "https://s3.example.com/?token=" + secret},
		{"STORAGE_EMULATOR_HOST", "gs://bkt/ds", "http://user:" + secret + "@localhost:9000"},
		{"STORAGE_EMULATOR_HOST", "gs://bkt/ds", "localhost:9000/?key=" + secret},
		{"AWS_ENDPOINT_URL", "s3://bkt/ds", "https://s3.example.com/#"},
		{"AWS_ENDPOINT_URL_S3", "s3://bkt/ds", "https://s3.example.com/#"},
		{"STORAGE_EMULATOR_HOST", "gs://bkt/ds", "localhost:9000/#"},
		{"AWS_ENDPOINT_URL_S3", "s3://bkt/ds", "https://127.0.0.1/%3Fsig=" + secret + "%0A"},
		{"AWS_ENDPOINT_URL_S3", "s3://bkt/ds", "https://127.0.0.1/%253Fsig=" + secret + "%25"},
	} {
		t.Setenv(c.env, c.endpoint)
		s, err := Open(context.Background(), c.location)
		if err == nil {
			s.Close()
			t.Errorf("%s=%s: Open succeeded, scope %q", c.env, c.endpoint, s.Scope())
		} else if strings.Contains(err.Error(), secret) {
			t.Errorf("%s: the error quotes the credential: %v", c.env, err)
		}
		t.Setenv(c.env, "")
	}
}

// TestOpenGCSCredentialsOutliveContext: the GCS credentials fetch each token
// with the context of the request that needs it, not the one the Store was
// opened with. With user credentials (application-default login), a Store
// opened with a context then cancelled still refreshes its token and reads;
// cancelling a read during its token exchange cancels the exchange.
func TestOpenGCSCredentialsOutliveContext(t *testing.T) {
	isolate(t)
	st := &store{}
	srv := serve(t, st)
	srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		st.ServeHTTP(w, r)
	})

	// User credentials exchange their refresh token at Google's fixed token
	// endpoint, over http.DefaultTransport; the test's transport dials that
	// endpoint's address to tokens.
	var exchanges atomic.Int32
	var block atomic.Bool
	arrived, cancelled := make(chan struct{}, 1), make(chan struct{}, 1)
	tokens := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm() // the server sees the client go only after the body is read
		if r.URL.Path != "/token" || r.Form.Get("refresh_token") != "refresh" {
			http.Error(w, "bad token request", http.StatusBadRequest)
			return
		}
		exchanges.Add(1)
		if block.Load() {
			arrived <- struct{}{}
			<-r.Context().Done()
			cancelled <- struct{}{}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		// Expired a second later: a request after that refreshes it before
		// it is sent (the library refreshes a token close to expiry in the
		// background, an expired one in the request).
		_, _ = w.Write([]byte(`{"access_token":"tok","token_type":"Bearer","expires_in":1}`))
	}))
	t.Cleanup(tokens.Close)
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.Proxy = nil // a proxy from the environment would carry the token request past the dialer below
	tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		if addr == "oauth2.googleapis.com:443" {
			addr = tokens.Listener.Addr().String()
		}
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}
	defer func(d http.RoundTripper) { http.DefaultTransport = d }(http.DefaultTransport)
	http.DefaultTransport = tr

	file := t.TempDir() + "/adc.json"
	if err := os.WriteFile(file, []byte(`{"type":"authorized_user","client_id":"c","client_secret":"s","refresh_token":"refresh"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", file)
	defer func(o []option.ClientOption) { gcsEndpoint = o }(gcsEndpoint)
	gcsEndpoint = []option.ClientOption{option.WithEndpoint(srv.URL + "/storage/v1/")}

	openCtx, cancelOpen := context.WithCancel(context.Background())
	s, err := Open(openCtx, "gs://bkt/ds")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	cancelOpen()

	for i := range 2 {
		if i > 0 {
			time.Sleep(1100 * time.Millisecond)
		}
		f, err := s.Open(context.Background(), "obj")
		if err != nil {
			t.Fatalf("Open %d after the opening context ended: %v", i, err)
		}
		got := make([]byte, 1000)
		if _, err := f.ReadAt(got, 100); err != nil || !bytes.Equal(got, data[100:1100]) {
			t.Fatalf("ReadAt %d after the opening context ended: %v", i, err)
		}
	}
	if n := exchanges.Load(); n < 2 {
		t.Fatalf("%d token exchanges, want a refresh after the first", n)
	}

	time.Sleep(1100 * time.Millisecond)
	block.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := s.Open(ctx, "obj"); done <- err }()
	select {
	case <-arrived:
	case <-time.After(10 * time.Second):
		t.Fatal("no token exchange")
	}
	cancel()
	select {
	case <-cancelled:
	case <-time.After(10 * time.Second):
		tokens.CloseClientConnections() // so the cleanup does not wait on the handler
		t.Fatal("cancelling the request did not cancel its token exchange")
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("Open = %v, want context.Canceled", err)
	}
}
