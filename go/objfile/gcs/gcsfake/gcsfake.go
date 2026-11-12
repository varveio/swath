// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package gcsfake is an in-process fake of the Google Cloud Storage calls
// the objfile packages make, for tests: object metadata and deletion on the
// JSON API, reads on the XML API (of the whole object, of one closed byte
// range, or of the rest from a byte), and uploads on the JSON upload API,
// single-request (multipart) and resumable, with the generation
// preconditions (ifGenerationMatch) evaluated atomically when an object is
// created. It keeps each object's live generation only, as a bucket without
// object versioning does.
//
// It is a fake, not the service: it checks what the clients send against
// the documented protocol, and gives tests the faults they need (a failed
// or lost response at a named step, a slow chunk), nothing more.
package gcsfake

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Steps a Fault sees, in the order an upload meets them.
const (
	OpMetadata = "metadata" // GET of an object's JSON metadata
	OpRead     = "read"     // ranged XML read
	OpDelete   = "delete"   // JSON delete
	OpSession  = "session"  // a resumable upload's initiation
	OpChunk    = "chunk"    // a resumable upload's chunk that does not complete it
	OpCreate   = "create"   // the request that creates an object: a multipart upload, or a resumable upload's last chunk
)

// Fault decides one request's fate before (after false) and after (after
// true) the fake acts on it: a status other than 0 is sent instead of the
// fake's answer. Before, nothing is done; after, the request has taken
// effect and its response is lost (the client sees the status instead).
type Fault func(op, name string, after bool) (status int)

// Server is the fake. Its zero value is not usable; New makes one.
type Server struct {
	Bucket string

	mu       sync.Mutex
	gen      int64
	objects  map[string]Object
	sessions map[string]*session
	nextID   int
	fault    Fault
	counts   map[string]int64
	uploaded int64 // bytes received in upload bodies (retransmits included)
}

// Object is one stored object.
type Object struct {
	Gen  int64
	Data []byte
}

type session struct {
	name     string
	cond     *int64 // ifGenerationMatch
	data     []byte
	complete bool
}

// New returns a fake of bucket.
func New(bucket string) *Server {
	return &Server{Bucket: bucket, objects: map[string]Object{}, sessions: map[string]*session{}, counts: map[string]int64{}}
}

// SetFault installs f (nil: none).
func (s *Server) SetFault(f Fault) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fault = f
}

// Put creates or replaces name, as another writer would, and returns its
// generation.
func (s *Server) Put(name string, data []byte) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gen++
	s.objects[name] = Object{Gen: s.gen, Data: bytes.Clone(data)}
	return s.gen
}

// Remove deletes name, as another client would.
func (s *Server) Remove(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, name)
}

// Get returns name's live object.
func (s *Server) Get(name string) (Object, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.objects[name]
	return o, ok
}

// Names returns the live objects' names, sorted.
func (s *Server) Names() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n []string
	for k := range s.objects {
		n = append(n, k)
	}
	sort.Strings(n)
	return n
}

// OpenSessions is the resumable uploads started and not completed.
func (s *Server) OpenSessions() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, x := range s.sessions {
		if !x.complete {
			n++
		}
	}
	return n
}

// Count is the requests of op answered so far (faulted ones included).
func (s *Server) Count(op string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counts[op]
}

// UploadedBytes is the media bytes received, retransmits included.
func (s *Server) UploadedBytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.uploaded
}

func jsonError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": code, "message": msg}})
}

// faulted applies the fault to op before or after it; true when it answered.
func (s *Server) faulted(w http.ResponseWriter, op, name string, after bool) bool {
	if !after {
		s.counts[op]++
	}
	if s.fault == nil {
		return false
	}
	if code := s.fault(op, name, after); code != 0 {
		jsonError(w, code, "injected fault")
		return true
	}
	return false
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	jsonPrefix := "/storage/v1/b/" + s.Bucket + "/o/"
	uploadPath := "/upload/storage/v1/b/" + s.Bucket + "/o"
	switch {
	case strings.HasPrefix(r.URL.Path, jsonPrefix):
		name, err := url.PathUnescape(strings.TrimPrefix(r.URL.EscapedPath(), jsonPrefix))
		if err != nil {
			jsonError(w, http.StatusBadRequest, "bad name")
			return
		}
		switch r.Method {
		case http.MethodGet:
			s.metadata(w, r, name)
		case http.MethodDelete:
			s.delete(w, r, name)
		default:
			jsonError(w, http.StatusMethodNotAllowed, r.Method)
		}
	case r.URL.Path == uploadPath && r.URL.Query().Get("upload_id") != "":
		s.chunk(w, r)
	case r.URL.Path == uploadPath:
		switch r.URL.Query().Get("uploadType") {
		case "multipart":
			s.multipart(w, r)
		case "resumable":
			s.initiate(w, r)
		default:
			jsonError(w, http.StatusBadRequest, "uploadType")
		}
	default:
		s.read(w, r)
	}
}

func (s *Server) resource(name string, o Object) map[string]string {
	sum := crc32.Checksum(o.Data, crc32.MakeTable(crc32.Castagnoli))
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], sum)
	return map[string]string{"bucket": s.Bucket, "name": name, "size": strconv.Itoa(len(o.Data)),
		"generation": strconv.FormatInt(o.Gen, 10), "metageneration": "1", "crc32c": base64.StdEncoding.EncodeToString(b[:])}
}

func (s *Server) metadata(w http.ResponseWriter, r *http.Request, name string) {
	if s.faulted(w, OpMetadata, name, false) {
		return
	}
	o, found := s.objects[name]
	if g := r.URL.Query().Get("generation"); found && g != "" && g != strconv.FormatInt(o.Gen, 10) {
		found = false
	}
	if !found {
		jsonError(w, http.StatusNotFound, "No such object")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.resource(name, o))
}

// precondition reports whether ifGenerationMatch (q's) holds for name.
func (s *Server) precondition(name string, cond *int64) bool {
	if cond == nil {
		return true
	}
	o, found := s.objects[name]
	if *cond == 0 {
		return !found
	}
	return found && o.Gen == *cond
}

func generationMatch(q url.Values) (*int64, bool) {
	v := q.Get("ifGenerationMatch")
	if v == "" {
		return nil, true
	}
	g, err := strconv.ParseInt(v, 10, 64)
	return &g, err == nil
}

func (s *Server) delete(w http.ResponseWriter, r *http.Request, name string) {
	if s.faulted(w, OpDelete, name, false) {
		return
	}
	q := r.URL.Query()
	o, found := s.objects[name]
	if g := q.Get("generation"); found && g != "" && g != strconv.FormatInt(o.Gen, 10) {
		found = false
	}
	if !found {
		jsonError(w, http.StatusNotFound, "No such object")
		return
	}
	cond, ok := generationMatch(q)
	if !ok {
		jsonError(w, http.StatusBadRequest, "ifGenerationMatch")
		return
	}
	if !s.precondition(name, cond) {
		jsonError(w, http.StatusPreconditionFailed, "conditionNotMet")
		return
	}
	delete(s.objects, name)
	if s.faulted(w, OpDelete, name, true) {
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// create installs data as name if cond holds: the atomic step of every
// upload. It reports whether it created the object.
func (s *Server) create(w http.ResponseWriter, name string, cond *int64, data []byte, crc string) bool {
	if crc != "" {
		sum := crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], sum)
		if base64.StdEncoding.EncodeToString(b[:]) != crc {
			jsonError(w, http.StatusBadRequest, "crc32c mismatch")
			return false
		}
	}
	if !s.precondition(name, cond) {
		jsonError(w, http.StatusPreconditionFailed, "conditionNotMet")
		return false
	}
	s.gen++
	o := Object{Gen: s.gen, Data: bytes.Clone(data)}
	s.objects[name] = o
	if !s.faulted(w, OpCreate, name, true) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(s.resource(name, o))
	}
	return true
}

func (s *Server) multipart(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	cond, ok := generationMatch(q)
	if !ok {
		jsonError(w, http.StatusBadRequest, "ifGenerationMatch")
		return
	}
	mt, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || !strings.HasPrefix(mt, "multipart/") {
		jsonError(w, http.StatusBadRequest, "not multipart")
		return
	}
	mr := multipart.NewReader(r.Body, params["boundary"])
	var meta struct {
		Name   string `json:"name"`
		CRC32C string `json:"crc32c"`
	}
	p, err := mr.NextPart()
	if err == nil {
		err = json.NewDecoder(p).Decode(&meta)
	}
	var data []byte
	if err == nil {
		if p, err = mr.NextPart(); err == nil {
			data, err = io.ReadAll(p)
		}
	}
	if err != nil {
		jsonError(w, http.StatusBadRequest, "multipart body: "+err.Error())
		return
	}
	if meta.Name == "" {
		meta.Name = q.Get("name")
	}
	if s.faulted(w, OpCreate, meta.Name, false) {
		return
	}
	s.uploaded += int64(len(data))
	_ = s.create(w, meta.Name, cond, data, meta.CRC32C)
}

func (s *Server) initiate(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	cond, ok := generationMatch(q)
	if !ok {
		jsonError(w, http.StatusBadRequest, "ifGenerationMatch")
		return
	}
	var meta struct {
		Name string `json:"name"`
	}
	_ = json.NewDecoder(r.Body).Decode(&meta)
	if meta.Name == "" {
		meta.Name = q.Get("name")
	}
	if s.faulted(w, OpSession, meta.Name, false) {
		return
	}
	// The service checks the precondition when the session starts, and
	// again when it creates the object.
	if !s.precondition(meta.Name, cond) {
		jsonError(w, http.StatusPreconditionFailed, "conditionNotMet")
		return
	}
	s.nextID++
	id := strconv.Itoa(s.nextID)
	s.sessions[id] = &session{name: meta.Name, cond: cond}
	if s.faulted(w, OpSession, meta.Name, true) {
		return
	}
	w.Header().Set("Location", "http://"+r.Host+"/upload/storage/v1/b/"+s.Bucket+"/o?uploadType=resumable&upload_id="+id)
	w.WriteHeader(http.StatusOK)
}

// chunk takes one request of a resumable session: Content-Range
// "bytes A-B/*" (more follows), "bytes A-B/TOTAL" or "bytes */TOTAL" (the
// last). A chunk sent again (A before what the session holds) is taken
// from where it left off.
func (s *Server) chunk(w http.ResponseWriter, r *http.Request) {
	x := s.sessions[r.URL.Query().Get("upload_id")]
	if x == nil {
		jsonError(w, http.StatusNotFound, "no such upload")
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		jsonError(w, http.StatusBadRequest, "body")
		return
	}
	rng, ok := strings.CutPrefix(r.Header.Get("Content-Range"), "bytes ")
	span, total, ok2 := strings.Cut(rng, "/")
	if !ok || !ok2 {
		jsonError(w, http.StatusBadRequest, "Content-Range")
		return
	}
	final := total != "*"
	op := OpChunk
	if final {
		op = OpCreate
	}
	if s.faulted(w, op, x.name, false) {
		return
	}
	if x.complete {
		// A completed session answers with its object, as the service
		// does for a retried last request.
		if o, ok := s.objects[x.name]; ok {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(s.resource(x.name, o))
			return
		}
		jsonError(w, http.StatusNotFound, "upload completed and object gone")
		return
	}
	s.uploaded += int64(len(body))
	if span != "*" {
		a, b, ok := strings.Cut(span, "-")
		first, err1 := strconv.ParseInt(a, 10, 64)
		last, err2 := strconv.ParseInt(b, 10, 64)
		if !ok || err1 != nil || err2 != nil || last-first+1 != int64(len(body)) || first > int64(len(x.data)) {
			jsonError(w, http.StatusBadRequest, "chunk range "+span)
			return
		}
		x.data = append(x.data, body[int64(len(x.data))-first:]...)
	}
	if !final {
		w.Header().Set("X-Http-Status-Code-Override", "308")
		if len(x.data) > 0 {
			w.Header().Set("Range", fmt.Sprintf("bytes=0-%d", len(x.data)-1))
		}
		if s.faulted(w, OpChunk, x.name, true) {
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	}
	if n, err := strconv.ParseInt(total, 10, 64); err != nil || n != int64(len(x.data)) {
		jsonError(w, http.StatusBadRequest, "total "+total)
		return
	}
	crc := ""
	if h, ok := strings.CutPrefix(r.Header.Get("X-Goog-Hash"), "crc32c="); ok {
		crc = h
	}
	x.complete = s.create(w, x.name, x.cond, x.data, crc)
}

func (s *Server) read(w http.ResponseWriter, r *http.Request) {
	name, ok := strings.CutPrefix(r.URL.Path, "/"+s.Bucket+"/")
	if s.faulted(w, OpRead, name, false) {
		return
	}
	o, found := s.objects[name]
	if gen := r.URL.Query().Get("generation"); !ok || !found || gen != "" && gen != strconv.FormatInt(o.Gen, 10) {
		http.Error(w, "NoSuchKey", http.StatusNotFound)
		return
	}
	w.Header().Set("X-Goog-Generation", strconv.FormatInt(o.Gen, 10))
	// The body is object bytes; never let a client take it for a page.
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	rng := r.Header.Get("Range")
	if rng == "" { // the whole object
		w.Header().Set("Content-Length", strconv.Itoa(len(o.Data)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(o.Data)
		return
	}
	start, end := 0, len(o.Data)-1
	if open, ok := strings.CutSuffix(rng, "-"); ok { // to the end
		if _, err := fmt.Sscanf(open, "bytes=%d", &start); err != nil {
			start = len(o.Data)
		}
	} else if _, err := fmt.Sscanf(rng, "bytes=%d-%d", &start, &end); err != nil {
		start = len(o.Data)
	}
	if start < 0 || start >= len(o.Data) || end < start {
		http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
		return
	}
	end = min(end, len(o.Data)-1) // a range past the end is clipped, as the store does
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(o.Data)))
	w.Header().Set("Content-Length", strconv.Itoa(end-start+1))
	w.WriteHeader(http.StatusPartialContent)
	_, _ = w.Write(o.Data[start : end+1])
}
