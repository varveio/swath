// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package s3fake is an in-process fake of the S3 calls the objfile
// packages make, for tests: HeadObject, ranged GetObject with If-Match and
// a version id, PutObject, the multipart calls (create, upload part,
// complete, abort, list parts), and DeleteObject with If-Match or a version id.
// If-None-Match: * on PutObject and on CompleteMultipartUpload is evaluated
// atomically with the creation, so of two creators of one name exactly one
// succeeds. The bucket is addressed path-style (/BUCKET/KEY); a request
// body in aws-chunked encoding (a streamed upload with a trailing
// checksum) is decoded.
//
// It is a fake, not the service. It checks what clients send against the
// documented protocol (part numbers, part ETags, the minimum part size,
// at most 10,000 parts) and gives tests the faults they need, nothing more.
// It does not model: signatures and credentials (a request's
// Authorization is ignored), checksums (accepted, never verified), object
// metadata beyond size and ETag, lifecycle rules (an incomplete upload
// lives until aborted), S3's 409 ConditionalRequestConflict except as an
// injected fault, a 200 answer carrying an error except as an injected
// fault, a request that goes on after its client gave up except as a late
// part (LateParts), suspended versioning or delete markers, eventual
// consistency, listing objects, or anything of another bucket.
package s3fake

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Steps a Fault sees. The names that mean the same step are gcsfake's.
const (
	OpMetadata = "metadata" // HeadObject
	OpRead     = "read"     // GetObject
	OpDelete   = "delete"   // DeleteObject
	OpSession  = "session"  // CreateMultipartUpload
	OpChunk    = "chunk"    // UploadPart
	OpCreate   = "create"   // the request that creates an object: PutObject or CompleteMultipartUpload
	OpAbort    = "abort"    // AbortMultipartUpload
	OpParts    = "parts"    // ListParts
)

// MinPartSize is S3's smallest part (all but the last of an upload), and
// MaxParts its most parts an upload.
const (
	MinPartSize = 5 << 20
	MaxParts    = 10000
)

// Fault decides one request's fate before (after false) and after (after
// true) the fake acts on it: a status other than 0 is sent instead of the
// fake's answer, with S3's error code for it (409 is
// ConditionalRequestConflict). 200 is an error in a 200 answer
// (InternalError), as S3 may send for CompleteMultipartUpload. Before,
// nothing is done; after, the request has taken effect and its response
// is lost (the client sees the status).
type Fault func(op, name string, after bool) (status int)

// Server is the fake. Its zero value is not usable; New makes one.
type Server struct {
	Bucket string
	// Versioned gives every creation a version id, kept with the bytes
	// (set before the first request).
	Versioned bool
	// MinPartSize is the smallest part but the last that a completion
	// accepts (New: MinPartSize).
	MinPartSize int64
	// LateParts is the aborts still to answer 204 while the upload stays
	// listed with its parts, as a part in flight when the abort ran can
	// leave it (AWS: it may be necessary to abort an upload more than
	// once). Set it before the requests it affects.
	LateParts int

	mu       sync.Mutex
	seq      int64
	objects  map[string]*Object   // live versions
	versions map[string][]*Object // every version, oldest first (Versioned)
	uploads  map[string]*upload
	fault    Fault
	counts   map[string]int64
	uploaded int64
	newID    func(int64) string
}

// Object is one stored version.
type Object struct {
	Data    []byte
	ETag    string // quoted, as S3 sends it
	Version string // "" unless Versioned
}

type upload struct {
	name  string
	parts map[int][]byte
}

// New returns a fake of bucket.
func New(bucket string) *Server { return NewWithIDs(bucket, nil) }

// NewWithIDs returns a fake of bucket whose multipart upload ids id makes
// (n counts from 1); nil is "upload-%08d". A test gives ids with reserved
// characters, as S3's may hold.
func NewWithIDs(bucket string, id func(n int64) string) *Server {
	if id == nil {
		id = func(n int64) string { return fmt.Sprintf("upload-%08d", n) }
	}
	return &Server{Bucket: bucket, MinPartSize: MinPartSize, objects: map[string]*Object{}, versions: map[string][]*Object{},
		uploads: map[string]*upload{}, counts: map[string]int64{}, newID: id}
}

// SetFault installs f (nil: none).
func (s *Server) SetFault(f Fault) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fault = f
}

// Put creates or replaces name, as another writer would.
func (s *Server) Put(name string, data []byte) Object {
	s.mu.Lock()
	defer s.mu.Unlock()
	sum := md5.Sum(data)
	return *s.store(name, bytes.Clone(data), `"`+hex.EncodeToString(sum[:])+`"`)
}

// Remove deletes name's live version, as another client would.
func (s *Server) Remove(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, name)
}

// Get returns name's live version.
func (s *Server) Get(name string) (Object, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.objects[name]
	if !ok {
		return Object{}, false
	}
	return *o, true
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

// Uploads are the names of the multipart uploads started and neither
// completed nor aborted, sorted.
func (s *Server) Uploads() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n []string
	for _, u := range s.uploads {
		n = append(n, u.name)
	}
	sort.Strings(n)
	return n
}

// Count is the requests of op answered so far (faulted ones included).
func (s *Server) Count(op string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counts[op]
}

// UploadedBytes is the object and part bytes received, retransmits
// included.
func (s *Server) UploadedBytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.uploaded
}

func (s *Server) store(name string, data []byte, etag string) *Object {
	s.seq++
	o := &Object{Data: data, ETag: etag}
	if s.Versioned {
		o.Version = fmt.Sprintf("v%08d", s.seq)
		s.versions[name] = append(s.versions[name], o)
	}
	s.objects[name] = o
	return o
}

// codes are S3's error codes by status, for an error the fake makes up.
var codes = map[int]string{
	http.StatusBadRequest: "InvalidRequest", http.StatusNotFound: "NoSuchKey", http.StatusConflict: "ConditionalRequestConflict",
	http.StatusPreconditionFailed: "PreconditionFailed", http.StatusInternalServerError: "InternalError",
	http.StatusServiceUnavailable: "SlowDown", http.StatusRequestedRangeNotSatisfiable: "InvalidRange",
}

func s3Error(w http.ResponseWriter, r *http.Request, status int, code, msg string) {
	if code == "" {
		code = codes[status]
	}
	if code == "" {
		code = "InternalError" // also an error in a 200 answer
	}
	if r.Method == http.MethodHead {
		w.WriteHeader(status) // HEAD answers carry no body
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_ = xml.NewEncoder(w).Encode(struct {
		XMLName xml.Name `xml:"Error"`
		Code    string
		Message string
	}{Code: code, Message: msg})
}

// faulted applies the fault to op before or after it; true when it answered.
func (s *Server) faulted(w http.ResponseWriter, r *http.Request, op, name string, after bool) bool {
	if !after {
		s.counts[op]++
	}
	if s.fault == nil {
		return false
	}
	if code := s.fault(op, name, after); code != 0 {
		s3Error(w, r, code, "", "injected fault")
		return true
	}
	return false
}

// ServeHTTP answers one request of the calls the package comment lists,
// path-style; any other is refused.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	name, ok := strings.CutPrefix(r.URL.Path, "/"+s.Bucket+"/")
	if !ok || name == "" {
		s3Error(w, r, http.StatusNotFound, "NoSuchBucket", "path-style /"+s.Bucket+"/KEY only")
		return
	}
	q := r.URL.Query()
	var body []byte
	if r.Method == http.MethodPut || r.Method == http.MethodPost {
		var err error
		if body, err = readBody(r); err != nil {
			s3Error(w, r, http.StatusBadRequest, "IncompleteBody", err.Error())
			return
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, uploads := q["uploads"]
	id := q.Get("uploadId")
	switch {
	case r.Method == http.MethodHead:
		s.head(w, r, name)
	case r.Method == http.MethodGet && id == "":
		s.read(w, r, name)
	case r.Method == http.MethodGet:
		s.listParts(w, r, name, id)
	case r.Method == http.MethodPut && id != "":
		s.part(w, r, name, id, body)
	case r.Method == http.MethodPut:
		s.put(w, r, name, body)
	case r.Method == http.MethodPost && uploads:
		s.initiate(w, r, name)
	case r.Method == http.MethodPost && id != "":
		s.complete(w, r, name, id, body)
	case r.Method == http.MethodDelete && id != "":
		s.abort(w, r, name, id)
	case r.Method == http.MethodDelete:
		s.delete(w, r, name)
	default:
		s3Error(w, r, http.StatusMethodNotAllowed, "MethodNotAllowed", r.Method)
	}
}

// readBody reads r's body, decoding aws-chunked encoding (signed or not,
// with trailing headers) when the request declares it.
func readBody(r *http.Request) ([]byte, error) {
	b, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	if !strings.Contains(r.Header.Get("Content-Encoding"), "aws-chunked") && r.Header.Get("X-Amz-Decoded-Content-Length") == "" {
		return b, nil
	}
	var out []byte
	br := bufio.NewReader(bytes.NewReader(b))
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("aws-chunked: %w", err)
		}
		size, _, _ := strings.Cut(strings.TrimRight(line, "\r\n"), ";")
		n, err := strconv.ParseInt(size, 16, 64)
		if err != nil {
			return nil, fmt.Errorf("aws-chunked: chunk size %q", size)
		}
		if n == 0 {
			break // trailing headers follow; checksums are not verified
		}
		chunk := make([]byte, n+2)
		if _, err := io.ReadFull(br, chunk); err != nil {
			return nil, fmt.Errorf("aws-chunked: %w", err)
		}
		out = append(out, chunk[:n]...)
	}
	if d := r.Header.Get("X-Amz-Decoded-Content-Length"); d != "" && d != strconv.Itoa(len(out)) {
		return nil, fmt.Errorf("aws-chunked: %d bytes, declared %s", len(out), d)
	}
	return out, nil
}

// version is the version of name a request names (versionId), or the
// live one.
func (s *Server) version(r *http.Request, name string) (*Object, bool) {
	v := r.URL.Query().Get("versionId")
	if v == "" {
		o, ok := s.objects[name]
		return o, ok
	}
	for _, o := range s.versions[name] {
		if o.Version == v {
			return o, true
		}
	}
	return nil, false
}

func (s *Server) headers(w http.ResponseWriter, o *Object) {
	w.Header().Set("ETag", o.ETag)
	if o.Version != "" {
		w.Header().Set("x-amz-version-id", o.Version)
	}
}

func (s *Server) head(w http.ResponseWriter, r *http.Request, name string) {
	if s.faulted(w, r, OpMetadata, name, false) {
		return
	}
	o, ok := s.version(r, name)
	if !ok {
		s3Error(w, r, http.StatusNotFound, "", "")
		return
	}
	s.headers(w, o)
	w.Header().Set("Content-Length", strconv.Itoa(len(o.Data)))
	w.WriteHeader(http.StatusOK)
}

func (s *Server) read(w http.ResponseWriter, r *http.Request, name string) {
	if s.faulted(w, r, OpRead, name, false) {
		return
	}
	o, ok := s.version(r, name)
	switch {
	case !ok:
		s3Error(w, r, http.StatusNotFound, "", "The specified key does not exist.")
		return
	case r.Header.Get("If-Match") != "" && r.Header.Get("If-Match") != o.ETag:
		s3Error(w, r, http.StatusPreconditionFailed, "", "If-Match")
		return
	}
	s.headers(w, o)
	rng := r.Header.Get("Range")
	if rng == "" {
		w.Header().Set("Content-Length", strconv.Itoa(len(o.Data)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(o.Data)
		return
	}
	var start, end int
	if _, err := fmt.Sscanf(rng, "bytes=%d-%d", &start, &end); err != nil || start < 0 || start >= len(o.Data) || end < start {
		s3Error(w, r, http.StatusRequestedRangeNotSatisfiable, "", rng)
		return
	}
	end = min(end, len(o.Data)-1)
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(o.Data)))
	w.Header().Set("Content-Length", strconv.Itoa(end-start+1))
	w.WriteHeader(http.StatusPartialContent)
	_, _ = w.Write(o.Data[start : end+1])
}

// createOnly reports whether r's If-None-Match lets name be created:
// absent, or "*" with no live object.
func (s *Server) createOnly(w http.ResponseWriter, r *http.Request, name string) bool {
	switch inm := r.Header.Get("If-None-Match"); {
	case inm == "":
		return true
	case inm != "*":
		s3Error(w, r, http.StatusNotImplemented, "NotImplemented", "If-None-Match takes * only")
		return false
	}
	if _, ok := s.objects[name]; ok {
		s3Error(w, r, http.StatusPreconditionFailed, "", "At least one of the pre-conditions you specified did not hold")
		return false
	}
	return true
}

func (s *Server) put(w http.ResponseWriter, r *http.Request, name string, body []byte) {
	s.uploaded += int64(len(body))
	if s.faulted(w, r, OpCreate, name, false) || !s.createOnly(w, r, name) {
		return
	}
	sum := md5.Sum(body)
	o := s.store(name, body, `"`+hex.EncodeToString(sum[:])+`"`)
	if s.faulted(w, r, OpCreate, name, true) {
		return
	}
	s.headers(w, o)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) initiate(w http.ResponseWriter, r *http.Request, name string) {
	if s.faulted(w, r, OpSession, name, false) {
		return
	}
	s.seq++
	id := s.newID(s.seq)
	s.uploads[id] = &upload{name: name, parts: map[int][]byte{}}
	if s.faulted(w, r, OpSession, name, true) {
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	_ = xml.NewEncoder(w).Encode(struct {
		XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
		Bucket   string
		Key      string
		UploadId string
	}{Bucket: s.Bucket, Key: name, UploadId: id})
}

// session is upload id of name, or an answer of NoSuchUpload.
func (s *Server) session(w http.ResponseWriter, r *http.Request, name, id string) *upload {
	u, ok := s.uploads[id]
	if !ok || u.name != name {
		s3Error(w, r, http.StatusNotFound, "NoSuchUpload", "The specified upload does not exist.")
		return nil
	}
	return u
}

func (s *Server) part(w http.ResponseWriter, r *http.Request, name, id string, body []byte) {
	s.uploaded += int64(len(body))
	if s.faulted(w, r, OpChunk, name, false) {
		return
	}
	u := s.session(w, r, name, id)
	if u == nil {
		return
	}
	n, err := strconv.Atoi(r.URL.Query().Get("partNumber"))
	if err != nil || n < 1 || n > MaxParts {
		s3Error(w, r, http.StatusBadRequest, "InvalidArgument", "partNumber")
		return
	}
	u.parts[n] = body
	if s.faulted(w, r, OpChunk, name, true) {
		return
	}
	sum := md5.Sum(body)
	w.Header().Set("ETag", `"`+hex.EncodeToString(sum[:])+`"`)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) complete(w http.ResponseWriter, r *http.Request, name, id string, body []byte) {
	if s.faulted(w, r, OpCreate, name, false) {
		return
	}
	u := s.session(w, r, name, id)
	if u == nil {
		return
	}
	var req struct {
		Parts []struct {
			PartNumber int
			ETag       string
		} `xml:"Part"`
	}
	if err := xml.Unmarshal(body, &req); err != nil || len(req.Parts) == 0 || len(req.Parts) > MaxParts {
		s3Error(w, r, http.StatusBadRequest, "MalformedXML", "CompleteMultipartUpload")
		return
	}
	var data, sums []byte
	for i, p := range req.Parts {
		b, ok := u.parts[p.PartNumber]
		sum := md5.Sum(b)
		switch {
		case i > 0 && p.PartNumber <= req.Parts[i-1].PartNumber:
			s3Error(w, r, http.StatusBadRequest, "InvalidPartOrder", "parts out of order")
			return
		case !ok || p.ETag != `"`+hex.EncodeToString(sum[:])+`"`:
			s3Error(w, r, http.StatusBadRequest, "InvalidPart", fmt.Sprintf("part %d", p.PartNumber))
			return
		case i < len(req.Parts)-1 && int64(len(b)) < s.MinPartSize:
			s3Error(w, r, http.StatusBadRequest, "EntityTooSmall", fmt.Sprintf("part %d is %d bytes", p.PartNumber, len(b)))
			return
		}
		data = append(data, b...)
		sums = append(sums, sum[:]...)
	}
	if mpu := r.Header.Get("X-Amz-Mp-Object-Size"); mpu != "" && mpu != strconv.Itoa(len(data)) {
		s3Error(w, r, http.StatusBadRequest, "InvalidRequest", "x-amz-mp-object-size")
		return
	}
	// The precondition and the creation, under one lock: atomic. A
	// refused completion leaves the upload, as S3 does, for an abort.
	if !s.createOnly(w, r, name) {
		return
	}
	sum := md5.Sum(sums)
	o := s.store(name, data, fmt.Sprintf(`"%s-%d"`, hex.EncodeToString(sum[:]), len(req.Parts)))
	delete(s.uploads, id)
	if s.faulted(w, r, OpCreate, name, true) {
		return
	}
	if o.Version != "" {
		w.Header().Set("x-amz-version-id", o.Version)
	}
	w.Header().Set("Content-Type", "application/xml")
	_ = xml.NewEncoder(w).Encode(struct {
		XMLName xml.Name `xml:"CompleteMultipartUploadResult"`
		Bucket  string
		Key     string
		ETag    string
	}{Bucket: s.Bucket, Key: name, ETag: o.ETag})
}

func (s *Server) abort(w http.ResponseWriter, r *http.Request, name, id string) {
	if s.faulted(w, r, OpAbort, name, false) {
		return
	}
	if s.session(w, r, name, id) == nil {
		return
	}
	if s.LateParts > 0 {
		s.LateParts-- // a late part keeps the upload
	} else {
		delete(s.uploads, id)
	}
	if s.faulted(w, r, OpAbort, name, true) {
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) listParts(w http.ResponseWriter, r *http.Request, name, id string) {
	if s.faulted(w, r, OpParts, name, false) {
		return
	}
	u := s.session(w, r, name, id)
	if u == nil {
		return
	}
	type part struct {
		PartNumber int
		ETag       string
		Size       int
	}
	res := struct {
		XMLName  xml.Name `xml:"ListPartsResult"`
		Bucket   string
		Key      string
		UploadId string
		Parts    []part `xml:"Part"`
	}{Bucket: s.Bucket, Key: name, UploadId: id}
	nums := make([]int, 0, len(u.parts))
	for n := range u.parts {
		nums = append(nums, n)
	}
	sort.Ints(nums)
	for _, n := range nums {
		sum := md5.Sum(u.parts[n])
		res.Parts = append(res.Parts, part{PartNumber: n, ETag: `"` + hex.EncodeToString(sum[:]) + `"`, Size: len(u.parts[n])})
	}
	w.Header().Set("Content-Type", "application/xml")
	_ = xml.NewEncoder(w).Encode(res)
}

// delete removes a version by its id (versionId), or the live object
// if its If-Match holds; a missing object is 404 when If-Match is given,
// as for a conditional write, and 204 otherwise.
func (s *Server) delete(w http.ResponseWriter, r *http.Request, name string) {
	if s.faulted(w, r, OpDelete, name, false) {
		return
	}
	if v := r.URL.Query().Get("versionId"); v != "" {
		vs := s.versions[name]
		for i, o := range vs {
			if o.Version == v {
				s.versions[name] = append(vs[:i:i], vs[i+1:]...)
				if s.objects[name] == o {
					delete(s.objects, name)
					if n := len(s.versions[name]); n > 0 {
						s.objects[name] = s.versions[name][n-1]
					}
				}
				break
			}
		}
	} else {
		o, ok := s.objects[name]
		im := r.Header.Get("If-Match")
		switch {
		case im != "" && !ok:
			s3Error(w, r, http.StatusNotFound, "", "The specified key does not exist.")
			return
		case im != "" && im != "*" && im != o.ETag:
			s3Error(w, r, http.StatusPreconditionFailed, "", "If-Match")
			return
		}
		delete(s.objects, name)
	}
	if s.faulted(w, r, OpDelete, name, true) {
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
