// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package azfake is an in-process fake of the Azure Blob Storage calls the
// objfile packages make on block blobs, for tests: Get Blob Properties, a
// ranged Get Blob with If-Match, Put Blob, Put Block, Put Block List, and
// Delete Blob with If-Match. If-None-Match: * on Put Blob and on Put Block
// List is evaluated atomically with the creation, so of two creators of
// one name exactly one succeeds. Blobs are addressed as an emulator
// addresses them, the account first: /ACCOUNT/CONTAINER/NAME.
//
// It is a fake, not the service. It checks what clients send against the
// documented protocol (block ids Base64, at most 64 bytes decoded and of
// one length a blob; at most MaxBlocks blocks committed; the blocks a list
// names staged) and gives tests the faults they need, nothing more. As the
// service does, it keeps a name's uncommitted blocks apart from its blob
// (they are never read, listed or counted as a blob) and discards them
// when a blob is created at the name. It does not model: authorization (a
// request's credentials, a SAS included, are ignored), transactional
// checksums other than x-ms-content-crc64 (which is verified), the
// collection of uncommitted blocks a week after their last change, blob
// versioning, soft delete, snapshots, leases, access tiers, metadata
// beyond size and ETag, listing, or anything of another container.
package azfake

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/xml"
	"fmt"
	"hash/crc64"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Steps a Fault sees. The names that mean the same step are gcsfake's and
// s3fake's.
const (
	OpMetadata = "metadata" // Get Blob Properties
	OpRead     = "read"     // Get Blob
	OpDelete   = "delete"   // Delete Blob
	OpChunk    = "chunk"    // Put Block
	OpCreate   = "create"   // the request that creates a blob: Put Blob or Put Block List
)

// MaxBlocks is the most blocks a block blob holds, and maxBlockID the
// longest block id, decoded.
const (
	MaxBlocks  = 50000
	maxBlockID = 64
)

// crcTable is Azure Storage's CRC64.
var crcTable = crc64.MakeTable(0x9A6C9329AC4BC9B5)

// CRC64 is data's x-ms-content-crc64: its CRC64, little-endian, Base64.
func CRC64(data []byte) string {
	return base64.StdEncoding.EncodeToString(binary.LittleEndian.AppendUint64(nil, crc64.Checksum(data, crcTable)))
}

// Fault decides one request's fate before (after false) and after (after
// true) the fake acts on it: a status other than 0 is sent instead of the
// fake's answer, with the service's error code for it. Before, nothing is
// done; after, the request has taken effect and its response is lost (the
// client sees the status).
type Fault func(op, name string, after bool) (status int)

// Server is the fake. Its zero value is not usable; New makes one.
type Server struct {
	Account   string
	Container string

	mu       sync.Mutex
	seq      int64
	blobs    map[string]Blob
	staged   map[string]map[string][]byte // uncommitted blocks by name, then block id
	fault    Fault
	counts   map[string]int64
	uploaded int64
}

// Blob is one committed blob.
type Blob struct {
	Data []byte
	ETag string // quoted, as the service sends it
}

// New returns a fake of container in account.
func New(account, container string) *Server {
	return &Server{Account: account, Container: container, blobs: map[string]Blob{}, staged: map[string]map[string][]byte{}, counts: map[string]int64{}}
}

// SetFault installs f (nil: none).
func (s *Server) SetFault(f Fault) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fault = f
}

// Put creates or replaces name, as another writer would (a new ETag, even
// for the same bytes).
func (s *Server) Put(name string, data []byte) Blob {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.store(name, append([]byte(nil), data...))
}

// Remove deletes name, as another client would.
func (s *Server) Remove(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.blobs, name)
}

// Get returns name's blob.
func (s *Server) Get(name string) (Blob, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.blobs[name]
	return b, ok
}

// Names returns the blobs' names, sorted: a name with uncommitted blocks
// only is not a blob.
func (s *Server) Names() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n []string
	for k := range s.blobs {
		n = append(n, k)
	}
	sort.Strings(n)
	return n
}

// Uncommitted is the uncommitted blocks staged under name.
func (s *Server) Uncommitted(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.staged[name])
}

// Staging are the names holding uncommitted blocks, sorted.
func (s *Server) Staging() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n []string
	for k, blocks := range s.staged {
		if len(blocks) > 0 {
			n = append(n, k)
		}
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

// UploadedBytes is the blob and block bytes received, retransmits
// included.
func (s *Server) UploadedBytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.uploaded
}

// store creates or replaces name, discarding its uncommitted blocks.
func (s *Server) store(name string, data []byte) Blob {
	s.seq++
	b := Blob{Data: data, ETag: fmt.Sprintf(`"0x8DE%013X"`, s.seq)}
	s.blobs[name] = b
	delete(s.staged, name)
	return b
}

// codes are the service's error codes by status, for an error the fake
// makes up.
var codes = map[int]string{
	http.StatusBadRequest: "InvalidInput", http.StatusForbidden: "AuthorizationFailure", http.StatusNotFound: "BlobNotFound",
	http.StatusConflict: "BlobAlreadyExists", http.StatusPreconditionFailed: "ConditionNotMet", http.StatusInternalServerError: "InternalError",
	http.StatusServiceUnavailable: "ServerBusy", http.StatusRequestedRangeNotSatisfiable: "InvalidRange",
}

func fail(w http.ResponseWriter, r *http.Request, status int, code, msg string) {
	if code == "" {
		code = codes[status]
	}
	if code == "" {
		code = "InternalError"
	}
	w.Header().Set("x-ms-error-code", code)
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
		fail(w, r, code, "", "injected fault")
		return true
	}
	return false
}

// ServeHTTP answers one request of the calls the package comment lists;
// any other is refused.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	name, ok := strings.CutPrefix(r.URL.Path, "/"+s.Account+"/"+s.Container+"/")
	if !ok || name == "" {
		fail(w, r, http.StatusNotFound, "ContainerNotFound", "/"+s.Account+"/"+s.Container+"/NAME only")
		return
	}
	var body []byte
	if r.Method == http.MethodPut {
		var err error
		if body, err = io.ReadAll(r.Body); err != nil {
			fail(w, r, http.StatusBadRequest, "InvalidInput", err.Error())
			return
		}
		if sum := r.Header.Get("x-ms-content-crc64"); sum != "" && sum != CRC64(body) {
			fail(w, r, http.StatusBadRequest, "Crc64Mismatch", "x-ms-content-crc64")
			return
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch comp := r.URL.Query().Get("comp"); {
	case r.Method == http.MethodHead:
		s.properties(w, r, name)
	case r.Method == http.MethodGet && comp == "":
		s.read(w, r, name)
	case r.Method == http.MethodPut && comp == "":
		s.putBlob(w, r, name, body)
	case r.Method == http.MethodPut && comp == "block":
		s.putBlock(w, r, name, body)
	case r.Method == http.MethodPut && comp == "blocklist":
		s.putBlockList(w, r, name, body)
	case r.Method == http.MethodDelete && comp == "":
		s.delete(w, r, name)
	default:
		fail(w, r, http.StatusMethodNotAllowed, "UnsupportedHttpVerb", r.Method+" comp="+comp)
	}
}

func headers(w http.ResponseWriter, b Blob) {
	w.Header().Set("ETag", b.ETag)
	w.Header().Set("Last-Modified", time.Unix(0, 0).UTC().Format(http.TimeFormat))
	w.Header().Set("x-ms-blob-type", "BlockBlob")
}

func (s *Server) properties(w http.ResponseWriter, r *http.Request, name string) {
	if s.faulted(w, r, OpMetadata, name, false) {
		return
	}
	b, ok := s.blobs[name]
	if !ok {
		fail(w, r, http.StatusNotFound, "", "")
		return
	}
	headers(w, b)
	w.Header().Set("Content-Length", strconv.Itoa(len(b.Data)))
	w.WriteHeader(http.StatusOK)
}

func (s *Server) read(w http.ResponseWriter, r *http.Request, name string) {
	if s.faulted(w, r, OpRead, name, false) {
		return
	}
	b, ok := s.blobs[name]
	switch {
	case !ok:
		fail(w, r, http.StatusNotFound, "", "The specified blob does not exist.")
		return
	case r.Header.Get("If-Match") != "" && r.Header.Get("If-Match") != b.ETag:
		fail(w, r, http.StatusPreconditionFailed, "", "If-Match")
		return
	}
	headers(w, b)
	rng := r.Header.Get("x-ms-range")
	if rng == "" {
		rng = r.Header.Get("Range")
	}
	if rng == "" {
		w.Header().Set("Content-Length", strconv.Itoa(len(b.Data)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(b.Data)
		return
	}
	var start, end int
	if _, err := fmt.Sscanf(rng, "bytes=%d-%d", &start, &end); err != nil || start < 0 || start >= len(b.Data) || end < start {
		fail(w, r, http.StatusRequestedRangeNotSatisfiable, "", rng)
		return
	}
	end = min(end, len(b.Data)-1) // a range past the end is clipped, as the service does
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(b.Data)))
	w.Header().Set("Content-Length", strconv.Itoa(end-start+1))
	w.WriteHeader(http.StatusPartialContent)
	_, _ = w.Write(b.Data[start : end+1])
}

// createOnly reports whether r's If-None-Match lets name be created:
// absent, or "*" with no blob (the service answers 409 BlobAlreadyExists).
func (s *Server) createOnly(w http.ResponseWriter, r *http.Request, name string) bool {
	switch inm := r.Header.Get("If-None-Match"); {
	case inm == "":
		return true
	case inm != "*":
		fail(w, r, http.StatusNotImplemented, "NotImplemented", "If-None-Match takes * only")
		return false
	}
	if _, ok := s.blobs[name]; ok {
		fail(w, r, http.StatusConflict, "BlobAlreadyExists", "The specified blob already exists.")
		return false
	}
	return true
}

// created answers a creation of b.
func created(w http.ResponseWriter, b Blob) {
	headers(w, b)
	w.WriteHeader(http.StatusCreated)
}

func (s *Server) putBlob(w http.ResponseWriter, r *http.Request, name string, body []byte) {
	s.uploaded += int64(len(body))
	if s.faulted(w, r, OpCreate, name, false) {
		return
	}
	if r.Header.Get("x-ms-blob-type") != "BlockBlob" {
		fail(w, r, http.StatusBadRequest, "InvalidHeaderValue", "x-ms-blob-type")
		return
	}
	if !s.createOnly(w, r, name) {
		return
	}
	b := s.store(name, body)
	if s.faulted(w, r, OpCreate, name, true) {
		return
	}
	created(w, b)
}

func (s *Server) putBlock(w http.ResponseWriter, r *http.Request, name string, body []byte) {
	s.uploaded += int64(len(body))
	if s.faulted(w, r, OpChunk, name, false) {
		return
	}
	id := r.URL.Query().Get("blockid")
	raw, err := base64.StdEncoding.DecodeString(id)
	if err != nil || len(raw) == 0 || len(raw) > maxBlockID {
		fail(w, r, http.StatusBadRequest, "InvalidQueryParameterValue", "blockid")
		return
	}
	blocks := s.staged[name]
	for other := range blocks {
		if len(other) != len(id) {
			fail(w, r, http.StatusBadRequest, "InvalidBlobOrBlock", "The specified blob or block content is invalid.")
			return
		}
		break
	}
	if blocks == nil {
		blocks = map[string][]byte{}
		s.staged[name] = blocks
	}
	blocks[id] = body
	if s.faulted(w, r, OpChunk, name, true) {
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (s *Server) putBlockList(w http.ResponseWriter, r *http.Request, name string, body []byte) {
	if s.faulted(w, r, OpCreate, name, false) {
		return
	}
	var req struct {
		Latest      []string `xml:"Latest"`
		Committed   []string `xml:"Committed"`
		Uncommitted []string `xml:"Uncommitted"`
	}
	if err := xml.Unmarshal(body, &req); err != nil || len(req.Committed)+len(req.Uncommitted) > 0 {
		fail(w, r, http.StatusBadRequest, "InvalidXmlDocument", "Latest blocks only")
		return
	}
	if len(req.Latest) > MaxBlocks {
		fail(w, r, http.StatusBadRequest, "BlockListTooLong", "The block list may not contain more than 50,000 blocks.")
		return
	}
	// The precondition and the creation, under one lock: atomic. A refused
	// list leaves the blocks staged, as the service does.
	if !s.createOnly(w, r, name) {
		return
	}
	var data []byte
	for _, id := range req.Latest {
		b, ok := s.staged[name][id]
		if !ok {
			fail(w, r, http.StatusBadRequest, "InvalidBlockList", "The specified block list is invalid.")
			return
		}
		data = append(data, b...)
	}
	b := s.store(name, data)
	if s.faulted(w, r, OpCreate, name, true) {
		return
	}
	created(w, b)
}

// delete removes name if its If-Match holds.
func (s *Server) delete(w http.ResponseWriter, r *http.Request, name string) {
	if s.faulted(w, r, OpDelete, name, false) {
		return
	}
	b, ok := s.blobs[name]
	switch im := r.Header.Get("If-Match"); {
	case !ok:
		fail(w, r, http.StatusNotFound, "", "The specified blob does not exist.")
		return
	case im != "" && im != "*" && im != b.ETag:
		fail(w, r, http.StatusPreconditionFailed, "", "If-Match")
		return
	}
	delete(s.blobs, name)
	if s.faulted(w, r, OpDelete, name, true) {
		return
	}
	w.WriteHeader(http.StatusAccepted)
}
