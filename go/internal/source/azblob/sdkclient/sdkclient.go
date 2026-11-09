// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package sdkclient lists Azure Blob containers through the official SDK
// (azblob's service and container clients): one pager seeded with the
// marker and one NextPage per attempt, on the hand-rolled client's core
// (client.Core: account root, transport pool, credentials, retry policy,
// counters, the body-cap halving, answers as *client.Error, SAS
// redaction), so the provider sees the same API, routes, errors and
// counters.
//
// The SDK sends; swath owns the rest. azcore's retry policy is off
// (MaxRetries -1: objhttp.Do retries); the NoCredential pipeline has no
// auth policy, so an Entra ID token is fetched before the attempt
// (Core.Token) and sent as a request header (policy.WithHTTPHeader), never
// through azcore's bearer policy and its 401 challenge re-send; Shared Key
// is the SDK's own signing policy. Every response passes an objhttp.Guard
// below the SDK (64 MiB success bodies, past which List halves
// maxresults; 64 KiB error bodies; the original body closed), and redirects
// are answers, never followed, their Location never read (Core.Pool).
//
// Custom code is kept where a test shows the SDK alone breaks a contract,
// each piece once:
//
//   - pin, a per-call policy (before the retry and signing policies, so
//     Shared Key signs what it sets): the service version (client.Version;
//     azblob v1.7.0 sends 2026-04-06 and refuses ClientOptions.APIVersion,
//     TestServiceVersionPinned), startFrom (the v1.7.0 pagers drop
//     StartFrom, TestStartFromSent) and the User-Agent.
//   - envelope, Guard.Validate on a listing's 200: its root must be
//     EnumerationResults with nothing after it and no <Error> element in it
//     (the SDK decodes an empty body, any root, or a listing holding an
//     error, as a page, an empty final one; TestScriptedAnswers,
//     TestSDKDecoding), and the decoded page must hold Blobs (Segment nil
//     otherwise).
//   - refusal and scrubLogs: the SAS kept out of what the SDK's decoder
//     errors quote (TestRedactedRefusal) and out of the SDK's own log
//     (TestLogScrubbed), which quotes failed requests' URLs and echoed
//     error messages before this package maps them.
//   - merge: the SDK returns a hierarchy page's blobs and prefixes as two
//     arrays; the provider's byte-order adapter needs them as one sequence
//     in wire (UTF-16 code-unit) order. Both arrays are in that order and
//     no name is in both on a valid page, so merging them with
//     utf16order.Compare restores the wire sequence
//     (TestDifferentialReplay, TestMergeOrder); a page out of order across
//     the two kinds is not detected (the hand-rolled parser would refuse
//     it), one out of order within a kind still is (ListOutput.Validate).
//
// Accepted differences from the hand-rolled parser, each pinned by a
// conformance test against the SDK's own decoding (TestSDKDecoding): an
// Encoded name is percent-decoded with query semantics (a literal '+' reads
// as a space; Azure sends '+' as %2B, as every Encoded name in the October
// 2026 captures escapes every reserved byte) and a malformed escape is a
// parse failure, not an invalid page; a root with a namespace is refused;
// a second <Blobs> is not refused, and a comment after the root is (the
// hand-rolled parser allows it); any
// malformed date property, not only the three a row keeps, fails the page,
// as does a malformed Date header on a listing. Get Account Information's
// x-ms-is-hns-enabled is read from the header as sent (client.HNS, shared
// with the hand-rolled client: exactly "true" or "false"), not from the
// SDK's strconv.ParseBool reading, which takes "0" for flat
// (TestAccountInfo).
package sdkclient

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	azlog "github.com/Azure/azure-sdk-for-go/sdk/azcore/log"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"

	"github.com/varveio/swath/go/internal/buildinfo"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/source/azblob/client"
	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/internal/utf16order"
)

// maxError is the error body the Guard keeps (an answer read for its code
// and message, as the hand-rolled client reads it).
const maxError = 64 << 10

// Client lists Azure Blob containers; safe for concurrent use.
type Client struct {
	*client.Core
	svc    []*service.Client // one per pooled transport (Core.Next)
	guards []*objhttp.Guard  // the same order (tests lower MaxBody)
}

// New builds the client on client.NewCore (the same credentials, pool and
// retry policy as client.New).
func New(ctx context.Context, cfg client.Config) (*Client, error) {
	core, err := client.NewCore(ctx, cfg)
	if err != nil {
		return nil, err
	}
	c := &Client{Core: core}
	scrubLogs(core)
	root := core.Base()
	if sas := core.SAS(); sas != "" {
		root += "?" + sas
	}
	var cred *service.SharedKeyCredential
	if key := core.SharedKey(); key != "" {
		if cred, err = service.NewSharedKeyCredential(core.Account(), key); err != nil {
			return nil, &client.CredentialError{Source: "shared key", Err: err}
		}
	}
	for _, hc := range core.Pool() {
		g := &objhttp.Guard{Next: hc.Transport, MaxBody: core.BodyCap(), MaxError: maxError, Validate: envelope}
		h := *hc // keeps the pool's no-redirect policy
		h.Transport = record{g}
		opt := &service.ClientOptions{ClientOptions: azcore.ClientOptions{
			Transport:       &h,
			Retry:           policy.RetryOptions{MaxRetries: -1}, // objhttp.Do retries
			PerCallPolicies: []policy.Policy{pin{}},
			Telemetry:       policy.TelemetryOptions{Disabled: true}, // pin sets the User-Agent
			Logging:         policy.LogOptions{IncludeBody: false},   // and no query value (scrubLogs)
		}}
		var svc *service.Client
		if cred != nil {
			svc, err = service.NewClientWithSharedKeyCredential(root, cred, opt)
		} else {
			svc, err = service.NewClientWithNoCredential(root, opt)
		}
		if err != nil {
			return nil, err
		}
		c.svc, c.guards = append(c.svc, svc), append(c.guards, g)
	}
	return c, nil
}

// The SDK's log: azcore writes it to its process-wide listener, which
// it sets itself, writing to stderr, when AZURE_SDK_GO_LOGGING is "all".
// Its lines quote what the SAS-free errors this package returns do not:
// a failed request's url.Error (the URL with the SAS), an error answer
// whose message echoes it. scrubLogs replaces that listener, process-wide,
// with the same one passing every line through each SAS client's
// Core.Scrub first; with the variable unset there is no listener and no
// log, as in azcore. A listener set later by other code is not scrubbed
// (swath sets none).
var (
	logMu     sync.Mutex
	logScrubs []func(string) string // one per client with a SAS
	logOnce   sync.Once
)

// scrubLogs registers core's SAS with the log scrubber and installs it
// once, when azcore would log.
func scrubLogs(core *client.Core) {
	if core.SAS() != "" {
		logMu.Lock()
		logScrubs = append(logScrubs, core.Scrub)
		logMu.Unlock()
	}
	logOnce.Do(func() {
		if os.Getenv("AZURE_SDK_GO_LOGGING") == "all" {
			azlog.SetListener(scrubbing(func(e azlog.Event, msg string) {
				fmt.Fprintf(os.Stderr, "[%s] %s: %s\n", time.Now().Format(time.StampMicro), e, msg)
			}))
		}
	})
}

// scrubbing is next with every line scrubbed of every registered SAS.
func scrubbing(next func(azlog.Event, string)) func(azlog.Event, string) {
	return func(e azlog.Event, msg string) {
		logMu.Lock()
		scrubs := logScrubs
		logMu.Unlock()
		for _, f := range scrubs {
			msg = f(msg)
		}
		next(e, msg)
	}
}

// List sends one List Blobs request, retrying retryable failures
// (objhttp.Do), and validates the page (client.ErrInvalidPage); a page
// past the body cap is asked again with fewer entries (Core.Shrinking).
func (c *Client) List(ctx context.Context, in *client.ListInput) (*client.ListOutput, error) {
	return c.Shrinking(ctx, in, func(ctx context.Context, in *client.ListInput) (*client.ListOutput, error) {
		return objhttp.Do(ctx, c.Retry(), c.Counts(), func() (*client.ListOutput, error) { return c.list(ctx, in) })
	})
}

// list is one List Blobs attempt: a fresh pager seeded with the marker,
// one NextPage.
func (c *Client) list(ctx context.Context, in *client.ListInput) (*client.ListOutput, error) {
	s := &sent{startFrom: in.StartFrom}
	actx, cancel, err := c.begin(ctx, s)
	if err != nil {
		return nil, err
	}
	defer cancel()
	cc := c.svc[c.Next()].NewContainerClient(in.Container)
	var size *int32
	if in.MaxResults > 0 {
		m := int32(min(in.MaxResults, math.MaxInt32))
		size = &m
	}
	var out *client.ListOutput
	done := c.Counts().Attempt()
	if in.Delimiter == "" {
		var r container.ListBlobsFlatResponse
		r, err = cc.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{Marker: str(in.Marker), Prefix: str(in.Prefix),
			MaxResults: size, StartFrom: str(in.StartFrom)}).NextPage(actx)
		if err == nil {
			out, err = flat(r.Segment, r.NextMarker)
		}
	} else {
		var r container.ListBlobsHierarchyResponse
		r, err = cc.NewListBlobsHierarchyPager(in.Delimiter, &container.ListBlobsHierarchyOptions{Marker: str(in.Marker),
			Prefix: str(in.Prefix), MaxResults: size, StartFrom: str(in.StartFrom)}).NextPage(actx)
		if err == nil {
			out, err = merge(r.Segment, r.NextMarker)
		}
	}
	done()
	if err != nil {
		if !s.refused(err) {
			return nil, c.failure(actx, ctx, s, err, in.Container)
		}
		return nil, c.refusal(err)
	}
	if err := faults.At("response-read"); err != nil {
		return nil, err
	}
	if err := out.Validate(in.Marker); err != nil {
		return nil, err
	}
	return out, nil
}

// AccountInfo sends Get Account Information on the container's path and
// reports whether the account has a hierarchical namespace, from the 200's
// x-ms-is-hns-enabled as sent (client.HNS: exactly "true" or "false";
// anything else is client.ErrHNSUnknown, never flat).
func (c *Client) AccountInfo(ctx context.Context, container string) (bool, error) {
	return objhttp.Do(ctx, c.Retry(), c.Counts(), func() (bool, error) {
		s := &sent{}
		actx, cancel, err := c.begin(ctx, s)
		if err != nil {
			return false, err
		}
		defer cancel()
		done := c.Counts().Attempt()
		_, err = c.svc[c.Next()].NewContainerClient(container).GetAccountInfo(actx, nil)
		done()
		switch {
		case s.refused(err) || err == nil:
			// the header as sent, not the SDK's strconv.ParseBool reading
			// of it ("1" and "0" are not Azure's answers)
			return client.HNS(s.header)
		}
		return false, c.failure(actx, ctx, s, err, container)
	})
}

// NamespaceInfo is Core.NamespaceInfo (one HEAD on the account's Data Lake
// host: there is no azblob call for it). With Shared Key it is not asked:
// the SDK client does not sign requests of its own, and a key Get Account
// Information refused would be refused there too.
func (c *Client) NamespaceInfo(ctx context.Context, container string) (bool, error) {
	if c.SharedKey() != "" {
		return false, fmt.Errorf("%w (Shared Key: not asked)", client.ErrNoDFSEndpoint)
	}
	return c.Core.NamespaceInfo(ctx, container)
}

// begin starts an attempt: its deadline, and with Entra ID its token,
// waited for before the attempt is counted (a failure is a
// *client.CredentialError, not an attempt).
func (c *Client) begin(ctx context.Context, s *sent) (context.Context, context.CancelFunc, error) {
	actx, cancel := context.WithTimeout(ctx, c.Timeout())
	if c.Bearer() {
		tok, err := c.Token(actx)
		if err != nil {
			cancel()
			return nil, nil, err
		}
		actx = policy.WithHTTPHeader(actx, http.Header{"Authorization": {"Bearer " + tok.AccessToken}})
	}
	return context.WithValue(actx, sentKey{}, s), cancel, nil
}

// failure is the error of an attempt that got no page: the service's answer
// as a *client.Error (from the bounded answer the Guard handed over; a
// non-200 2xx too), a body past the cap as client.ErrBodyCap, the Guard's
// own failures unwrapped (objhttp.Cause), an attempt timeout as
// objhttp.ErrTimeout. The SAS is never in it.
func (c *Client) failure(actx, ctx context.Context, s *sent, err error, container string) error {
	u := s.safeURL(c, container)
	if re, ok := errors.AsType[*azcore.ResponseError](err); ok && re.RawResponse != nil {
		body, _ := runtime.Payload(re.RawResponse)
		return c.ResponseError(re.RawResponse, body, u)
	}
	err = objhttp.Cause(err)
	if _, ok := errors.AsType[*objhttp.TooLargeError](err); ok {
		return client.ErrBodyCap
	}
	if errors.Is(err, objhttp.ErrSyntax) {
		c.Counts().ParseFailure()
		return err
	}
	return objhttp.Timeout(actx, ctx, c.Timeout(), c.Redact(err, u))
}

// refusal is the error of a 200 the SDK's decoder (or a header it reads)
// refused, or whose page did not convert: client.ErrInvalidPage when the
// page broke the listing contract, else objhttp.ErrSyntax (a parse
// failure, counted and retried). Its text is scrubbed of the SAS: the SDK's
// errors quote what they could not read (a Date header, a name).
func (c *Client) refusal(err error) error {
	sentinel := objhttp.ErrSyntax
	if errors.Is(err, client.ErrInvalidPage) {
		sentinel = client.ErrInvalidPage
	} else {
		c.Counts().ParseFailure()
	}
	msg := err.Error()
	if s := c.Scrub(msg); s != msg || !errors.Is(err, sentinel) {
		return fmt.Errorf("%w: %s", sentinel, s)
	}
	return err
}

// str is s as an SDK option: nil when empty (not sent).
func str(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// errNoBlobs is a decoded listing without <Blobs> (the SDK reads it as an
// empty page).
var errNoBlobs = fmt.Errorf("%w: <EnumerationResults> without <Blobs>", objhttp.ErrSyntax)

// flat converts a flat page.
func flat(seg *container.BlobFlatListSegment, next *string) (*client.ListOutput, error) {
	if seg == nil {
		return nil, errNoBlobs
	}
	out := &client.ListOutput{Items: make([]client.Item, 0, len(seg.BlobItems)), NextMarker: deref(next)}
	for _, b := range seg.BlobItems {
		it, err := item(b)
		if err != nil {
			return nil, err
		}
		out.Items = append(out.Items, it)
	}
	return out, nil
}

// merge converts a hierarchy page: its blobs and prefixes, each in wire
// order, merged into one sequence in UTF-16 code-unit order, which is the
// wire's on a valid page (the package comment).
func merge(seg *container.BlobHierarchyListSegment, next *string) (*client.ListOutput, error) {
	if seg == nil {
		return nil, errNoBlobs
	}
	blobs, prefixes := seg.BlobItems, seg.BlobPrefixes
	out := &client.ListOutput{Items: make([]client.Item, 0, len(blobs)+len(prefixes)), NextMarker: deref(next)}
	for len(blobs) > 0 || len(prefixes) > 0 {
		var p string
		if len(prefixes) > 0 {
			if prefixes[0] == nil || prefixes[0].Name == nil {
				return nil, fmt.Errorf("%w: a <BlobPrefix> without a name", objhttp.ErrSyntax)
			}
			p = *prefixes[0].Name
		}
		if len(blobs) > 0 && (len(prefixes) == 0 || blobs[0] == nil || blobs[0].Name == nil || utf16order.Compare(*blobs[0].Name, p) <= 0) {
			it, err := item(blobs[0])
			if err != nil {
				return nil, err
			}
			out.Items, blobs = append(out.Items, it), blobs[1:]
			continue
		}
		if !utf8.ValidString(p) {
			return nil, fmt.Errorf("%w: prefix %q is not UTF-8", client.ErrInvalidPage, p)
		}
		out.Items, prefixes = append(out.Items, client.Item{Name: p, Prefix: true}), prefixes[1:]
	}
	return out, nil
}

// item converts one blob: the fields a row keeps.
func item(b *container.BlobItem) (client.Item, error) {
	if b == nil || b.Name == nil {
		return client.Item{}, fmt.Errorf("%w: a <Blob> without a name", objhttp.ErrSyntax)
	}
	it := client.Item{Name: *b.Name}
	if !utf8.ValidString(it.Name) {
		return client.Item{}, fmt.Errorf("%w: name %q is not UTF-8", client.ErrInvalidPage, it.Name)
	}
	p := b.Properties
	if p == nil {
		return it, nil
	}
	if p.ContentLength != nil {
		it.Size = *p.ContentLength
	}
	if p.LastModified != nil {
		it.LastModified = p.LastModified.UnixMicro()
	}
	if p.CreationTime != nil {
		it.CreationTime = p.CreationTime.UnixMicro()
	}
	if p.ETag != nil {
		it.ETag = string(*p.ETag)
	}
	if p.ContentMD5 != nil {
		it.ContentMD5 = base64.StdEncoding.EncodeToString(p.ContentMD5)
	}
	it.ContentType = deref(p.ContentType)
	if p.BlobType != nil {
		it.BlobType = string(*p.BlobType)
	}
	if p.AccessTier != nil {
		it.AccessTier = string(*p.AccessTier)
	}
	return it, nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// bom starts every body Azure sends.
var bom = []byte("\xef\xbb\xbf")

// envelope is the Guard's check of a listing's 200 (comp=list), what the
// SDK's decoder does not check: the root element is EnumerationResults
// without a namespace (Azure never prefixes it), nothing but whitespace
// follows its end, and no <Error> element is in it
// (the SDK would read such a body as a page, an empty final one when the
// error replaces the entries). XML text cannot hold a literal '<', so a
// body with one "</EnumerationResults" ending in it has nothing after its
// root, and one without "Error" has no Error element, even with a namespace
// prefix: the token walk runs only for a body that holds "Error" (an
// ErrorCode element or a blob name, say). Any other answer is the SDK's to
// read.
func envelope(req *http.Request, status int, body []byte) error {
	if status != http.StatusOK || req.URL.Query().Get("comp") != "list" {
		return nil
	}
	d := xml.NewDecoder(bytes.NewReader(bytes.TrimPrefix(body, bom)))
	for {
		tok, err := d.Token()
		if err == io.EOF {
			return fmt.Errorf("%w: a 200 without a root element (%d bytes)", objhttp.ErrSyntax, len(body))
		}
		if err != nil {
			return fmt.Errorf("%w (%d bytes): %v", objhttp.ErrSyntax, len(body), err)
		}
		if s, ok := tok.(xml.StartElement); ok {
			if s.Name.Local != "EnumerationResults" || s.Name.Space != "" {
				return fmt.Errorf("%w: a 200 whose root is not unnamespaced <EnumerationResults> (%d bytes)", objhttp.ErrSyntax, len(body))
			}
			break
		}
	}
	const end = "</EnumerationResults>"
	if bytes.Count(body, []byte("</EnumerationResults")) != 1 || !bytes.HasSuffix(bytes.TrimRight(body, " \t\r\n"), []byte(end)) {
		return fmt.Errorf("%w: a 200 listing with content after %s, or without it (%d bytes)", objhttp.ErrSyntax, end, len(body))
	}
	if !bytes.Contains(body, []byte("Error")) {
		return nil
	}
	for {
		tok, err := d.RawToken()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return nil // the SDK's decoder refuses it
		}
		// Match the local name regardless of the prefix kept in Name.Space.
		if s, ok := tok.(xml.StartElement); ok && s.Name.Local == "Error" {
			return fmt.Errorf("%w: a 200 listing holding an <Error> element (%d bytes)", objhttp.ErrSyntax, len(body))
		}
	}
}

// sentKey carries an attempt's sent on its context.
type sentKey struct{}

// sent is what an attempt met on the wire: its startFrom (for pin), the
// request URL as sent and whether a 200 passed the Guard to the SDK.
type sent struct {
	startFrom string
	url       *url.URL
	ok        bool
	header    http.Header // the 200's
}

// refused reports err as the SDK's refusal of a 200 the Guard passed (its
// decoder's or a header's), not a failure to get one: a 200 reached it and
// the attempt was not cancelled meanwhile (azcore then returns the
// context's error).
func (s *sent) refused(err error) bool {
	return s.ok && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

// safeURL is the attempt's URL without the SAS: as sent, or the
// container's route when nothing was sent; a 400's Query and a redacted
// transport error name it.
func (s *sent) safeURL(c *Client, container string) *url.URL {
	if s.url == nil {
		u, _ := url.Parse(c.Route(container))
		return u
	}
	u := *s.url
	sas, _ := url.ParseQuery(c.SAS())
	q := u.Query()
	for k := range sas {
		q.Del(k)
	}
	u.RawQuery = strings.ReplaceAll(q.Encode(), "+", "%20")
	return &u
}

// pin is the per-call policy (azcore runs it before its retry policy and
// the Shared Key signing policy): the service version client.Version,
// replacing the SDK's own (the generated code keys it in lower case,
// outside the canonical form, so that key is replaced); startFrom, which
// azblob v1.7.0's pagers drop, when the SDK did not send it; and the
// User-Agent the hand-rolled client sends.
type pin struct{}

func (pin) Do(req *policy.Request) (*http.Response, error) {
	r := req.Raw()
	r.Header["x-ms-version"] = []string{client.Version}
	r.Header.Set("User-Agent", buildinfo.Tool)
	if s, _ := r.Context().Value(sentKey{}).(*sent); s != nil && s.startFrom != "" {
		if q := r.URL.Query(); !q.Has("startFrom") {
			q.Set("startFrom", s.startFrom)
			r.URL.RawQuery = strings.ReplaceAll(q.Encode(), "+", "%20")
		}
	}
	return req.Next()
}

// record notes the request URL and whether a response reached the SDK,
// in the attempt's sent (from the request context).
type record struct{ next http.RoundTripper }

func (r record) RoundTrip(req *http.Request) (*http.Response, error) {
	s, _ := req.Context().Value(sentKey{}).(*sent)
	if s != nil {
		u := *req.URL
		s.url = &u
	}
	resp, err := r.next.RoundTrip(req)
	if s != nil && err == nil && resp.StatusCode == http.StatusOK {
		s.ok, s.header = true, resp.Header
	}
	return resp, err
}
