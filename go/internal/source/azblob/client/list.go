// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

// The list path: List Blobs and Get Account Information through the Azure
// SDK (azblob's service and container clients), one pager seeded with the
// marker and one NextPage per attempt.
//
// The SDK sends; swath owns the rest. azcore's retry policy is off
// (MaxRetries -1: objhttp.Do retries); the NoCredential pipeline has no
// auth policy, so an Entra ID token is fetched before the attempt
// (Client.token) and sent as a request header (policy.WithHTTPHeader),
// never through azcore's bearer policy and its 401 challenge re-send;
// Shared Key is the SDK's own signing policy. Every response passes an
// objhttp.Guard below the SDK (64 MiB success bodies, past which the
// request fails with ErrBodyCap; 64 KiB error bodies; the original body
// closed), and redirects are answers, never followed, their Location never
// read (noLocation).
//
// Custom code is kept where a test shows the SDK alone breaks a contract,
// each piece once:
//
//   - pin, a per-call policy (before the retry and signing policies, so
//     Shared Key signs what it sets): the service version (Version; azblob
//     v1.8.3 sends its own default and refuses ClientOptions.APIVersion,
//     TestServiceVersionPinned). The User-Agent is azcore's own telemetry
//     policy's, swath's product token (buildinfo.UserAgent, its
//     ApplicationID, 24 bytes at most) before the SDK's.
//   - the listing options name ResponseFormat Arrow: the SDK asks for
//     Arrow, then XML (Accept), and decodes the answer by its Content-Type.
//     The service picks the format; an account or endpoint without Arrow
//     listings (a hierarchical namespace, Azurite, swath-replay) answers
//     XML. Each valid page counts in its format (list_format.*). StartFrom
//     is the SDK's own option. TestWireSnapshot pins every request the
//     client sends.
//   - envelope, Guard.Validate on a listing's 200, in the format its
//     Content-Type names. XML: the root must be EnumerationResults with
//     nothing after it and no <Error> element in it (the SDK decodes an
//     empty body, any root, or a listing holding an error, as a page, an
//     empty final one; TestScriptedAnswers, TestSDKDecoding), and the
//     decoded page must hold Blobs (Segment nil otherwise). Arrow: the
//     stream must read without error, its schema metadata must hold
//     NextMarker and NumberOfRecords, its batches that many rows, and each
//     column the SDK maps a type the SDK reads (the SDK reads a stream cut
//     after a batch as whole, one without NextMarker as a final page, and
//     a column of another type, a dictionary-encoded one say, as nulls;
//     TestArrowEnvelope, TestArrowColumnTypes).
//   - refusal: the SAS kept out of what the SDK's decoder errors quote
//     (TestRedactedRefusal). The SDK's own log, which quotes failed
//     requests' URLs, is not swath's: the command turns it off at startup
//     (cmd/swath), whatever AZURE_SDK_GO_LOGGING says.
//   - merge: the SDK returns a hierarchy page's blobs and prefixes as two
//     arrays; the provider's byte-order adapter needs them as one sequence
//     in wire (UTF-16 code-unit) order. Both arrays are in that order and
//     no name is in both on a valid page, so merging them with
//     utf16order.Compare restores the wire sequence (TestWireOrder,
//     TestMergeOrder). An Arrow page is one sequence the SDK splits by
//     ResourceType, each kind kept in order, so the merge restores it too
//     (TestArrowEquivalence). A page out of order across the two kinds is not
//     detected, one out of order within a kind still is
//     (ListOutput.Validate).
//
// Accepted readings of the SDK's XML decoder, each pinned by TestSDKDecoding:
// an Encoded name is percent-decoded with query semantics (a literal '+'
// reads as a space; Azure sends '+' as %2B, as every Encoded name in the
// October 2026 captures escapes every reserved byte) and a malformed escape
// is a parse failure, not an invalid page; a root with a namespace is
// refused, and so is a comment after the root. A malformed Date header on
// a listing fails the page (TestRedactedRefusal). The decoder also reads a
// second <Blobs> and fails a page on any malformed date property; swath
// depends on neither. Get Account
// Information's x-ms-is-hns-enabled is read from the header as sent (hns:
// exactly "true" or "false"), not from the SDK's strconv.ParseBool reading,
// which takes "0" for flat (TestAccountInfoExact).

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
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"

	"github.com/varveio/swath/go/internal/buildinfo"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/internal/utf16order"
)

// services builds one SDK service client per pooled transport, each over
// a Guard capping success bodies at bodyCap (MaxBody), on the account's
// root (the SAS appended to it).
func (c *Client) services(bodyCap int64) error {
	root := c.base.String()
	if c.sas != "" {
		root += "?" + c.sas
	}
	var cred *service.SharedKeyCredential
	if c.key != "" {
		var err error
		if cred, err = service.NewSharedKeyCredential(c.cfg.Account, c.key); err != nil {
			return &objhttp.CredentialError{Source: c.source, Err: err}
		}
	}
	for _, hc := range c.http {
		g := &objhttp.Guard{Next: hc.Transport, MaxBody: bodyCap, MaxError: maxErrorBody, Validate: envelope}
		h := *hc // keeps the pool's no-redirect policy
		h.Transport = g
		opt := &service.ClientOptions{ClientOptions: azcore.ClientOptions{
			Transport:       &h,
			Retry:           policy.RetryOptions{MaxRetries: -1}, // objhttp.Do retries
			PerCallPolicies: []policy.Policy{pin{}},
			Telemetry:       policy.TelemetryOptions{ApplicationID: buildinfo.UserAgent()}, // swath's token, then the SDK's
		}}
		var svc *service.Client
		var err error
		if cred != nil {
			svc, err = service.NewClientWithSharedKeyCredential(root, cred, opt)
		} else {
			svc, err = service.NewClientWithNoCredential(root, opt)
		}
		if err != nil {
			return err
		}
		c.svc = append(c.svc, svc)
	}
	return nil
}

// List sends one List Blobs request, retrying retryable failures
// (objhttp.Do), and validates the page (objhttp.ErrInvalidPage). A page past the
// body cap is ErrBodyCap, not retried.
func (c *Client) List(ctx context.Context, in *ListInput) (*ListOutput, error) {
	return objhttp.Do(ctx, c.Retry, &c.Tally, func(ctx context.Context) (*ListOutput, error) { return c.list(ctx, in) })
}

// list is one List Blobs attempt: a fresh pager seeded with the marker,
// one NextPage.
func (c *Client) list(ctx context.Context, in *ListInput) (*ListOutput, error) {
	s := &sent{}
	actx, cancel, err := c.begin(ctx, s)
	if err != nil {
		return nil, err
	}
	defer cancel()
	cc := c.svc[c.pick()].NewContainerClient(in.Container)
	var size *int32
	if in.MaxResults > 0 {
		m := int32(min(in.MaxResults, math.MaxInt32))
		size = &m
	}
	var out *ListOutput
	done := c.Tally.Attempt()
	if in.Delimiter == "" {
		var r container.ListBlobsFlatResponse
		r, err = cc.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{Marker: str(in.Marker), Prefix: str(in.Prefix),
			MaxResults: size, StartFrom: str(in.StartFrom), ResponseFormat: container.StorageResponseFormatArrow}).NextPage(actx)
		if err == nil {
			out, err = flat(r.Segment, r.NextMarker)
		}
	} else {
		var r container.ListBlobsHierarchyResponse
		r, err = cc.NewListBlobsHierarchyPager(in.Delimiter, &container.ListBlobsHierarchyOptions{Marker: str(in.Marker),
			Prefix: str(in.Prefix), MaxResults: size, StartFrom: str(in.StartFrom), ResponseFormat: container.StorageResponseFormatArrow}).NextPage(actx)
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
	c.formats.count(arrowAnswer(s.Header), len(out.Items))
	return out, nil
}

// AccountInfo sends Get Account Information on the container's path
// (restype=account&comp=properties, which a SAS scoped to the container
// may call) and reports whether the account has a hierarchical namespace,
// from the 200's x-ms-is-hns-enabled as sent (hns: exactly "true" or
// "false"; anything else is ErrHNSUnknown, never flat). A refused call is
// an error the caller takes for "unknown", never for flat.
func (c *Client) AccountInfo(ctx context.Context, container string) (bool, error) {
	return objhttp.Do(ctx, c.Retry, &c.Tally, func(ctx context.Context) (bool, error) {
		s := &sent{}
		actx, cancel, err := c.begin(ctx, s)
		if err != nil {
			return false, err
		}
		defer cancel()
		done := c.Tally.Attempt()
		_, err = c.svc[c.pick()].NewContainerClient(container).GetAccountInfo(actx, nil)
		done()
		switch {
		case s.refused(err) || err == nil:
			// the header as sent, not the SDK's strconv.ParseBool reading
			// of it ("1" and "0" are not Azure's answers)
			return hns(s.Header)
		}
		return false, c.failure(actx, ctx, s, err, container)
	})
}

// begin starts an attempt: its deadline, and with Entra ID its token,
// waited for before the attempt is counted (a failure is a
// *objhttp.CredentialError, not an attempt).
func (c *Client) begin(ctx context.Context, s *sent) (context.Context, context.CancelFunc, error) {
	actx, cancel := context.WithTimeout(ctx, c.cfg.RequestTimeout)
	if c.ts != nil {
		tok, err := objhttp.WaitToken(actx, c.ts, c.source, c.cfg.RequestTimeout, transientToken)
		if err != nil {
			cancel()
			return nil, nil, err
		}
		actx = policy.WithHTTPHeader(actx, http.Header{headerAuthorization: {"Bearer " + tok.AccessToken}})
	}
	return objhttp.WithCapture(actx, &s.Capture), cancel, nil
}

// failure is the error of an attempt that got no page: the service's answer
// as an *Error (from the bounded answer the Guard handed over; a non-200
// 2xx too), a body past the cap as ErrBodyCap (terminal), the Guard's own
// failures resolved by objhttp.Normalize, an attempt timeout as
// objhttp.ErrTimeout. The SAS is never in it.
func (c *Client) failure(actx, ctx context.Context, s *sent, err error, container string) error {
	u := s.safeURL(c, container)
	if re, ok := errors.AsType[*azcore.ResponseError](err); ok && re.RawResponse != nil {
		body, _ := runtime.Payload(re.RawResponse)
		return c.responseError(re.RawResponse, re.ErrorCode, body, u)
	}
	if tl, ok := errors.AsType[*objhttp.TooLargeError](err); ok {
		return fmt.Errorf("%w: a listing page past %d bytes", ErrBodyCap, tl.Limit)
	}
	if errors.Is(err, objhttp.ErrSyntax) {
		return err
	}
	return objhttp.Timeout(actx, ctx, c.cfg.RequestTimeout, c.redact(err, u))
}

// refusal is the error of a 200 the SDK's decoder (or a header it reads)
// refused, or whose page did not convert: objhttp.ErrInvalidPage when the page
// broke the listing contract, else objhttp.ErrSyntax (a parse failure,
// counted and retried by objhttp.Do). Its text is scrubbed of the SAS: the SDK's errors
// quote what they could not read (a Date header, a name).
func (c *Client) refusal(err error) error {
	sentinel := objhttp.ErrSyntax
	if errors.Is(err, objhttp.ErrInvalidPage) {
		sentinel = objhttp.ErrInvalidPage
	}
	msg := err.Error()
	if s := c.scrub(msg); s != msg || !errors.Is(err, sentinel) {
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
func flat(seg *container.BlobFlatListSegment, next *string) (*ListOutput, error) {
	if seg == nil {
		return nil, errNoBlobs
	}
	out := &ListOutput{Items: make([]Item, 0, len(seg.BlobItems)), NextMarker: deref(next)}
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
// wire's on a valid page (the comment at the top of this file).
func merge(seg *container.BlobHierarchyListSegment, next *string) (*ListOutput, error) {
	if seg == nil {
		return nil, errNoBlobs
	}
	blobs, prefixes := seg.BlobItems, seg.BlobPrefixes
	out := &ListOutput{Items: make([]Item, 0, len(blobs)+len(prefixes)), NextMarker: deref(next)}
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
			return nil, fmt.Errorf("%w: prefix %q is not UTF-8", objhttp.ErrInvalidPage, p)
		}
		out.Items, prefixes = append(out.Items, Item{Name: p, Prefix: true}), prefixes[1:]
	}
	return out, nil
}

// item converts one blob: the fields a row keeps.
func item(b *container.BlobItem) (Item, error) {
	if b == nil || b.Name == nil {
		return Item{}, fmt.Errorf("%w: a <Blob> without a name", objhttp.ErrSyntax)
	}
	it := Item{Name: *b.Name}
	if !utf8.ValidString(it.Name) {
		return Item{}, fmt.Errorf("%w: name %q is not UTF-8", objhttp.ErrInvalidPage, it.Name)
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

// arrowType is the Content-Type of an Arrow listing. The SDK decodes a 200
// whose Content-Type starts with it as Arrow, any other as XML.
const arrowType = "application/vnd.apache.arrow.stream"

// arrowAnswer reports a response the SDK decodes as an Arrow listing.
func arrowAnswer(h http.Header) bool { return strings.HasPrefix(h.Get("Content-Type"), arrowType) }

// envelope is the Guard's check of a listing's 200 (comp=list): what the
// SDK's decoder does not check, in the format the Content-Type names, the
// one the SDK decodes (arrowEnvelope, xmlEnvelope).
func envelope(req *http.Request, status int, header http.Header, body []byte) error {
	if status != http.StatusOK || req.URL.Query().Get("comp") != "list" {
		return nil
	}
	if arrowAnswer(header) {
		return arrowEnvelope(body)
	}
	return xmlEnvelope(body)
}

// arrowEnvelope checks an Arrow listing: an IPC stream whose schema
// metadata holds NextMarker and NumberOfRecords (Azure's listing metadata),
// whose columns the SDK maps each have a type its decoder reads
// (arrowColumns), and whose record batches hold NumberOfRecords rows. The
// SDK's decoder reads a stream cut at a message boundary as whole (the
// Arrow reader takes the body's end for the stream's), batches missing,
// all of them included, as a shorter page, a schema without NextMarker as
// a final page, and a column of another type as nulls (a dictionary
// ResourceType makes prefixes blobs); each would drop or blank entries, so
// each is refused (TestArrowEnvelope, TestArrowColumnTypes). Columns the
// SDK does not map are ignored, as it ignores them. The batches are read
// (one copy of the body), not converted row by row.
func arrowEnvelope(body []byte) error {
	r, err := ipc.NewReader(bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("%w: an Arrow listing without a schema (%d bytes): %v", objhttp.ErrSyntax, len(body), err)
	}
	defer r.Release()
	for _, f := range r.Schema().Fields() {
		if read, ok := arrowColumns[f.Name]; ok && !read(f.Type) {
			return fmt.Errorf("%w: an Arrow listing's column %q is %s, which the SDK reads as nulls", objhttp.ErrSyntax, f.Name, f.Type)
		}
	}
	md := r.Schema().Metadata()
	n := md.FindKey("NumberOfRecords")
	if n < 0 || md.FindKey("NextMarker") < 0 {
		return fmt.Errorf("%w: an Arrow listing whose schema lacks NextMarker or NumberOfRecords (%d bytes)", objhttp.ErrSyntax, len(body))
	}
	want, err := strconv.ParseInt(md.Values()[n], 10, 64)
	if err != nil {
		return fmt.Errorf("%w: an Arrow listing's NumberOfRecords %q", objhttp.ErrSyntax, md.Values()[n])
	}
	var rows int64
	for r.Next() {
		rows += r.RecordBatch().NumRows()
	}
	if err := r.Err(); err != nil {
		return fmt.Errorf("%w: an Arrow listing (%d bytes): %v", objhttp.ErrSyntax, len(body), err)
	}
	if rows != want {
		return fmt.Errorf("%w: an Arrow listing of %d rows declaring %d (%d bytes)", objhttp.ErrSyntax, rows, want, len(body))
	}
	return nil
}

// The physical types the SDK's Arrow resolvers take (azblob v1.8.3
// internal/arrow: resolveString, resolveBool, resolveTimestamp,
// resolveInt64, resolveInt32, resolveMapCol, each a type assertion that
// yields nil, every value null, on any other type).
var (
	arrowString    = func(t arrow.DataType) bool { return t.ID() == arrow.STRING }
	arrowBool      = func(t arrow.DataType) bool { return t.ID() == arrow.BOOL }
	arrowTimestamp = func(t arrow.DataType) bool { return t.ID() == arrow.TIMESTAMP }
	arrowInt64     = func(t arrow.DataType) bool { return t.ID() == arrow.UINT64 || t.ID() == arrow.INT64 }
	arrowInt32     = func(t arrow.DataType) bool { return t.ID() == arrow.UINT64 || t.ID() == arrow.INT32 }
	arrowMap       = func(t arrow.DataType) bool {
		m, ok := t.(*arrow.MapType)
		return ok && m.KeyType().ID() == arrow.STRING && m.ItemType().ID() == arrow.STRING
	}
)

// arrowColumns are the columns the SDK's Arrow decoder maps
// (resolveColumns), each with the types its resolver reads.
var arrowColumns = map[string]func(arrow.DataType) bool{
	"Name": arrowString, "Snapshot": arrowString, "VersionId": arrowString, "Etag": arrowString,
	"Content-Type": arrowString, "Content-Encoding": arrowString, "Content-Language": arrowString,
	"Content-Disposition": arrowString, "Cache-Control": arrowString, "Content-MD5": arrowString,
	"BlobType": arrowString, "AccessTier": arrowString, "SmartAccessTier": arrowString, "LeaseState": arrowString,
	"LeaseStatus": arrowString, "LeaseDuration": arrowString, "CustomerProvidedKeySha256": arrowString,
	"EncryptionScope": arrowString, "ArchiveStatus": arrowString, "RehydratePriority": arrowString,
	"CopyId": arrowString, "CopyStatus": arrowString, "CopySource": arrowString, "CopyProgress": arrowString,
	"CopyStatusDescription": arrowString, "CopyDestinationSnapshot": arrowString,
	"ImmutabilityPolicyMode": arrowString, "ResourceType": arrowString,
	"Deleted": arrowBool, "IsCurrentVersion": arrowBool, "HasVersionsOnly": arrowBool,
	"AccessTierInferred": arrowBool, "ServerEncrypted": arrowBool, "IncrementalCopy": arrowBool,
	"Sealed": arrowBool, "LegalHold": arrowBool,
	"Creation-Time": arrowTimestamp, "Last-Modified": arrowTimestamp, "AccessTierChangeTime": arrowTimestamp,
	"CopyCompletionTime": arrowTimestamp, "ImmutabilityPolicyUntilDate": arrowTimestamp,
	"DeletedTime": arrowTimestamp, "LastAccessTime": arrowTimestamp,
	"Content-Length": arrowInt64, "x-ms-blob-sequence-number": arrowInt64,
	"RemainingRetentionDays": arrowInt32, "TagCount": arrowInt32,
	"Metadata": arrowMap, "OrMetadata": arrowMap, "Tags": arrowMap,
}

// xmlEnvelope checks an XML listing: the root element is
// EnumerationResults without a namespace (Azure never prefixes it),
// nothing but whitespace follows its end, and no <Error> element is in it
// (the SDK would read such a body as a page, an empty final one when the
// error replaces the entries). XML text cannot hold a literal '<', so a
// body with one "</EnumerationResults" ending in it has nothing after its
// root, and one without "Error" has no Error element, even with a namespace
// prefix: the token walk runs only for a body that holds "Error" (an
// ErrorCode element or a blob name, say). Any other answer is the SDK's to
// read.
func xmlEnvelope(body []byte) error {
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

// sent is what an attempt met on the wire: the request URL as sent and
// whether a 200 passed the Guard to the SDK.
type sent struct {
	objhttp.Capture
}

// refused reports err as the SDK's refusal of a 200 the Guard passed (its
// decoder's or a header's), not a failure to get one: a 200 reached it and
// the attempt was not cancelled meanwhile (azcore then returns the
// context's error).
func (s *sent) refused(err error) bool {
	return s.Status == http.StatusOK && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

// safeURL is the attempt's URL without the SAS: as sent, or the
// container's route when nothing was sent; a 400's Query and a redacted
// transport error name it.
func (s *sent) safeURL(c *Client, container string) *url.URL {
	if s.URL == nil {
		u := c.route(container)
		return &u
	}
	u := *s.URL
	sas, _ := url.ParseQuery(c.sas)
	q := u.Query()
	for k := range sas {
		q.Del(k)
	}
	u.RawQuery = encodeQuery(q)
	return &u
}

// pin is the per-call policy (azcore runs it before its retry policy and
// the Shared Key signing policy): the service version Version, replacing
// the SDK's own (the generated code keys it in lower case, outside the
// canonical form, so that key is replaced).
type pin struct{}

func (pin) Do(req *policy.Request) (*http.Response, error) {
	req.Raw().Header["x-ms-version"] = []string{Version}
	return req.Next()
}
