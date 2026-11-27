// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/pflag"

	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/parquetout"
	"github.com/varveio/swath/internal/registry"
	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/keyrange"
	"github.com/varveio/swath/internal/source/objhttp"
	"github.com/varveio/swath/internal/source/s3"
	"github.com/varveio/swath/internal/source/tokensource"
)

func init() {
	registry.Register(registry.Factory{
		EncoderBytes: parquetout.EncoderBytes(&s3.Schema), Schemas: []schema.Info{s3.Schema.Info, s3.VersionsSchema.Info}, Classify: objhttp.Label,
		Scheme: "s3", KeyPrefix: s3.KeyPrefix, WithPrefix: s3.WithPrefix, Suggest: s3Suggest,
		Flags: s3Flags, Shared: []*registry.FlagGroup{StoreGroup, EngineGroup}, Defaults: s3Defaults, Open: openS3,
		Secrets:     []registry.Secret{{Flag: "s3-bearer-token-command"}},
		Operational: []string{"s3-profile", "s3-bearer-token-refresh-interval"},
		Title:       "S3", Example: "s3://bucket/logs/", Usage: "s3://bucket[/prefix]"})
}

// s3Suggest is the s3:// root of a source given as another tool's S3
// scheme (s3a://, s3n://) or as an S3 https URL, virtual-hosted
// (BUCKET.s3[.REGION].amazonaws.com/KEY) or path-style
// (s3[.REGION].amazonaws.com/BUCKET/KEY) (registry.Factory.Suggest).
func s3Suggest(root string) (string, bool) {
	u, err := url.Parse(root)
	if err != nil {
		return "", false
	}
	bucket, key := u.Host, u.Path
	switch scheme, host := strings.ToLower(u.Scheme), strings.ToLower(u.Hostname()); {
	case scheme == "s3a" || scheme == "s3n":
	case scheme != "https" && scheme != "http" || !strings.HasSuffix(host, ".amazonaws.com"):
		return "", false
	case strings.HasPrefix(host, "s3.") || strings.HasPrefix(host, "s3-"):
		bucket, key, _ = strings.Cut(strings.TrimPrefix(u.Path, "/"), "/")
	default:
		i := strings.Index(host, ".s3.")
		if i < 0 {
			i = strings.Index(host, ".s3-")
		}
		if i <= 0 {
			return "", false
		}
		bucket = host[:i]
	}
	if bucket == "" {
		return "", false
	}
	if key = strings.TrimPrefix(key, "/"); key != "" {
		bucket += "/" + key
	}
	return "s3://" + bucket, true
}

// s3Flagged is the S3 flag group's value: the S3 options, the store
// group's values and the range engine's policy and HTTP admission, whose
// flags every engine provider shares.
type s3Flagged struct {
	s3.Options
	store        *source.Store
	common       *keyrange.Common
	pathStyleSet bool // --s3-path-style given (else on with a custom endpoint, as Java's)
	refreshSet   bool // --s3-bearer-token-refresh-interval given
	fipsSet      bool // --s3-fips given (else AWS_USE_FIPS_ENDPOINT)
	dualStackSet bool // --s3-dualstack given (else AWS_USE_DUALSTACK_ENDPOINT)
}

// tracked is a switch's value that sets *p to whether the value given is
// on, and records in *set that it was given.
func tracked(p *bool, on bool, set *bool) pflag.Value {
	return Func(func(s string) error {
		v, err := strconv.ParseBool(s)
		if err != nil {
			return errors.New("want true or false")
		}
		*p, *set = v == on, true
		return nil
	})
}

func s3Defaults() any { return &s3Flagged{} }

// s3Flags registers the S3 flag group beside the shared groups' values;
// the returned value is filled when the flag set is parsed.
func s3Flags(f *pflag.FlagSet, diag bool, shared registry.SharedValues) any {
	o := &s3Flagged{store: registry.Shared[source.Store](shared, StoreGroup), common: registry.Shared[keyrange.Common](shared, EngineGroup)}
	Define(f, Flag{Name: "s3-region", Familiar: []string{"region"}, Value: Text(&o.Region), Metavar: "NAME", Usage: "the bucket's region",
		Long: "Default: AWS_REGION, AWS_DEFAULT_REGION, the profile's, else us-east-1."})
	Define(f, Flag{Name: "s3-profile", Familiar: []string{"profile"}, Value: Text(&o.Profile), Metavar: "NAME", Usage: "the shared config profile to take credentials and region from",
		Long: "Default: the AWS SDK chain (environment, AWS_PROFILE or default, SSO, web identity, IMDS)."})
	Define(f, Flag{Name: "s3-requester-pays", Familiar: []string{"requester-pays", "request-payer"}, Value: Switch(&o.RequestPayer), Usage: "accept the request charges of a requester-pays bucket"})
	Define(f, Flag{Name: "s3-path-style", Familiar: []string{"path-style", "force-path-style"}, Value: tracked(&o.PathStyle, true, &o.pathStyleSet), Usage: "address the bucket in the path (default with --endpoint-url)"})
	Define(f, Flag{Name: "no-s3-path-style", Familiar: []string{"no-force-path-style"}, Value: tracked(&o.PathStyle, false, &o.pathStyleSet), Usage: "address the bucket in the host name, also with --endpoint-url"})
	Define(f, Flag{Name: "s3-bearer-token-command", Shown: ShownNever, Familiar: []string{"bearer-token-command"}, Value: Text(&o.BearerCommand), Metavar: "COMMAND",
		Usage: "a shell command printing a bearer token, sent instead of SigV4",
		Long:  "For GCS's XML API: 'gcloud auth print-access-token'. Never recorded: swath resume needs it again."})
	Define(f, Flag{Name: "s3-bearer-token-refresh-interval", Familiar: []string{"bearer-token-refresh-interval"}, Metavar: "DURATION", Default: source.FormatDuration(tokensource.DefaultRefresh),
		Usage: "run --s3-bearer-token-command again after this long",
		Value: Func(func(v string) error {
			d, err := source.ParseDuration(v)
			if err != nil || d <= 0 {
				return fmt.Errorf("want a positive duration such as 90s, 1h30m")
			}
			o.BearerRefresh, o.refreshSet = d, true
			return nil
		})})
	Define(f, Flag{Name: "s3-fips", Familiar: []string{"fips"}, Value: tracked(&o.FIPS, true, &o.fipsSet), Usage: "use the FIPS endpoint (s3-fips.REGION.amazonaws.com)",
		Long: "Default: AWS_USE_FIPS_ENDPOINT. Refused with --endpoint-url, where it would select nothing."})
	Define(f, Flag{Name: "s3-dualstack", Familiar: []string{"dualstack"}, Value: tracked(&o.DualStack, true, &o.dualStackSet), Usage: "use the dual-stack (IPv6) endpoint",
		Long: "Default: AWS_USE_DUALSTACK_ENDPOINT. Refused with --endpoint-url, where it would select nothing."})
	Define(f, Flag{Name: "s3-fetch-owner", Familiar: []string{"fetch-owner"}, Value: Switch(&o.FetchOwner), Usage: "fill owner_id and owner_display_name"})
	return o
}

// openS3 opens an S3 root with the command line's options (s3Flagged) or
// a library caller's (*s3.Options).
func openS3(ctx context.Context, req registry.Request) (registry.Source, error) {
	if _, _, err := s3.ParseURL(req.Root); err != nil {
		return nil, err
	}
	var opt s3.Options
	switch o := req.Options.(type) {
	case *s3Flagged: // the command line
		var err error
		if opt, err = o.options(); err != nil {
			return nil, err
		}
	case *s3.Options: // a library caller
		opt = *o
	default:
		return nil, fmt.Errorf("S3: options are %T, not *s3.Options", req.Options)
	}
	opt.Workers, opt.Warn = req.Workers, req.Warn

	if opt.AllVersions {
		o, err := s3.OpenVersions(ctx, req.Root, opt)
		if err != nil {
			return nil, err
		}
		return &s3Source[s3.Version]{registry.From(o.Bound()), o}, nil
	}
	o, err := s3.Open(ctx, req.Root, opt)
	if err != nil {
		return nil, err
	}
	switch o := o.(type) {
	case *s3.Opened[s3.Object]:
		return &s3Source[s3.Object]{registry.From(o.Bound()), o}, nil
	case *s3.OpenedTree:
		return &s3TreeSource{registry.From(o.Bound()), o, req.Warn}, nil
	default:
		return nil, fmt.Errorf("S3: unexpected inventory type %T", o)
	}
}

// treeDatasetNotice: a dataset run of the folder walk keeps no journal.
const treeDatasetNotice = "a folder-by-folder listing (--listing tree) keeps no journal: if this dataset run is interrupted, swath resume cannot continue it; run the command again"

// s3TreeSource is the folder walk as a run takes it; a dataset run is told
// that it cannot be resumed.
type s3TreeSource struct {
	registry.Bound[s3.Object]
	*s3.OpenedTree
	warn func(code, msg string)
}

func (o *s3TreeSource) Run(ctx context.Context, env registry.Env) (order.Stats, parquetout.Encoder, error) {
	if env.Router != nil && o.warn != nil {
		o.warn(source.WarnTreeListing, treeDatasetNotice)
	}
	return o.Bound.Run(ctx, env)
}

var (
	_ registry.Source   = (*s3TreeSource)(nil)
	_ source.Identified = (*s3TreeSource)(nil)
	_ source.Capable    = (*s3TreeSource)(nil)
	_ source.Reporter   = (*s3TreeSource)(nil)
	_ source.Priced     = (*s3TreeSource)(nil)
	_ source.Live       = (*s3TreeSource)(nil)
	_ source.Faulted    = (*s3TreeSource)(nil)
	_ source.Advancing  = (*s3TreeSource)(nil)
	_ source.Requested  = (*s3TreeSource)(nil)
)

// options are the S3 options the command line gives: the S3 flags, the
// shared groups' values, the AWS CLI's endpoint variables where no flag
// was given (environment), and path-style addressing for a custom
// endpoint unless a flag says otherwise. Credentials that exclude each
// other are refused.
func (o *s3Flagged) options() (s3.Options, error) {
	opt := o.Options
	if o.store != nil {
		opt.Endpoint, opt.Timeout, opt.Anonymous, opt.Trust, opt.Listing = o.store.Endpoint, o.store.Timeout, o.store.Anonymous, o.store.Trust, o.store.Listing
	}
	if o.common != nil {
		opt.Policy, opt.Admission, opt.AllVersions = o.common.Policy, o.common.Admission, o.common.AllVersions
	}
	if err := o.environment(&opt); err != nil {
		return s3.Options{}, err
	}
	if !o.pathStyleSet && opt.Endpoint != "" {
		opt.PathStyle = true // Java's default for a custom endpoint
	}

	switch {
	case o.refreshSet && opt.BearerCommand == "":
		return s3.Options{}, source.Unsupported(errors.New("--s3-bearer-token-refresh-interval requires --s3-bearer-token-command"))
	case opt.Anonymous && opt.Profile != "":
		return s3.Options{}, source.AnonymousConflict("--s3-profile")
	case opt.Anonymous && opt.BearerCommand != "":
		return s3.Options{}, source.AnonymousConflict("--s3-bearer-token-command")
	case opt.BearerCommand != "" && opt.Profile != "":
		return s3.Options{}, source.Unsupported(errors.New("--s3-bearer-token-command excludes --s3-profile: the token is the credential"))
	}
	return opt, nil
}

// environment applies the AWS CLI's endpoint variables where no flag was
// given: AWS_ENDPOINT_URL_S3, else AWS_ENDPOINT_URL, as --endpoint-url
// (unless AWS_IGNORE_CONFIGURED_ENDPOINT_URLS is true), and
// AWS_USE_FIPS_ENDPOINT and AWS_USE_DUALSTACK_ENDPOINT as --s3-fips and
// --s3-dualstack. FIPS and dual-stack select among AWS's own endpoints, so
// either with a custom endpoint is refused, whichever named them.
func (o *s3Flagged) environment(opt *s3.Options) error {
	endpoint, fips, dualStack := "--endpoint-url", "--s3-fips", "--s3-dualstack"
	if opt.Endpoint == "" && !envTrue("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS") {
		for _, v := range []string{"AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL"} {
			if opt.Endpoint = os.Getenv(v); opt.Endpoint != "" {
				endpoint = v
				break
			}
		}
	}
	if !o.fipsSet && envTrue("AWS_USE_FIPS_ENDPOINT") {
		opt.FIPS, fips = true, "AWS_USE_FIPS_ENDPOINT"
	}
	if !o.dualStackSet && envTrue("AWS_USE_DUALSTACK_ENDPOINT") {
		opt.DualStack, dualStack = true, "AWS_USE_DUALSTACK_ENDPOINT"
	}

	switch {
	case opt.Endpoint != "" && opt.FIPS:
		return source.Unsupported(fmt.Errorf("%s has no effect with %s: it selects among AWS's own S3 endpoints", fips, endpoint))
	case opt.Endpoint != "" && opt.DualStack:
		return source.Unsupported(fmt.Errorf("%s has no effect with %s: it selects among AWS's own S3 endpoints", dualStack, endpoint))
	}
	return nil
}

// envTrue reports whether environment variable v is true, read as the AWS
// SDK reads its switches (in any case).
func envTrue(v string) bool { return strings.EqualFold(os.Getenv(v), "true") }

// s3Source is an opened S3 listing as a run takes it: its rows, bound when
// it opened, go where the run's Env sends them (registry.Bound, whose
// field hides the view's Bound method); the rest is the opened view's.
type s3Source[P any] struct {
	registry.Bound[P]
	*s3.Opened[P]
}

// The registered source's whole method set: registry.Source from both
// halves, and every optional interface of the view, which an ambiguous
// or missing method would silently turn off.
var (
	_ registry.Source     = (*s3Source[s3.Object])(nil)
	_ keyrange.Registered = (*s3Source[s3.Object])(nil)
	_ source.Identified   = (*s3Source[s3.Object])(nil)
	_ source.Capable      = (*s3Source[s3.Object])(nil)
	_ source.Reporter     = (*s3Source[s3.Object])(nil)
	_ source.Priced       = (*s3Source[s3.Object])(nil)
	_ registry.Source     = (*s3Source[s3.Version])(nil)
	_ keyrange.Registered = (*s3Source[s3.Version])(nil)
	_ source.Identified   = (*s3Source[s3.Version])(nil)
	_ source.Capable      = (*s3Source[s3.Version])(nil)
	_ source.Reporter     = (*s3Source[s3.Version])(nil)
	_ source.Priced       = (*s3Source[s3.Version])(nil)
)
