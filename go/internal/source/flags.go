// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package source

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/pflag"
)

// DefaultRequestTimeout bounds one object-store request attempt.
const DefaultRequestTimeout = 30 * time.Second

// A FlagGroup is a flag group several providers share (the store's, the
// range engine's): the command registers it once and passes its value to
// every provider that names it (registry.Factory.Shared), so the flags are
// spelled the same for every provider and owned by each of them. A group
// is identified by its address: providers name the one declared value
// (StoreGroup, keyrange.Group), so two groups never alias by name.
type FlagGroup struct {
	// Name names the group in messages.
	Name string
	// Flags registers the group's flags on fs (Define) and returns the
	// value they fill when fs is parsed; diag adds its diagnostic flags
	// (swath diag list only).
	Flags func(fs *pflag.FlagSet, diag bool) any
	// Operational are the group's flags a resume may change: how requests
	// are authenticated, timed and admitted, which no identity holds.
	Operational []string
	// URLs are the group's flags whose value is a URL: a recorded command
	// line redacts one that carries userinfo, or does not parse
	// (registry.RedactArgv).
	URLs []string
}

// SharedValues are the values of the shared groups a provider names, by
// group, as its flags receive them (registry.Factory.Flags).
type SharedValues map[*FlagGroup]any

// Shared is group g's value in values, the *T its Flags returned. A
// provider asking for a group it does not name, or for another type than
// the group's, is a programming error, so Shared panics rather than
// dropping the group's flags silently.
func Shared[T any](values SharedValues, g *FlagGroup) *T {
	v, ok := values[g]
	if !ok {
		panic(fmt.Sprintf("source: flag group %q is not among the groups the factory names (registry.Factory.Shared)", g.Name))
	}
	t, ok := v.(*T)
	if !ok {
		panic(fmt.Sprintf("source: flag group %q holds %T, not %T", g.Name, v, t))
	}
	return t
}

// The help groups a flag is shown under (Flag.Group), in the help's order
// apart from the providers' own, which follow GroupStores.
const (
	GroupTotals      = "Totals"
	GroupOutput      = "Output"
	GroupFilters     = "Filters"
	GroupStores      = "Object stores"
	GroupPerformance = "Performance"
	GroupLimits      = "Limits"
	GroupReporting   = "Reporting"
	GroupDiagnostics = "Diagnostics"
)

// A Flag is one entry of the command line's flag table: what parsing
// fills, and what the help, the docs and shell completion say of it.
// Define adds it to a flag set; Described reads it back.
type Flag struct {
	Name  string      // the long name, without dashes
	Short string      // the one-letter form, or ""
	Value pflag.Value // what parsing fills
	// Metavar is the value's placeholder in the help: N, SIZE, DURATION,
	// TIME, PATH, FILE, DIR, URL, REGEX, GLOB, NAME, COMMAND or TOKEN, or
	// an enum's choices (auto|table|tsv|jsonl). A flag without one is a
	// switch: given alone, it is set to true.
	Metavar string
	Usage   string // one line
	Long    string // what --help adds under the line: when to change it, its constraints
	Default string // the default as a user would type it; "" shows none
	// Group is the help group (GroupOutput, ...); "" is the registering
	// provider's own group, or GroupStores for a shared group's flag.
	Group string
	// Commands are the commands that take the flag ("list", "du", "diag
	// list", "diag count"); nil is every command that registers it.
	Commands []string
	// Familiar are other tools' spellings of the flag (region for
	// --s3-region): the unknown-flag error suggests the flag for them.
	Familiar []string
}

// The annotations Define records a Flag's help fields under.
const (
	annMetavar  = "swath.metavar"
	annLong     = "swath.long"
	annDefault  = "swath.default"
	annGroup    = "swath.group"
	annCommands = "swath.commands"
	annFamiliar = "swath.familiar"
)

// Define adds f to fs and returns the flag set's entry for it.
func Define(fs *pflag.FlagSet, f Flag) *pflag.Flag {
	fl := fs.VarPF(f.Value, f.Name, f.Short, f.Usage)
	if f.Metavar == "" {
		fl.NoOptDefVal = "true"
	}
	fl.Annotations = map[string][]string{
		annMetavar: {f.Metavar}, annLong: {f.Long}, annDefault: {f.Default}, annGroup: {f.Group}, annCommands: f.Commands, annFamiliar: f.Familiar,
	}
	return fl
}

// Described is fl's table entry as Define recorded it (the name, short
// form, value and usage from fl itself).
func Described(fl *pflag.Flag) Flag {
	a := func(k string) string {
		if v := fl.Annotations[k]; len(v) > 0 {
			return v[0]
		}
		return ""
	}
	return Flag{Name: fl.Name, Short: fl.Shorthand, Value: fl.Value, Usage: fl.Usage, Metavar: a(annMetavar), Long: a(annLong),
		Default: a(annDefault), Group: a(annGroup), Commands: fl.Annotations[annCommands], Familiar: fl.Annotations[annFamiliar]}
}

// SetGroup places fl in group when its definition named none: the
// command's default for a provider's or a shared group's flags.
func SetGroup(fl *pflag.Flag, group string) {
	if fl.Annotations == nil {
		fl.Annotations = map[string][]string{}
	}
	if v := fl.Annotations[annGroup]; len(v) == 0 || v[0] == "" {
		fl.Annotations[annGroup] = []string{group}
	}
}

// Switch is a flag value that sets *p to true when given (--x), or to
// the value given with '=' (--x=false), which the help never shows.
func Switch(p *bool) pflag.Value { return (*switchValue)(p) }

// Off is the value of a --no-x flag: given, it sets *p to false.
func Off(p *bool) pflag.Value { return offValue{p} }

type switchValue bool

func (s *switchValue) Set(v string) error {
	b, err := strconv.ParseBool(v)
	if err != nil {
		return errors.New("want true or false")
	}
	*s = switchValue(b)
	return nil
}
func (s *switchValue) String() string { return strconv.FormatBool(bool(*s)) }
func (s *switchValue) Type() string   { return "bool" }

type offValue struct{ p *bool }

func (o offValue) Set(v string) error {
	b, err := strconv.ParseBool(v)
	if err != nil {
		return errors.New("want true or false")
	}
	*o.p = !b
	return nil
}
func (o offValue) String() string { return strconv.FormatBool(!*o.p) }
func (o offValue) Type() string   { return "bool" }

// Text is a string flag value.
func Text(p *string) pflag.Value { return (*textValue)(p) }

type textValue string

func (t *textValue) Set(v string) error { *t = textValue(v); return nil }
func (t *textValue) String() string     { return string(*t) }
func (t *textValue) Type() string       { return "string" }

// Duration is a duration flag value (ParseDuration), never negative (zero
// keeps each flag's documented meaning).
func Duration(p *time.Duration) pflag.Value { return (*durationValue)(p) }

type durationValue time.Duration

// errDuration is a duration flag's refusal of a value it cannot read.
var errDuration = errors.New("want a duration such as 90s, 1h30m, 2d")

// Days and weeks, the units ParseDuration adds to Go's.
const (
	day  = 24 * time.Hour
	week = 7 * day
)

// ParseDuration is Go's duration syntax (90s, 1h30m) with d (24h) and w
// (7d) besides: 2d, 1w, 1d12h, 1.5d.
func ParseDuration(v string) (time.Duration, error) {
	if !strings.ContainsAny(v, "dw") {
		return time.ParseDuration(v)
	}
	var total time.Duration
	for rest := v; rest != ""; {
		i := strings.IndexFunc(rest, func(r rune) bool { return r != '.' && (r < '0' || r > '9') })
		if i <= 0 {
			return 0, fmt.Errorf("invalid duration %q", v)
		}
		j := i + strings.IndexFunc(rest[i:], func(r rune) bool { return r == '.' || r >= '0' && r <= '9' })
		if j < i {
			j = len(rest)
		}
		num, unit := rest[:i], rest[i:j]
		var d time.Duration
		switch unit {
		case "d", "w":
			u := day
			if unit == "w" {
				u = week
			}
			f, err := strconv.ParseFloat(num, 64)
			x := f * float64(u)
			if err != nil || x >= 1<<63 {
				return 0, fmt.Errorf("invalid duration %q", v)
			}
			d = time.Duration(x)
		default:
			var err error
			if d, err = time.ParseDuration(num + unit); err != nil {
				return 0, fmt.Errorf("invalid duration %q", v)
			}
		}
		if total+d < total {
			return 0, fmt.Errorf("invalid duration %q", v)
		}
		total += d
		rest = rest[j:]
	}
	return total, nil
}

func (d *durationValue) Set(v string) error {
	x, err := ParseDuration(v)
	switch {
	case err != nil:
		return errDuration
	case x < 0:
		return errors.New("negative")
	}
	*d = durationValue(x)
	return nil
}
func (d *durationValue) String() string { return FormatDuration(time.Duration(*d)) }
func (d *durationValue) Type() string   { return "duration" }

// FormatDuration is d as a user would type it: 10m, 1h30m, 30s, 0.
func FormatDuration(d time.Duration) string {
	s := d.String()
	if strings.HasSuffix(s, "m0s") {
		s = s[:len(s)-2]
	}
	if strings.HasSuffix(s, "h0m") {
		s = s[:len(s)-2]
	}
	if s == "0s" {
		return "0"
	}
	return s
}

// Count is a whole-number flag value of at least min (0 where 0 is
// documented, 1 for a count that must be positive).
func Count(p *int, min int) pflag.Value { return &countValue{p, min} }

// Count64 is Count for an int64.
func Count64(p *int64, min int64) pflag.Value { return &count64Value{p, min} }

type countValue struct {
	p   *int
	min int
}

func (c *countValue) Set(v string) error {
	n, err := strconv.Atoi(v)
	if err != nil || n < c.min {
		return wantCount(c.min)
	}
	*c.p = n
	return nil
}
func (c *countValue) String() string { return strconv.Itoa(*c.p) }
func (c *countValue) Type() string   { return "int" }

type count64Value struct {
	p   *int64
	min int64
}

func (c *count64Value) Set(v string) error {
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < c.min {
		return wantCount(int(c.min))
	}
	*c.p = n
	return nil
}
func (c *count64Value) String() string { return strconv.FormatInt(*c.p, 10) }
func (c *count64Value) Type() string   { return "int" }

func wantCount(min int) error {
	if min > 0 {
		return fmt.Errorf("want a whole number of at least %d", min)
	}
	return errors.New("want a whole number, 0 or more")
}

// Counter is a flag counted each time it is given (-v -v, -vv).
func Counter(p *int) pflag.Value { return (*counterValue)(p) }

type counterValue int

func (c *counterValue) Set(v string) error {
	b, err := strconv.ParseBool(v)
	if err != nil {
		return errors.New("takes no value")
	}
	if b {
		*c++
	}
	return nil
}
func (c *counterValue) String() string { return strconv.Itoa(int(*c)) }
func (c *counterValue) Type() string   { return "count" }

// Func is a flag value that calls set with each value given.
func Func(set func(string) error) pflag.Value { return funcValue(set) }

type funcValue func(string) error

func (f funcValue) Set(v string) error { return f(v) }
func (f funcValue) String() string     { return "" }
func (f funcValue) Type() string       { return "string" }

// Store is the store group's value: what every object store takes alike.
// Each provider reads it into its own options.
type Store struct {
	// Endpoint replaces the provider's own service (an emulator, a
	// compatible store); "" is the service itself.
	Endpoint string
	// Timeout bounds each request; one that exceeds it is retried.
	Timeout time.Duration
	// Anonymous sends no credentials: the one way an object store is
	// listed unauthenticated. Without it a source with no usable
	// credentials fails (class auth) before any listing request.
	Anonymous bool
	// Trust is how the service's TLS certificate is verified.
	Trust Trust
}

// The store group's flag names.
const (
	endpointFlag       = "endpoint-url"
	requestTimeoutFlag = "request-timeout"
	anonymousFlag      = "anonymous"
	caBundleFlag       = "ca-bundle"
	noVerifyFlag       = "no-verify-ssl"
)

// StoreGroup is the flag group every object store provider names: one
// --endpoint-url, --request-timeout, --anonymous, --ca-bundle and
// --no-verify-ssl, whichever provider owns the root. --endpoint-url is part
// of each provider's identity (a resume refuses another); the timeout,
// anonymity and TLS trust are operational.
var StoreGroup = &FlagGroup{
	Name: "store",
	Flags: func(fs *pflag.FlagSet, _ bool) any {
		s := &Store{Timeout: DefaultRequestTimeout}
		Define(fs, Flag{Name: anonymousFlag, Familiar: []string{"no-sign-request"}, Value: Switch(&s.Anonymous), Usage: "send no credentials (a public bucket or container)",
			Long: "Ignores any configured. Without it a source with no usable credentials fails (auth) before listing."})
		Define(fs, Flag{Name: endpointFlag, Familiar: []string{"endpoint"}, Value: Text(&s.Endpoint), Metavar: "URL", Usage: "the service URL in place of the provider's own (emulators, MinIO)",
			Long: "S3: path-style addressing unless --no-s3-path-style; default AWS_ENDPOINT_URL_S3, else AWS_ENDPOINT_URL. GCS: the JSON API base URL (fake-gcs-server). Azure Blob: the blob service URL, the account then its first path segment (Azurite). User information, a query and a fragment are refused. Part of the resume identity."})
		Define(fs, Flag{Name: caBundleFlag, Value: Text(&s.Trust.CABundle), Metavar: "FILE", Usage: "trust the certificates in this PEM file besides the system's",
			Long: "For a service behind a private certificate authority. S3: default AWS_CA_BUNDLE."})
		Define(fs, Flag{Name: noVerifyFlag, Value: Switch(&s.Trust.NoVerify), Usage: "do not verify the service's TLS certificate (warns)",
			Long: "For a test endpoint only. A resume replays it, and warns again."})
		Define(fs, Flag{Name: requestTimeoutFlag, Value: Duration(&s.Timeout), Metavar: "DURATION", Usage: "per-request timeout; a request over it is retried",
			Default: FormatDuration(DefaultRequestTimeout), Group: GroupLimits,
			Long: "Raise it for a slow endpoint or emulator whose pages take longer to answer."})
		return s
	},
	Operational: []string{anonymousFlag, requestTimeoutFlag, caBundleFlag, noVerifyFlag},
	URLs:        []string{endpointFlag},
}

// AllVersionsFlag names the range engine's switch that lists every version
// of every key. The command reads it by name for the terminal's nouns (a
// versions run counts rows, not keys) without importing the engine.
const AllVersionsFlag = "all-versions"
