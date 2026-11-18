// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package cli is what the command line composes providers with: the flag
// table (Flag, Define, Described and the flag values), the help groups,
// the flag groups the providers share (StoreGroup, EngineGroup), which
// fill the plain configuration of package source and of the range engine,
// and each provider's registration, one file each (s3.go, gcs.go,
// azblob.go, localfs.go). A registration defines the provider's own flags
// with the table, turns them into the provider's typed options, names the
// groups it takes in its registry.Factory, registered from this package's
// init, and joins the provider's opened view with the run's writers
// (registry.From); the command registers each group once. Importing this
// package registers every provider. No provider, nor anything a provider is
// built from, imports it (TestProviderImportBoundary).
package cli

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/spf13/pflag"

	"github.com/varveio/swath/internal/source"
)

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
	// Shown is how a recorded or reported command line shows the value:
	// as typed (""), as a URL (ShownURL: one that may carry a credential,
	// its userinfo, query and fragment redacted) or never (ShownNever: a
	// secret).
	Shown string
}

// The values of Flag.Shown.
const (
	ShownURL   = "url"
	ShownNever = "never"
)

// The annotations Define records a Flag's help fields under.
const (
	annMetavar  = "swath.metavar"
	annLong     = "swath.long"
	annDefault  = "swath.default"
	annGroup    = "swath.group"
	annCommands = "swath.commands"
	annFamiliar = "swath.familiar"
	annShown    = "swath.shown"
)

// Define adds f to fs and returns the flag set's entry for it.
func Define(fs *pflag.FlagSet, f Flag) *pflag.Flag {
	fl := fs.VarPF(f.Value, f.Name, f.Short, f.Usage)
	if f.Metavar == "" {
		fl.NoOptDefVal = "true"
	}
	fl.Annotations = map[string][]string{
		annMetavar: {f.Metavar}, annLong: {f.Long}, annDefault: {f.Default}, annGroup: {f.Group}, annCommands: f.Commands, annFamiliar: f.Familiar, annShown: {f.Shown},
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
		Default: a(annDefault), Group: a(annGroup), Commands: fl.Annotations[annCommands], Familiar: fl.Annotations[annFamiliar], Shown: a(annShown)}
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

// Duration is a duration flag value (source.ParseDuration), never
// negative (zero keeps each flag's documented meaning).
func Duration(p *time.Duration) pflag.Value { return (*durationValue)(p) }

type durationValue time.Duration

// errDuration is a duration flag's refusal of a value it cannot read.
var errDuration = errors.New("want a duration such as 90s, 1h30m, 2d")

func (d *durationValue) Set(v string) error {
	x, err := source.ParseDuration(v)
	switch {
	case err != nil:
		return errDuration
	case x < 0:
		return errors.New("negative")
	}
	*d = durationValue(x)
	return nil
}
func (d *durationValue) String() string { return source.FormatDuration(time.Duration(*d)) }
func (d *durationValue) Type() string   { return "duration" }

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
