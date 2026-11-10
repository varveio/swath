// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objhttp

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/counters"
	"github.com/varveio/swath/go/internal/source"
)

func TestLabel(t *testing.T) {
	for _, c := range []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, ""},
		{"other", errors.New("construction failed"), ""},
		{"canceled", context.Canceled, ""},
		{"invalid page", ErrInvalidPage, ""},
		{"credentials", &CredentialError{Err: errors.New("refused")}, "auth"},
		{"timeout", ErrTimeout, "timeout"},
		{"parse", ErrSyntax, "parse"},
		{"unsupported", source.ErrUnsupported, "unsupported"},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := Label(c.err); got != c.want {
				t.Fatalf("Label(%v) = %q, want %q", c.err, got, c.want)
			}
		})
	}
	for _, class := range []Class{ClassAuth, ClassAccessDenied, ClassThrottle, ClassTimeout, ClassParse, ClassUnsupported, ClassOther} {
		t.Run("stored/"+string(class), func(t *testing.T) {
			// A stored label survives wrapping and exhaustion even when
			// the original cause would classify differently.
			f := &Failure{Class: class, Cause: ErrSyntax}
			want := string(class)
			if class == ClassOther {
				want = ""
			}
			for _, err := range []error{f, fmt.Errorf("list: %w", f), &ExhaustedError{Attempts: 3, Err: f}} {
				if got := Label(err); got != want {
					t.Errorf("Label(%v) = %q, want %q", err, got, want)
				}
			}
		})
	}
}

func TestCanonicalRoute(t *testing.T) {
	for in, want := range map[string]string{
		"HTTP://Host:1/storage/v1/b/x/o": "http://host:1/storage/v1/b/x/o",
		"https://B.S3.Amazonaws.com/":    "https://b.s3.amazonaws.com/",
		"http://h/Base//K":               "http://h/Base//K", // path bytes kept
		"http://H":                       "http://h",
		"no-scheme":                      "no-scheme",
	} {
		if got := CanonicalRoute(in); got != want {
			t.Errorf("%q: %q, want %q", in, got, want)
		}
	}
}

// TestCredentialDeadlineIsRequeued: a transient credential failure that
// wraps a deadline (a token exchange's own timeout, the caller's context
// still live) classifies as timeout or transport, not canceled, and its
// exhaustion is requeued; a refused credential stays auth.
func TestCredentialDeadlineIsRequeued(t *testing.T) {
	for _, c := range []struct {
		err                error
		transient, timeout bool
		class              Class
	}{
		{&CredentialError{Transient: true, Err: fmt.Errorf("token: %w", context.DeadlineExceeded)}, true, false, ClassOther},
		{&CredentialError{Transient: true, Timeout: true, Err: fmt.Errorf("wait: %w", context.DeadlineExceeded)}, true, true, ClassTimeout},
		{&CredentialError{Err: fmt.Errorf("invalid_grant: %w", context.DeadlineExceeded)}, false, false, ClassAuth},
		{fmt.Errorf("list: %w", context.Canceled), false, false, ClassOther},
	} {
		f := Normalize(c.err)
		ex := &ExhaustedError{Attempts: 9, Err: f}
		if f.Transient != c.transient || f.Timeout != c.timeout || f.Class != c.class || Requeue(ex) != c.transient || Normalize(ex) != f {
			t.Errorf("%v: failure %+v requeue %v", c.err, f, Requeue(ex))
		}
	}
}

// TestCounterTable: every ClientCounter before the sentinel has a
// definition with a distinct key, and Store writes each from its own
// field: Counts filled with distinct values land at distinct IDs.
func TestCounterTable(t *testing.T) {
	defs := CounterDefs("throttle")
	seen := map[string]bool{}
	for id, d := range defs {
		if d.Key == "" || seen[d.Key] {
			t.Fatalf("counter %d: key %q missing or repeated", id, d.Key)
		}
		seen[d.Key] = true
	}
	var n Counts
	v := int64(1)
	fill := func(rv reflect.Value) {
		for i := 0; i < rv.NumField(); i++ {
			if f := rv.Field(i); f.Kind() == reflect.Int64 {
				f.SetInt(v)
				v++
			}
		}
	}
	rv := reflect.ValueOf(&n).Elem()
	fill(rv)
	fill(rv.FieldByName("Limiter"))
	fill(rv.FieldByName("Rate"))
	if int(v-1) != int(NumCounters) {
		t.Fatalf("Counts has %d values, the table %d", v-1, NumCounters)
	}
	set := counters.New(defs)
	n.Store(set, 0)
	got := map[int64]bool{}
	for id := range defs {
		x := set.Load(counters.ID(id))
		if x == 0 || got[x] {
			t.Fatalf("counter %s: value %d unset or stored twice", defs[id].Key, x)
		}
		got[x] = true
	}
}

// TestAdmissionFeatures: slow start has its own feature state, so a run
// with the limiter but without slow start reports it off, not on and
// unengaged; a supplied client has every admission feature off.
func TestAdmissionFeatures(t *testing.T) {
	for _, c := range []struct {
		a   Admission
		own bool
		off []string
	}{
		{Admission{Limiter: true}, true, []string{FeatureSlowStart, FeatureRequestRate}},
		{Admission{Limiter: true, SlowStart: true, RequestRate: 5}, true, nil},
		{Admission{SlowStart: true}, true, []string{FeatureLimiter, FeatureSlowStart, FeatureRequestRate}},
		{Admission{Limiter: true, SlowStart: true, RequestRate: 5}, false, []string{FeatureLimiter, FeatureSlowStart, FeatureRequestRate}},
	} {
		if got := c.a.Off(c.own); fmt.Sprint(got) != fmt.Sprint(c.off) {
			t.Fatalf("%+v own %v: off %v, want %v", c.a, c.own, got, c.off)
		}
	}
	for _, d := range CounterDefs("t") {
		if strings.HasPrefix(d.Key, "slow_start.") != (d.Feature == FeatureSlowStart) {
			t.Fatalf("%s: feature %q", d.Key, d.Feature)
		}
	}
}

// waitAnswer is a retryable answer naming its own wait.
type waitAnswer time.Duration

func (w waitAnswer) Error() string             { return "busy" }
func (w waitAnswer) Failure() Failure          { return Failure{Transient: true, Throttle: true} }
func (w waitAnswer) RetryAfter() time.Duration { return time.Duration(w) }

// TestRetryAfterWait: a failure carrying the service's wait (RetryAfter)
// holds the next attempt at least that long, past a backoff drawn from 1
// ms; without one the backoff alone applies.
func TestRetryAfterWait(t *testing.T) {
	r := Retry{MaxAttempts: 2, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond}
	for _, c := range []struct {
		err      error
		min, max time.Duration
	}{
		{waitAnswer(150 * time.Millisecond), 150 * time.Millisecond, 5 * time.Second},
		{fmt.Errorf("wrapped: %w", waitAnswer(150*time.Millisecond)), 150 * time.Millisecond, 5 * time.Second},
		{waitAnswer(0), 0, 100 * time.Millisecond},
	} {
		var n Counters
		var times []time.Time
		start := time.Now()
		_, err := Do(context.Background(), r, &n, func(context.Context) (int, error) {
			times = append(times, time.Now())
			if len(times) == 1 {
				return 0, c.err
			}
			return 1, nil
		})
		if err != nil || len(times) != 2 {
			t.Fatalf("%v: err %v after %d attempts", c.err, err, len(times))
		}
		if d := times[1].Sub(start); d < c.min || d > c.max {
			t.Errorf("%v: second attempt after %s, want %s to %s", c.err, d, c.min, c.max)
		}
	}
}

// observedAnswer catches repeated provider classification even when the
// resulting signals would otherwise look identical to the caller.
type observedAnswer struct{ calls int }

func (a *observedAnswer) Error() string { return "safe throttle answer" }
func (a *observedAnswer) Failure() Failure {
	a.calls++
	return Failure{Transient: true, Throttle: true, Status: 429, Code: "busy"}
}

// TestNormalizedFailureShared: admission, accounting, exhaustion, report
// and requeue must share the attempt's value, without consulting it again.
func TestNormalizedFailureShared(t *testing.T) {
	a := new(observedAnswer)
	l := NewLimiter(8)
	var counts Counters
	_, err := Do(context.Background(), Retry{MaxAttempts: 1, Limiter: l}, &counts, func(context.Context) (int, error) { return 0, a })
	var f *Failure
	var ex *ExhaustedError
	if !errors.As(err, &f) || !errors.As(err, &ex) || ex.Err != f || Normalize(err) != f || Normalize(f) != f ||
		!Requeue(err) || Classify(err) != ClassThrottle || f.Status != 429 || f.Code != "busy" || !errors.Is(err, a) {
		t.Fatalf("normalized exhaustion: %v, failure %+v", err, f)
	}
	if a.calls != 1 || counts.Load().Throttles != 1 || l.Counts().Tolerated != 1 {
		t.Fatalf("classification calls %d, counts %+v, limiter %+v", a.calls, counts.Load(), l.Counts())
	}
	if Requeue(&ExhaustedError{Attempts: 1, Err: a}) {
		t.Fatal("a raw error has no stored requeue decision")
	}
}

// TestNormalizePrecedence: SDK/URL wrappers cannot turn a terminal guard
// rejection or an invalid page into a retry, or conceal a parse failure.
func TestNormalizePrecedence(t *testing.T) {
	guard := func(err error) error {
		return fmt.Errorf("SDK wrapper: %w", &url.Error{Op: "Get", URL: "redacted", Err: &GuardError{Err: err}})
	}
	for _, c := range []struct {
		name                         string
		err                          error
		transient, throttle, timeout bool
		is                           error
	}{
		{"invalid before parse and deadline", errors.Join(ErrInvalidPage, ErrSyntax, ErrTimeout), false, false, false, ErrInvalidPage},
		{"parse before nested deadline", errors.Join(ErrSyntax, context.DeadlineExceeded), true, false, false, ErrSyntax},
		{"parse before attempt deadline", errors.Join(ErrSyntax, ErrTimeout), true, false, false, ErrSyntax},
		{"credentials before invalid", &CredentialError{Err: errors.Join(ErrInvalidPage, ErrSyntax)}, false, false, false, ErrInvalidPage},
		{"credential timeout before nested deadline", &CredentialError{Transient: true, Timeout: true, Err: context.DeadlineExceeded}, true, false, true, context.DeadlineExceeded},
		{"wrapped guard cap", guard(&TooLargeError{Limit: 10}), false, false, false, nil},
		{"wrapped guard gzip", guard(gzip.ErrHeader), false, false, false, gzip.ErrHeader},
		{"wrapped guard parse", guard(ErrSyntax), true, false, false, ErrSyntax},
		{"wrapped guard invalid", guard(ErrInvalidPage), false, false, false, ErrInvalidPage},
		{"guard cap before deadline", errors.Join(ErrTimeout, guard(&TooLargeError{Limit: 10})), false, false, false, nil},
		{"guard gzip before deadline", errors.Join(ErrTimeout, guard(gzip.ErrHeader)), false, false, false, gzip.ErrHeader},
		{"guard cut body", guard(io.ErrUnexpectedEOF), true, false, false, io.ErrUnexpectedEOF},
		{"guard cut body deadline", errors.Join(ErrTimeout, guard(io.ErrUnexpectedEOF)), true, false, true, io.ErrUnexpectedEOF},
		{"attempt deadline", errors.Join(ErrTimeout, context.DeadlineExceeded), true, false, true, ErrTimeout},
		{"caller cancellation", fmt.Errorf("list: %w", context.Canceled), false, false, false, context.Canceled},
		{"caller deadline", context.DeadlineExceeded, false, false, false, context.DeadlineExceeded},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := Normalize(c.err)
			if f.Transient != c.transient || f.Throttle != c.throttle || f.Timeout != c.timeout || (c.is != nil && !errors.Is(f, c.is)) {
				t.Fatalf("failure %+v from %v", f, c.err)
			}
		})
	}
}

// TestCallerCancellationPrecedesCredentialFailure: a canceled caller must
// not report or feed back the credential service's simultaneous timeout.
func TestCallerCancellationPrecedesCredentialFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var counts Counters
	_, err := Do(ctx, Retry{MaxAttempts: 3, Limiter: NewLimiter(8)}, &counts, func(context.Context) (int, error) {
		cancel()
		return 0, &CredentialError{Transient: true, Timeout: true, Err: context.DeadlineExceeded}
	})
	var f *Failure
	if !errors.Is(err, context.Canceled) || !errors.As(err, &f) || f.Transient || f.Timeout || Requeue(err) || counts.Load().Timeouts != 0 {
		t.Fatalf("canceled caller: %v (%+v), counts %+v", err, f, counts.Load())
	}
}

func TestTextConnectionReset(t *testing.T) {
	for _, err := range []error{errors.New("read: connection reset by peer"), fmt.Errorf("send: %w", errors.New("connection reset"))} {
		f := Normalize(err)
		if !Transport(err) || !f.Transient || f.Timeout || f.Throttle || f.Class != ClassOther || !Requeue(&ExhaustedError{Attempts: 2, Err: f}) {
			t.Fatalf("text-only reset: %+v", f)
		}
	}
	if Transport(errors.New("unrelated failure")) {
		t.Fatal("unrelated text is transport")
	}
}
