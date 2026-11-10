// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/spf13/pflag"
)

// TestSubcommandHelp: every command's --help prints usable help on stdout
// and is a help request (exit 0), not a usage error: diag, show, and
// resume with each accepted flag's description.
func TestSubcommandHelp(t *testing.T) {
	for _, c := range []struct {
		args []string
		want []string
	}{
		{[]string{"diag", "--help"}, []string{"swath diag list SOURCE", "swath diag count SOURCE"}},
		{[]string{"show", "--help"}, []string{"swath show DIR", "--journal"}},
		{[]string{"resume", "--help"}, []string{"swath resume DIR", "-workers", "--s3-bearer-token-command", "-progress"}},
		{[]string{"list", "--help"}, []string{"SOURCE"}},
	} {
		var out bytes.Buffer
		err := dispatch(context.Background(), c.args, &out)
		if !errors.Is(err, pflag.ErrHelp) {
			t.Errorf("%v: err %v, want a help request", c.args, err)
		}
		var u usageError
		if errors.As(err, &u) {
			t.Errorf("%v: a usage error", c.args)
		}
		for _, w := range c.want {
			if !strings.Contains(out.String(), w) {
				t.Errorf("%v: help lacks %q:\n%s", c.args, w, out.String())
			}
		}
	}
}
