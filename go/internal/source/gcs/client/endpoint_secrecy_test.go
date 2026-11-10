// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"strings"
	"testing"
)

func TestEndpointSecrecy(t *testing.T) {
	for _, ep := range []string{
		"https://host/?sig=SECRET", "https://host/#SECRET", "https://host?", "https://host#",
		"https://user:SECRET@host", "host/?sig=SECRET", "ftp://host/SECRET", "https:///SECRET",
	} {
		t.Run(ep, func(t *testing.T) {
			_, err := Endpoint(ep)
			if err == nil {
				t.Fatal("endpoint accepted")
			}
			if strings.Contains(err.Error(), "SECRET") || strings.Contains(err.Error(), "host/?") {
				t.Fatalf("endpoint leaked: %v", err)
			}
		})
	}
}
