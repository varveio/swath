// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import "encoding/base64"

func base64Std(b []byte) string { return base64.StdEncoding.EncodeToString(b) }
