// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"errors"
)

func open(context.Context, string, Options) (Store, Metadata, error) {
	return nil, Metadata{}, errors.New("store: not implemented")
}
