// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build purego

package parquetout

// puregoBuild: built with the purego tag, which selects parquet-go's
// generic code on every platform (footerOnlyGeneric).
const puregoBuild = true
