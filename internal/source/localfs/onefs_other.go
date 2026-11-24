// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !darwin

package localfs

const filesystemMechanism = "device"

// Linux retains the device boundary, including same-device bind mounts.
// Platforms without stat devices still reject OneFilesystem in rootDevice.
func filesystemID(int) ([2]int32, error) { return [2]int32{}, nil }
