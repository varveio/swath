// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"io/fs"
	"testing"
)

const (
	mountV2 = "30 23 0:26 / /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime shared:4 - cgroup2 cgroup2 rw,nsdelegate\n"
	mountV1 = "25 23 0:22 / /sys/fs/cgroup/memory rw,nosuid shared:9 - cgroup cgroup rw,memory\n" +
		"26 23 0:23 / /sys/fs/cgroup/cpu,cpuacct rw,nosuid shared:10 - cgroup cgroup rw,cpu,cpuacct\n"
)

func files(m map[string]string) func(string) ([]byte, error) {
	return func(p string) ([]byte, error) {
		if v, ok := m[p]; ok {
			return []byte(v), nil
		}
		return nil, fs.ErrNotExist
	}
}

func TestCgroupLimit(t *testing.T) {
	for _, c := range []struct {
		name      string
		cgroup    string
		mountinfo string
		files     map[string]string
		want      int64
	}{
		{"v2 leaf limit", "0::/user.slice/app.scope\n", mountV2,
			map[string]string{"/sys/fs/cgroup/user.slice/app.scope/memory.max": "1073741824\n"}, 1 << 30},
		{"v2 ancestor tighter than the leaf", "0::/kubepods/pod1/ctr\n", mountV2,
			map[string]string{
				"/sys/fs/cgroup/kubepods/pod1/ctr/memory.max": "max\n",
				"/sys/fs/cgroup/kubepods/pod1/memory.max":     "536870912\n",
				"/sys/fs/cgroup/kubepods/memory.max":          "8589934592\n",
			}, 512 << 20},
		{"v2 unlimited everywhere", "0::/a/b\n", mountV2,
			map[string]string{"/sys/fs/cgroup/a/b/memory.max": "max\n", "/sys/fs/cgroup/a/memory.max": "max"}, 0},
		{"v2 namespaced: the container sees its cgroup as /", "0::/\n", mountV2,
			map[string]string{"/sys/fs/cgroup/memory.max": "268435456\n"}, 256 << 20},
		{"v2 mount root below /: the path is relative to it",
			"0::/docker/abc\n", "40 30 0:26 /docker /sys/fs/cgroup rw - cgroup2 cgroup2 rw\n",
			map[string]string{"/sys/fs/cgroup/abc/memory.max": "2147483648", "/sys/fs/cgroup/docker/abc/memory.max": "1"}, 2 << 30},
		{"v2 path outside the mount's root: the mount is skipped (its root is not an ancestor)",
			"0::/other\n", "40 30 0:26 /docker /sys/fs/cgroup rw - cgroup2 cgroup2 rw\n",
			map[string]string{"/sys/fs/cgroup/memory.max": "4294967296", "/sys/fs/cgroup/other/memory.max": "1"}, 0},
		{"v2 root that is only a string prefix of the path", "0::/dockerx/y\n", "40 30 0:26 /docker /sys/fs/cgroup rw - cgroup2 cgroup2 rw\n",
			map[string]string{"/sys/fs/cgroup/memory.max": "4294967296", "/sys/fs/cgroup/x/y/memory.max": "1"}, 0},
		{"a sibling's bind mount does not bind, the sibling's mount first", "0::/tenant/job\n",
			"40 30 0:26 /other /mnt/other rw - cgroup2 cgroup2 rw\n" + "41 30 0:26 / /sys/fs/cgroup rw - cgroup2 cgroup2 rw\n",
			map[string]string{"/mnt/other/memory.max": "67108864", "/sys/fs/cgroup/tenant/job/memory.max": "17179869184"}, 16 << 30},
		{"a sibling's bind mount does not bind, the sibling's mount second", "0::/tenant/job\n",
			"41 30 0:26 / /sys/fs/cgroup rw - cgroup2 cgroup2 rw\n" + "40 30 0:26 /other /mnt/other rw - cgroup2 cgroup2 rw\n",
			map[string]string{"/mnt/other/memory.max": "67108864", "/sys/fs/cgroup/tenant/job/memory.max": "17179869184"}, 16 << 30},
		{"v1 memory hierarchy", "4:memory:/user.slice\n3:cpu,cpuacct:/user.slice\n", mountV1,
			map[string]string{"/sys/fs/cgroup/memory/user.slice/memory.limit_in_bytes": "3221225472\n"}, 3 << 30},
		{"v1 unlimited sentinel", "4:memory:/\n", mountV1,
			map[string]string{"/sys/fs/cgroup/memory/memory.limit_in_bytes": "9223372036854771712\n"}, 0},
		{"v1 combined controllers", "4:cpu,memory:/x\n", "25 23 0:22 / /sys/fs/cgroup/cm rw - cgroup cgroup rw,cpu,memory\n",
			map[string]string{"/sys/fs/cgroup/cm/x/memory.limit_in_bytes": "1048576"}, 1 << 20},
		{"hybrid: v1 memory and v2 both limit, the least wins", "4:memory:/s\n0::/s\n", mountV1 + "31 23 0:27 / /sys/fs/cgroup/unified rw - cgroup2 cgroup2 rw\n",
			map[string]string{
				"/sys/fs/cgroup/memory/s/memory.limit_in_bytes": "2147483648",
				"/sys/fs/cgroup/unified/s/memory.max":           "1073741824",
			}, 1 << 30},
		{"escaped mount point", "0::/a\n", "30 23 0:26 / /mnt/cg\\040two rw - cgroup2 cgroup2 rw\n",
			map[string]string{"/mnt/cg two/a/memory.max": "65536"}, 65536},
		{"a second mount sees an ancestor's limit the first hides",
			"0::/tenant/job\n", "40 30 0:26 /tenant/job /sys/fs/cgroup rw - cgroup2 cgroup2 rw\n" +
				"41 30 0:26 / /host/cgroup ro - cgroup2 cgroup2 rw\n",
			map[string]string{
				"/sys/fs/cgroup/memory.max":          "max",
				"/host/cgroup/tenant/job/memory.max": "max",
				"/host/cgroup/tenant/memory.max":     "1073741824",
			}, 1 << 30},
		{"an unreadable first mount, a readable second", "0::/a\n",
			"40 30 0:26 / /gone rw - cgroup2 cgroup2 rw\n" + "41 30 0:26 / /sys/fs/cgroup rw - cgroup2 cgroup2 rw\n",
			map[string]string{"/sys/fs/cgroup/a/memory.max": "8192"}, 8192},
		{"namespace: a sibling of the namespace root through an inherited /.. mount", "0::/../job\n",
			"40 30 0:26 /.. /sys/fs/cgroup rw - cgroup2 cgroup2 rw\n",
			map[string]string{"/sys/fs/cgroup/job/memory.max": "1073741824", "/sys/fs/cgroup/memory.max": "max"}, 1 << 30},
		{"namespace: a cgroup above a / mount is neither walked into nor bound by the mount", "0::/../job\n", mountV2,
			map[string]string{"/sys/fs/cgroup/memory.max": "4294967296", "/sys/fs/job/memory.max": "1", "/sys/fs/cgroup/job/memory.max": "2"}, 0},
		{"nested namespace: the process at its namespace root, an inherited /.. mount binds", "0::/\n",
			"40 30 0:26 /.. /sys/fs/cgroup rw - cgroup2 cgroup2 rw\n",
			map[string]string{"/sys/fs/cgroup/memory.max": "1073741824"}, 1 << 30},
		{"nested namespace: a child of the namespace root, the /.. mount's own limit binds", "0::/x\n",
			"40 30 0:26 /.. /sys/fs/cgroup rw - cgroup2 cgroup2 rw\n",
			map[string]string{"/sys/fs/cgroup/memory.max": "1073741824", "/sys/fs/cgroup/x/memory.max": "1"}, 1 << 30},
		{"nested namespace: a deeper inherited ancestor binds", "0::/x\n",
			"40 30 0:26 /../.. /sys/fs/cgroup rw - cgroup2 cgroup2 rw\n",
			map[string]string{"/sys/fs/cgroup/memory.max": "2147483648"}, 2 << 30},
		{"nested namespace, v1: an inherited /.. memory mount binds", "4:memory:/\n",
			"25 23 0:22 /.. /sys/fs/cgroup/memory rw - cgroup cgroup rw,memory\n",
			map[string]string{"/sys/fs/cgroup/memory/memory.limit_in_bytes": "536870912"}, 512 << 20},
		{"no cgroup mount", "0::/a\n", "1 0 8:1 / / rw - ext4 /dev/sda1 rw\n", nil, 0},
		{"malformed values are none", "0::/a\n", mountV2,
			map[string]string{"/sys/fs/cgroup/a/memory.max": "lots", "/sys/fs/cgroup/memory.max": "-5"}, 0},
		{"malformed lines are skipped", "garbage\n0:\n\n0::/a\n", "short - \nno separator here\n" + mountV2,
			map[string]string{"/sys/fs/cgroup/a/memory.max": "4096"}, 4096},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := cgroupLimit(c.cgroup, c.mountinfo, files(c.files)); got != c.want {
				t.Fatalf("cgroupLimit = %d, want %d", got, c.want)
			}
		})
	}
}

func TestMountRel(t *testing.T) {
	for _, c := range []struct {
		p, root, rel string
		ok           bool
	}{
		{"/a/b", "/", "/a/b", true},
		{"/", "/", "/", true},
		{"/a/b", "/a", "/b", true},
		{"/a", "/a", "/", true},
		{"/ab", "/a", "", false},
		{"/../job", "/..", "/job", true},
		{"/../job", "/", "", false},
		{"/x", "/..", "/", true},
		{"/", "/..", "/", true},
		{"/a/b", "/../..", "/", true},
		{"/../../a", "/..", "", false},
		{"/../x", "/../y", "", false},
		{"/../x/y", "/../x", "/y", true},
		{"/a/../b", "/", "", false},
	} {
		if rel, ok := mountRel(c.p, c.root); rel != c.rel || ok != c.ok {
			t.Errorf("mountRel(%q, %q) = %q, %v; want %q, %v", c.p, c.root, rel, ok, c.rel, c.ok)
		}
	}
}

func TestUnescapeMount(t *testing.T) {
	for in, want := range map[string]string{`/a\040b`: "/a b", `/a\134b`: `/a\b`, `/plain`: "/plain", `/trail\04`: `/trail\04`} {
		if got := unescapeMount(in); got != want {
			t.Errorf("unescapeMount(%q) = %q, want %q", in, got, want)
		}
	}
}
