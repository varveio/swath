#!/usr/bin/env python3
"""Exercise cache deletion boundaries and the actual change/matrix shell in ci.yml."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = (ROOT / '.github/workflows/ci.yml').read_text()


def run_step(name):
    # Extract literal shell without a YAML dependency on the runner.
    step = WORKFLOW.split('      - name: ' + name + '\n', 1)[1]
    body = step.split('        run: |\n', 1)[1]
    lines = []
    for line in body.splitlines():
        if line and not line.startswith('          '):
            break
        lines.append(line[10:])
    return '\n'.join(lines)


class PolicyTests(unittest.TestCase):
    def test_cache_plan(self):
        def cache(i, key, ref='refs/heads/main', version='v1'):
            return dict(id=i, key=key, ref=ref, version=version,
                        created_at=f'2026-11-10T00:00:{i:02d}Z', size_in_bytes=100)
        build = 'go-build-Linux-ARM64-go-race-cmd-'
        dep = 'a' * 64
        sha = 'b' * 40
        codeql = 'codeql-dependencies-1-Linux-java-'
        overlay = 'codeql-overlay-base-database-1-abcdef-go-2.27.4-'
        rows = [
            cache(1, build + dep + '-' + sha),
            cache(2, build + 'c' * 64 + '-' + sha),
            cache(3, build + dep + '-' + sha, version='v2'),
            cache(4, build + dep + '-' + sha, ref='refs/pull/1/merge'),
            cache(5, codeql + dep), cache(6, codeql + 'c' * 64),
            cache(7, codeql + dep, ref='refs/pull/1/head'),
            cache(8, codeql + dep, ref='refs/pull/1/merge'),
            cache(9, 'unrelated-buildkit-cache'),
            cache(10, 'go-mod-Windows-X64-' + dep),
            cache(11, 'go-mod-Windows-X64-' + 'c' * 64),
            cache(12, overlay + sha + '-123'), cache(13, overlay + sha + '-124'),
            cache(14, 'codeql-dependencies-unrecognized'),
            cache(15, build.replace('cmd-', 'cmd-kill-') + dep + '-' + sha),
            cache(16, codeql + dep, ref='refs/heads/feature'),
        ]
        # API pagination can duplicate a row; never delete the sole retained copy.
        rows += [rows[1]]
        result = subprocess.run([str(ROOT / 'scripts/ci/cache-prune.sh'), '--plan'],
                                input=json.dumps(rows[::-1]), text=True, capture_output=True, check=True)
        plan = json.loads(result.stdout)
        deleted = {x['id'] for family in plan for x in family['delete']}
        kept = {family['keep']['id'] for family in plan if family['keep']}
        self.assertEqual(deleted, {1, 5, 7, 8, 10, 12})
        self.assertTrue({2, 3, 6, 11, 13, 15} <= kept)
        self.assertFalse(deleted & kept)

    def test_changes_and_matrix(self):
        with tempfile.TemporaryDirectory() as temp:
            cwd = Path(temp)
            subprocess.run(['git', 'init', '-q', temp], check=True)
            for args in [('user.email', 'ci@example.invalid'), ('user.name', 'CI')]:
                subprocess.run(['git', 'config', *args], cwd=temp, check=True)
            (cwd / 'base').write_text('base')
            subprocess.run(['git', 'add', '.'], cwd=temp, check=True)
            subprocess.run(['git', 'commit', '-qm', 'base'], cwd=temp, check=True)
            base = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=temp, text=True).strip()
            output = cwd / 'output'
            env = dict(os.environ, EVENT='pull_request', BASE=base, GITHUB_OUTPUT=str(output))
            cases = [
                (['go/internal/x.go', 'CHANGELOG.md'], 'false', 'true'),
                (['README.md', 'SECURITY.md'], 'false', 'false'),
                (['docs/fixtures.json'], 'true', 'false'),
                (['docs/README.md'], 'true', 'false'),
                (['CHANGELOG.md.java'], 'true', 'false'),
                (['build.gradle.kts'], 'true', 'false'),
                (['.github/workflows/ci.yml'], 'true', 'true'),
                (['swath-core/src/X.java', 'go/X.java'], 'true', 'true'),
                ([], 'true', 'true'),
            ]
            for paths, java, go in cases:
                subprocess.run(['git', 'reset', '--hard', '-q', base], cwd=temp, check=True)
                for path in paths:
                    target = cwd / path
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_text('changed')
                subprocess.run(['git', 'add', '.'], cwd=temp, check=True)
                subprocess.run(['git', 'commit', '--allow-empty', '-qm', 'case'], cwd=temp, check=True)
                output.write_text('')
                subprocess.run(['bash', '-c', run_step('Detect Java- and Go-relevant changes')],
                               cwd=temp, env=env, check=True, capture_output=True)
                self.assertEqual(output.read_text(), f'java={java}\ngo={go}\n', paths)
                output.unlink()
            for event, base_arg in [('workflow_dispatch', base), ('pull_request', ''),
                                    ('push', '0' * 40)]:
                output.write_text('')
                subprocess.run(['bash', '-c', run_step('Detect Java- and Go-relevant changes')],
                               cwd=temp, env=dict(env, EVENT=event, BASE=base_arg),
                               check=True, capture_output=True)
                self.assertEqual(output.read_text(), 'java=true\ngo=true\n')
            output.write_text('')
            failed = subprocess.run(['bash', '-c', run_step('Detect Java- and Go-relevant changes')],
                                    cwd=temp, env=dict(env, BASE='missing-ref'), capture_output=True)
            self.assertNotEqual(failed.returncode, 0)
            self.assertEqual(output.read_text(), '')
            output.unlink()
            (cwd / 'Source.java').write_text('source')
            subprocess.run(['git', 'add', '.'], cwd=temp, check=True)
            subprocess.run(['git', 'commit', '-qm', 'Java source'], cwd=temp, check=True)
            rename_base = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=temp, text=True).strip()
            (cwd / 'go').mkdir(exist_ok=True)
            subprocess.run(['git', 'mv', 'Source.java', 'go/Source.java'], cwd=temp, check=True)
            subprocess.run(['git', 'commit', '-qm', 'Move source'], cwd=temp, check=True)
            output.write_text('')
            subprocess.run(['bash', '-c', run_step('Detect Java- and Go-relevant changes')],
                           cwd=temp, env=dict(env, BASE=rename_base), check=True, capture_output=True)
            self.assertEqual(output.read_text(), 'java=true\ngo=true\n')
            for changed, event, expected in [
                ('true', 'pull_request', ['ubuntu-24.04-arm']),
                ('true', 'push', ['macos-latest', 'windows-latest', 'ubuntu-24.04-arm']),
                ('true', 'workflow_dispatch', ['macos-latest', 'windows-latest', 'ubuntu-24.04-arm']),
                ('false', 'pull_request', ['off']),
            ]:
                output.write_text('')
                subprocess.run(['bash', '-c', run_step('Go cross-OS matrix')], cwd=temp,
                               env=dict(env, GO_CHANGED=changed, EVENT=event), check=True, capture_output=True)
                self.assertEqual([x['os'] for x in json.loads(output.read_text().split('=', 1)[1])], expected)


if __name__ == '__main__':
    unittest.main()
