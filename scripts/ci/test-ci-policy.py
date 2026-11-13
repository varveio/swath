#!/usr/bin/env python3
"""Exercise cache deletion boundaries and the actual change/matrix shell in ci.yml."""
import json
import importlib.util
import re
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


spec = importlib.util.spec_from_file_location('go_ci_policy', ROOT / 'scripts/ci/go-ci-policy.py')
policy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(policy)


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

    def test_module_and_tier_selection(self):
        cases = [
            (['internal/x.go'], {'go', 'fixtures'}),
            (['cmd/swath/main.go'], {'go', 'fixtures'}),
            (['cmd/swath-replay/main_test.go'], {'go', 'fixtures'}),
            (['bench/s3fake/main.go'], {'go', 'fixtures'}),
            (['bench/compare.sh'], {'go', 'fixtures'}),
            (['testdata/readers/s3.json'], {'go', 'fixtures'}),
            (['verify.py'], {'go', 'fixtures'}),
            (['dataset_schema.py'], {'go', 'fixtures'}),
            (['test_dataset_readers.py'], {'go', 'fixtures'}),
            # A page under docs/ selects only the docs tier, with light files or an
            # independent module beside it; with any main-module input main runs as before.
            (['docs/getting-started.md'], {'docs'}),
            (['docs/commands/swath-list.md'], {'docs'}),
            (['docs/internals/testing.md', 'docs/internals/range-engine.md'], {'docs'}),
            (['docs/troubleshooting.md', 'site/index.html', 'README.md', 'docs/ops/dev/CI.md'], {'docs'}),
            (['docs/getting-started.md', 'objfile/object.go'], {'docs', 'objfile'}),
            (['docs/getting-started.md', 'cmd/swath/main.go'], {'go', 'fixtures'}),
            (['docs/commands/swath-list.md', 'cmd/swath/main_test.go'], {'go', 'fixtures'}),
            (['docs/getting-started.md', 'reader/read.go'], {'go', 'reader', 'fixtures'}),
            (['docs/getting-started.md', 'Dockerfile'], {'go', 'fixtures', 'images', 'platforms'}),
            (['docs/getting-started.md', 'docs/replay-provider-evidence.tar.gz'], {'go', 'fixtures'}),
            # Other files under docs/ stay the main module's: the docs package, the provider
            # evidence (its page included) and a file type no rule names yet.
            (['docs/links_test.go'], {'go', 'fixtures'}),
            (['docs/docs.go'], {'go', 'fixtures'}),
            (['docs/replay-provider-conformance.md'], {'go', 'fixtures'}),
            (['docs/replay-provider-ledger.json'], {'go', 'fixtures'}),
            (['docs/internals/diagram.svg'], {'go', 'fixtures'}),
            (['docs/reference/schema.json'], {'go', 'fixtures'}),
            (['reader/read_test.go'], {'go', 'reader', 'fixtures'}),
            (['reader/README.md'], {'go', 'reader', 'fixtures'}),
            (['objfile/object.go'], {'objfile'}),
            (['objfile/testdata/fixture'], {'objfile'}),
            (['objfile/go.sum'], {'objfile', 'platforms'}),
            (['go.mod'], {'go', 'fixtures', 'images', 'platforms'}),
            (['go.sum'], {'go', 'fixtures', 'images', 'platforms'}),
            (['reader/go.sum'], {'go', 'reader', 'fixtures', 'images', 'platforms'}),
            (['cmd/swath/foo_windows.go'], {'go', 'fixtures', 'platforms'}),
            (['internal/journal/lock.go'], {'go', 'fixtures', 'platforms'}),
            (['internal/source/localfs/scan.go'], {'go', 'fixtures', 'platforms'}),
            (['internal/parquetout/sink.go'], {'go', 'fixtures', 'platforms'}),
            (['internal/order/spill.go'], {'go', 'fixtures', 'platforms'}),
            (['objfile/foo_arm64.s'], {'objfile', 'platforms'}),
            (['Dockerfile'], {'go', 'fixtures', 'images', 'platforms'}),
            (['.dockerignore'], {'go', 'fixtures', 'images', 'platforms'}),
            (['build.sh'], {'go', 'fixtures', 'images', 'platforms'}),
            (['notices.sh'], {'go', 'fixtures', 'images', 'platforms'}),
            (['LICENSE'], {'go', 'fixtures', 'images', 'platforms'}),
            (['docs/replay-provider-expectations.json'], {'go', 'fixtures'}),
            (['docs/replay-provider-evidence.tar.gz'], {'go', 'fixtures'}),
            (['docs/style.md'], set()),
            (['docs/go-release-copy-checklist.md'], set()),
            (['docs/ops/dev/CI.md'], set()),
            (['README.md', 'SECURITY.md'], set()),
            (['AGENTS.md', 'CONTRIBUTING.md', 'CHANGELOG.md'], set()),
            (['site/data/runs/facts.json'], set()),
            (['docs/assets/demo.mp4'], set()),
            (['site/index.html'], set()),
            (['config/osv/osv-scanner.toml'], set()),
            (['scripts/provider-conformance/campaign.py'], set()),
            (['scripts/benchmarks/replay/run_pair.py'], set()),
        ]
        for paths, expected in cases:
            with self.subTest(paths=paths):
                got = policy.select(paths, 'pull_request')
                self.assertEqual({key for key, value in got.items() if value}, expected)
        # A Go file or module outside the known tree has no owner yet: everything runs.
        for paths in [['.github/workflows/ci.yml'], ['.github/actions/setup-go/action.yml'],
                      ['scripts/ci/go-race-shards.sh'], ['unknown-build-input'], [],
                      ['docs/getting-started.md', 'scripts/ci/go-docs-tests.sh'],
                      ['pkg/new.go'], ['tools/x/go.mod'], ['internal/new/go.mod']]:
            got = policy.select(paths, 'pull_request')
            self.assertTrue(all(got.values()), paths)
        for kwargs in [dict(force_full=True), dict(unknown=True), dict(toolchain=True)]:
            for paths in [['README.md'], ['docs/getting-started.md']]:
                got = policy.select(paths, 'pull_request', **kwargs)
                self.assertTrue(all(got.values()), (paths, kwargs))
        for event in ['push', 'workflow_dispatch']:
            for paths in [['README.md'], ['docs/getting-started.md']]:
                got = policy.select(paths, event)
                self.assertTrue(all(got.values()), (paths, event))
        self.assertTrue(policy.select(['internal/foo.go'], 'pull_request',
                                      build_tags=True)['platforms'])

    def test_dependency_closure_uses_modules_not_cache_keys(self):
        manifests = {'go': 'module example/main\nrequire example/reader v1.0.0',
                     'reader': 'module example/reader', 'objfile': 'module example/objfile'}
        out = policy.select(['objfile/object.go'], 'pull_request')
        self.assertFalse(policy.dependants(out, manifests)['go'])
        manifests['reader'] += '\nrequire example/objfile v1.0.0'
        out = policy.dependants(out, manifests)
        self.assertTrue(out['reader'] and out['go'] and out['fixtures'])

    def test_real_git_diff_renames_tags_toolchain_and_missing_history(self):
        with tempfile.TemporaryDirectory() as temp:
            cwd = Path(temp)
            subprocess.run(['git', 'init', '-q', temp], check=True)
            for args in [('user.email', 'ci@example.invalid'), ('user.name', 'CI')]:
                subprocess.run(['git', 'config', *args], cwd=temp, check=True)
            for flag, path in policy.MODULES.items():
                f = cwd / path
                f.parent.mkdir(parents=True, exist_ok=True)
                f.write_text('module example/' + flag + '\ngo 1.26.0\n')
            source = cwd / 'objfile/source.go'
            source.write_text('package objfile\n')
            subprocess.run(['git', 'add', '.'], cwd=temp, check=True)
            subprocess.run(['git', 'commit', '-qm', 'base'], cwd=temp, check=True)
            base = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=temp, text=True).strip()
            output = cwd / 'output'
            env = dict(os.environ, EVENT='pull_request', BASE=base,
                       GITHUB_OUTPUT=str(output), PR_LABELS='[]')

            def run(**extra):
                output.write_text('')
                result = subprocess.run(['python3', str(ROOT / 'scripts/ci/go-ci-policy.py')],
                                        cwd=temp, env=dict(env, **extra), capture_output=True, text=True)
                rows = dict(line.split('=', 1) for line in output.read_text().splitlines())
                return result, rows

            (cwd / 'internal').mkdir()
            subprocess.run(['git', 'mv', 'objfile/source.go', 'internal/source.go'], cwd=temp, check=True)
            subprocess.run(['git', 'commit', '-qm', 'move across modules'], cwd=temp, check=True)
            result, rows = run()
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual((rows['go'], rows['objfile'], rows['reader']), ('true', 'true', 'false'))
            result, rows = run(PR_LABELS='[{"name":"ci:full"}]')
            self.assertEqual(rows['full'], 'true')
            # Adding, editing a constrained body, or deleting a build constraint
            # must escalate even when the filename has no platform suffix.
            (cwd / 'internal/source.go').write_text('// license\n' * 55 + '//go:build linux\n\npackage objfile\n')
            subprocess.run(['git', 'add', 'internal/source.go'], cwd=temp, check=True)
            subprocess.run(['git', 'commit', '-qm', 'constraint'], cwd=temp, check=True)
            self.assertEqual(run()[1]['platforms'], 'true')
            constrained_base = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=temp, text=True).strip()
            (cwd / 'internal/source.go').write_text('// license\n' * 55 + '//go:build linux\n\npackage changed\n')
            subprocess.run(['git', 'add', 'internal/source.go'], cwd=temp, check=True)
            subprocess.run(['git', 'commit', '-qm', 'constrained body'], cwd=temp, check=True)
            self.assertEqual(run(BASE=constrained_base)[1]['platforms'], 'true')
            (cwd / 'internal/source.go').write_text('package changed\n')
            subprocess.run(['git', 'add', 'internal/source.go'], cwd=temp, check=True)
            subprocess.run(['git', 'commit', '-qm', 'remove constraint'], cwd=temp, check=True)
            self.assertEqual(run(BASE=constrained_base)[1]['platforms'], 'true')
            # A page quoting a build constraint or a go directive is still only a page.
            (cwd / 'docs').mkdir()
            (cwd / 'docs/page.md').write_text('```go\n//go:build linux\n```\n\ngo 1.27.0\ntoolchain go1.27.0\n')
            subprocess.run(['git', 'add', 'docs/page.md'], cwd=temp, check=True)
            subprocess.run(['git', 'commit', '-qm', 'page'], cwd=temp, check=True)
            page_base = subprocess.check_output(['git', 'rev-parse', 'HEAD~1'], cwd=temp, text=True).strip()
            result, rows = run(BASE=page_base)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual({k for k, v in rows.items() if v == 'true'}, {'docs'})
            (cwd / 'go.mod').write_text('module example/go\ngo 1.27.0\n')
            subprocess.run(['git', 'add', 'go.mod'], cwd=temp, check=True)
            subprocess.run(['git', 'commit', '-qm', 'toolchain'], cwd=temp, check=True)
            self.assertEqual(run()[1]['full'], 'true')
            for bad_base in ['missing-ref']:
                result, rows = run(BASE=bad_base)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(rows, {})
            self.assertEqual(run(BASE='')[1]['full'], 'true')
            # Go ignores fixture module declarations; binary/non-UTF-8 fixture
            # contents and filenames still select their enclosing module.
            fixture = cwd / 'objfile/testdata/sample'
            fixture.mkdir(parents=True)
            (fixture / 'go.mod').write_text('module example/fixture\n')
            raw_name = os.fsdecode(b'latin-\xff.txt')
            (fixture / raw_name).write_bytes(b'fixture\xff\n')
            subprocess.run(['git', 'add', 'objfile/testdata'], cwd=temp, check=True)
            subprocess.run(['git', 'commit', '-qm', 'fixtures'], cwd=temp, check=True)
            result, rows = run()
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(rows['objfile'], 'true')
            # A new nested module cannot silently miss every existing job.
            (cwd / 'internal/new').mkdir()
            (cwd / 'internal/new/go.mod').write_text('module example/new\n')
            subprocess.run(['git', 'add', 'internal/new/go.mod'], cwd=temp, check=True)
            subprocess.run(['git', 'commit', '-qm', 'new module'], cwd=temp, check=True)
            result, rows = run()
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(rows, {})

    def test_actual_xos_matrix(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / 'output'
            for main, any_module, platforms, expected, scope in [
                ('true', 'true', 'false', ['ubuntu-24.04-arm'], 'extra'),
                ('true', 'true', 'true', ['macos-latest', 'windows-latest', 'ubuntu-24.04-arm'], 'full'),
                ('false', 'true', 'true', ['macos-latest', 'windows-latest', 'ubuntu-24.04-arm'], 'full'),
                ('false', 'true', 'false', ['off'], 'off'),
                ('false', 'false', 'false', ['off'], 'off'),
            ]:
                output.write_text('')
                env = dict(os.environ, GO_CHANGED=main, GO_ANY=any_module, PLATFORMS=platforms,
                           GITHUB_OUTPUT=str(output))
                subprocess.run(['bash', '-c', run_step('Go cross-OS matrix')], cwd=temp,
                               env=env, check=True, capture_output=True)
                matrix = json.loads(output.read_text().split('=', 1)[1])
                self.assertEqual([row['os'] for row in matrix], expected)
                self.assertEqual({row['scope'] for row in matrix}, {scope})

    def test_full_gate_refuses_partial_or_failed_validation(self):
        flags = ('full', 'go', 'reader', 'objfile', 'fixtures', 'images', 'platforms', 'docs')
        selection = dict.fromkeys(flags, 'true')
        selection['go_shards'] = subprocess.check_output(
            [str(ROOT / 'scripts/ci/go-race-shards.sh'), '--matrix'], text=True)
        xos = [dict(os=os_name, runner=os_name, scope='full')
               for os_name in ('macos-latest', 'windows-latest', 'ubuntu-24.04-arm')]
        selection['go_xos'] = json.dumps(xos)

        def check(selected, result='success'):
            return subprocess.run(['bash', '-c', run_step('Full Go validation passed')], cwd=ROOT,
                                  env=dict(os.environ, NEEDS=json.dumps({'go-race': {'result': result}}),
                                           SELECTION=json.dumps(selected)), capture_output=True).returncode

        self.assertEqual(check(selection), 0)
        for result in ['failure', 'skipped', 'cancelled']:
            self.assertNotEqual(check(selection, result), 0)
        for omitted in flags:
            self.assertNotEqual(check(dict(selection, **{omitted: 'false'})), 0, omitted)
        for bad_matrix in [xos[:2], [dict(row, scope='extra') for row in xos],
                           [dict(row, runner='ubuntu-latest') for row in xos]]:
            self.assertNotEqual(check(dict(selection, go_xos=json.dumps(bad_matrix))), 0)
        self.assertNotEqual(check(dict(selection, go_shards='[]')), 0)

        # A new Go job cannot be omitted from either aggregate unnoticed.
        go_jobs = set(re.findall(r'^  (go-[a-z0-9_-]+):$', WORKFLOW, re.M)) - {'go-full-gate'}
        for gate in ['ci-gate', 'go-full-gate']:
            section = WORKFLOW.split('  ' + gate + ':\n', 1)[1]
            needs = set(re.search(r'needs: \[(.*?)\]', section)[1].split(', '))
            self.assertTrue(go_jobs <= needs, (gate, go_jobs - needs))

        with tempfile.NamedTemporaryFile() as summary:
            for result in ['success', 'skipped', 'failure', 'cancelled']:
                run = subprocess.run(['bash', '-c', run_step('Every gating job passed')],
                                     env=dict(os.environ, GITHUB_STEP_SUMMARY=summary.name,
                                              NEEDS=json.dumps({'go-test': {'result': result}})),
                                     capture_output=True)
                self.assertEqual(run.returncode == 0, result == 'success')


if __name__ == '__main__':
    unittest.main()
