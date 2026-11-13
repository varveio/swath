#!/usr/bin/env python3
"""Select complete Go modules and CI tiers; source/build dependencies, not cache keys."""
import json
import os
import re
import subprocess
from pathlib import Path


PLATFORM_FILE = re.compile(r'_(windows|darwin|linux|unix|arm64|amd64)(_|\.)|\.s$')
PLATFORM_PATHS = ('go/internal/journal/', 'go/internal/source/localfs/',
                  'go/internal/parquetout/', 'go/internal/order/')
PACKAGING = {'Dockerfile.go', 'Dockerfile.go.dockerignore',
             'go/build.sh', 'go/notices.sh', 'LICENSE', 'NOTICE'}
ROOT_PROSE = {'README.md', 'CHANGELOG.md', 'AGENTS.md', 'CONTRIBUTING.md',
              'CODE_OF_CONDUCT.md', 'SECURITY.md', 'RELEASING.md', 'LICENSE.txt'}
MODULES = {'go': 'go/go.mod', 'reader': 'go/reader/go.mod', 'objfile': 'go/objfile/go.mod'}


def fixture_module(path):
    return any(part in {'testdata', 'vendor'} or part.startswith(('.', '_'))
               for part in Path(path).parts[1:-1])


def select(paths, event, *, force_full=False, unknown=False, build_tags=False, toolchain=False):
    out = dict.fromkeys(('go', 'reader', 'objfile', 'fixtures', 'images',
                         'platforms', 'full'), False)
    # Main and manual runs qualify the whole Go tree, even on a docs-only merge.
    out['full'] = event != 'pull_request' or force_full or unknown or not paths or toolchain
    for path in paths:
        if path.startswith('.github/') or path.startswith('scripts/ci/'):
            out['full'] = True
        elif path in PACKAGING:
            out['go'] = out['images'] = out['platforms'] = True
        elif path.startswith('go/'):
            # Longest module prefix owns source, tests, embeds and fixtures alike.
            if path.startswith('go/objfile/'):
                out['objfile'] = True
            elif path.startswith('go/reader/'):
                out['reader'] = out['go'] = True  # main's local replace and reader tests
            else:
                out['go'] = True
            if path.endswith('/go.mod') and path not in {
                    'go/go.mod', 'go/reader/go.mod', 'go/objfile/go.mod'} and not fixture_module(path):
                out['full'] = True  # a new module needs explicit ownership
            if (PLATFORM_FILE.search(path) or path.startswith(PLATFORM_PATHS)
                    or path.endswith(('/go.mod', '/go.sum'))):
                out['platforms'] = True
            if path in {'go/go.mod', 'go/go.sum', 'go/reader/go.mod', 'go/reader/go.sum'}:
                out['images'] = True
        elif path.startswith('docs/replay-provider-'):
            # Also read directly by the Azure client tests, outside the gated fixture tier.
            out['go'] = out['fixtures'] = True
        elif (path in ROOT_PROSE
              or path.startswith(('docs/', 'site/', 'config/', 'scripts/provider-conformance/'))
              or path in {'.gitignore', '.gitattributes', '.editorconfig'}):
            pass
        else:
            out['full'] = True  # unknown ownership fails toward more coverage
    if build_tags:
        out['platforms'] = True
    if out['full']:
        for key in ('go', 'reader', 'objfile', 'fixtures', 'images', 'platforms'):
            out[key] = True
    # Reader changes already selected main above. Main's tests consume the replay fixtures.
    out['fixtures'] |= out['go']
    return out


def git(*args):
    return subprocess.check_output(['git', *args])


def dependants(outputs, manifests):
    """Follow local module references, including replace/test dependencies."""
    names = {key: re.search(r'^module\s+(\S+)', value, re.M).group(1)
             for key, value in manifests.items()}
    changed = True
    while changed:
        changed = False
        for consumer, text in manifests.items():
            for dependency, name in names.items():
                if (outputs[dependency] and not outputs[consumer]
                        and re.search(r'(?<!\S)' + re.escape(name) + r'(?!\S)', text)):
                    outputs[consumer] = changed = True
    outputs['fixtures'] |= outputs['go']
    return outputs


def detect(event, base, labels):
    unknown = not base or base == '0' * 40
    paths, build_tags, toolchain = [], False, False
    if not unknown:
        # Missing/unreadable git history is an error, never an empty successful selection.
        paths = git('diff', '--no-renames', '--name-only', '-z', base, 'HEAD').decode(errors='surrogateescape').split('\0')[:-1]
        diff = git('diff', '--no-renames', '--unified=0', base, 'HEAD', '--', 'go/').decode(errors='replace')
        changed_lines = [line[1:] for line in diff.splitlines()
                         if line[:1] in {'+', '-'} and not line.startswith(('+++', '---'))]
        build_tags = any(re.match(r'\s*//\s*(go:build|\+build)\b', line) for line in changed_lines)
        # A body-only edit in a constrained file is platform-sensitive too. Read
        # both revisions so deletion or removal of the constraint cannot hide it.
        for path in paths:
            if path.startswith('go/') and path.endswith('.go'):
                for revision in (base, 'HEAD'):
                    file = subprocess.run(['git', 'show', f'{revision}:{path}'],
                                          capture_output=True, text=True, errors='replace')
                    if file.returncode == 0:
                        build_tags |= any(re.match(r'\s*//\s*(go:build|\+build)\b', line)
                                          for line in file.stdout.splitlines())
        toolchain = any(re.match(r'(?:go\s+\d|toolchain\s+go\d)', line) for line in changed_lines)
    tracked = {path for path in git('ls-files', '-z', 'go/go.mod', 'go/**/go.mod')
               .decode(errors='surrogateescape').split('\0') if path and not fixture_module(path)}
    if tracked != set(MODULES.values()):
        raise RuntimeError('Go modules changed: update CI module ownership and jobs before merging')
    outputs = select(paths, event, force_full='ci:full' in labels, unknown=unknown,
                     build_tags=build_tags, toolchain=toolchain)
    outputs = dependants(outputs, {key: Path(path).read_text() for key, path in MODULES.items()})
    if outputs['go'] and any(path.endswith(('/go.mod', '/go.sum')) for path in paths):
        outputs['images'] = True
    return outputs


def main():
    labels = [label['name'] for label in json.loads(os.environ.get('PR_LABELS', '[]'))]
    outputs = detect(os.environ['EVENT'], os.environ.get('BASE', ''), labels)
    with open(os.environ['GITHUB_OUTPUT'], 'a') as output:
        for key, value in outputs.items():
            print(f'{key}={str(value).lower()}', file=output)
    print('CI selection: ' + json.dumps(outputs, sort_keys=True))


if __name__ == '__main__':
    main()
