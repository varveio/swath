#!/usr/bin/env python3
"""Select complete Go modules and CI tiers; source/build dependencies, not cache keys."""
import json
import os
import re
import subprocess
from pathlib import Path


PLATFORM_FILE = re.compile(r'_(windows|darwin|linux|unix|arm64|amd64)(_|\.)|\.s$')
PLATFORM_PATHS = ('internal/journal/', 'internal/source/localfs/',
                  'internal/parquetout/', 'internal/order/')
PACKAGING = {'Dockerfile', '.dockerignore', 'build.sh', 'notices.sh', 'LICENSE', 'NOTICE'}
ROOT_PROSE = {'README.md', 'CHANGELOG.md', 'AGENTS.md', 'CONTRIBUTING.md',
              'CODE_OF_CONDUCT.md', 'SECURITY.md', 'RELEASING.md', 'LICENSE.txt'}
MODULES = {'go': 'go.mod', 'reader': 'reader/go.mod', 'objfile': 'objfile/go.mod'}
# Pages under docs/ that no Go test reads: the media, the style guide and the maintainers' pages.
# Their links are checked by go-lint on every event, like the root prose.
LIGHT_DOCS = ('docs/assets/', 'docs/style.md', 'docs/ops/')
# The Go tree: the nested modules' directories, and the main module's directories and root files.
NESTED = ('objfile/', 'reader/')
MAIN_DIRS = ('cmd/', 'internal/', 'bench/', 'testdata/', 'docs/')
MAIN_FILES = {'go.mod', 'go.sum', 'verify.py', 'dataset_schema.py', 'test_dataset_readers.py'}
GO_TREE = (*NESTED, *MAIN_DIRS, *MAIN_FILES)


def in_go_tree(path):
    return path.startswith(NESTED + MAIN_DIRS) or path in MAIN_FILES


def fixture_module(path):
    return any(part in {'testdata', 'vendor'} or part.startswith(('.', '_'))
               for part in Path(path).parts[:-1])


def select(paths, event, *, force_full=False, unknown=False, build_tags=False, toolchain=False):
    out = dict.fromkeys(('go', 'reader', 'objfile', 'fixtures', 'images',
                         'platforms', 'docs', 'full'), False)
    # Main and manual runs qualify the whole Go tree, even on a docs-only merge.
    out['full'] = event != 'pull_request' or force_full or unknown or not paths or toolchain
    for path in paths:
        if path.startswith('.github/') or path.startswith('scripts/ci/'):
            out['full'] = True
        elif path in PACKAGING:
            out['go'] = out['images'] = out['platforms'] = True
        elif path.startswith('docs/replay-provider-'):
            # Also read directly by the Azure client tests, outside the gated fixture tier.
            out['go'] = out['fixtures'] = True
        elif (path in ROOT_PROSE
              or path.startswith((*LIGHT_DOCS, 'site/', 'config/', 'scripts/provider-conformance/',
                                  'scripts/benchmarks/'))
              or path in {'.gitignore', '.gitattributes', '.editorconfig'}):
            pass  # go-lint checks every Markdown file's links on every event
        elif path.startswith('docs/') and path.endswith('.md'):
            # Any other page: the tests that read pages (TestDocs*, scripts/ci/go-docs-tests.sh).
            # Other files under docs/ stay the main module's: the docs package, and data tests read.
            out['docs'] = True
        elif in_go_tree(path):
            # Longest module prefix owns source, tests, embeds and fixtures alike.
            if path.startswith('objfile/'):
                out['objfile'] = True
            elif path.startswith('reader/'):
                out['reader'] = out['go'] = True  # main's local replace and reader tests
            else:
                out['go'] = True
            name = Path(path).name
            if name == 'go.mod' and path not in MODULES.values() and not fixture_module(path):
                out['full'] = True  # a new module needs explicit ownership
            if (PLATFORM_FILE.search(path) or path.startswith(PLATFORM_PATHS)
                    or name in {'go.mod', 'go.sum'}):
                out['platforms'] = True
            if path in {'go.mod', 'go.sum', 'reader/go.mod', 'reader/go.sum'}:
                out['images'] = True
        else:
            out['full'] = True  # unknown ownership fails toward more coverage
    if build_tags:
        out['platforms'] = True
    if out['full']:
        for key in ('go', 'reader', 'objfile', 'fixtures', 'images', 'platforms', 'docs'):
            out[key] = True
    elif out['go']:
        out['docs'] = False  # main's race shards run the TestDocs tests with the rest
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
        # Markdown builds nothing: a page quoting `//go:build` or a `go 1.x` line escalates nothing.
        diff = git('diff', '--no-renames', '--unified=0', base, 'HEAD', '--', *GO_TREE,
                   ':(exclude,glob)**/*.md').decode(errors='replace')
        changed_lines = [line[1:] for line in diff.splitlines()
                         if line[:1] in {'+', '-'} and not line.startswith(('+++', '---'))]
        build_tags = any(re.match(r'\s*//\s*(go:build|\+build)\b', line) for line in changed_lines)
        # A body-only edit in a constrained file is platform-sensitive too. Read
        # both revisions so deletion or removal of the constraint cannot hide it.
        for path in paths:
            if path.endswith('.go'):
                for revision in (base, 'HEAD'):
                    file = subprocess.run(['git', 'show', f'{revision}:{path}'],
                                          capture_output=True, text=True, errors='replace')
                    if file.returncode == 0:
                        build_tags |= any(re.match(r'\s*//\s*(go:build|\+build)\b', line)
                                          for line in file.stdout.splitlines())
        toolchain = any(re.match(r'(?:go\s+\d|toolchain\s+go\d)', line) for line in changed_lines)
    tracked = {path for path in git('ls-files', '-z', 'go.mod', '**/go.mod')
               .decode(errors='surrogateescape').split('\0') if path and not fixture_module(path)}
    if tracked != set(MODULES.values()):
        raise RuntimeError('Go modules changed: update CI module ownership and jobs before merging')
    outputs = select(paths, event, force_full='ci:full' in labels, unknown=unknown,
                     build_tags=build_tags, toolchain=toolchain)
    outputs = dependants(outputs, {key: Path(path).read_text() for key, path in MODULES.items()})
    if outputs['go'] and any(Path(path).name in {'go.mod', 'go.sum'} for path in paths):
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
