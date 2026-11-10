#!/usr/bin/env python3
"""Require full Go validation from main CI for the exact release commit."""
import argparse
import json
import os
import re
import subprocess
import time


class Rejected(RuntimeError):
    pass


def gh_pages(endpoint):
    result = subprocess.run(['gh', 'api', '--paginate', '--slurp', endpoint],
                            check=True, text=True, capture_output=True)
    return json.loads(result.stdout)


def qualification(repo, sha, api=gh_pages):
    """Return the qualifying run URL, None while pending; fail closed otherwise."""
    pages = api(f'repos/{repo}/actions/workflows/ci.yml/runs'
                f'?event=push&branch=main&head_sha={sha}&per_page=100')
    runs = [run for page in pages for run in page['workflow_runs']
            if run['head_sha'] == sha and run['head_branch'] == 'main'
            and run['event'] == 'push' and run['path'] == '.github/workflows/ci.yml'
            and run['head_repository']['full_name'] == repo]
    if not runs:
        return None
    # A newer attempt/run supersedes an earlier green result at the same SHA.
    run = max(runs, key=lambda row: row['id'])
    pages = api(f"repos/{repo}/actions/runs/{run['id']}/attempts/{run['run_attempt']}"
                '/jobs?per_page=100')
    gates = [job for page in pages for job in page['jobs']
             if job['name'] == 'go-full-gate']
    if len(gates) > 1:
        raise Rejected('ambiguous full Go gate')
    if gates and gates[0]['status'] == 'completed':
        if gates[0]['conclusion'] == 'success':
            return run['html_url']
        raise Rejected(f"full Go gate is {gates[0]['conclusion']}: {run['html_url']}")
    if run['status'] == 'completed':
        raise Rejected(f"no successful full Go gate for {sha}: {run['html_url']}")
    return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('sha')
    parser.add_argument('--wait-seconds', type=int, default=600,
                        help='wait for main CI; zero checks once (default: 600)')
    args = parser.parse_args()
    repo = os.environ['GITHUB_REPOSITORY']
    if not re.fullmatch(r'[0-9a-f]{40}', args.sha) or args.wait_seconds < 0:
        parser.error('expected a full lowercase commit SHA and a nonnegative wait')
    deadline = time.monotonic() + args.wait_seconds
    try:
        while True:
            url = qualification(repo, args.sha)
            if url:
                print(f'Full Go validation passed for {args.sha}: {url}')
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise Rejected('full Go validation is missing or pending; wait for the main CI '
                               'go-full-gate and rerun this release workflow')
            print(f'Waiting for full Go validation of {args.sha}', flush=True)
            time.sleep(min(15, remaining))
    except (Rejected, subprocess.CalledProcessError, KeyError, ValueError) as error:
        raise SystemExit(f'::error::{error}') from error


if __name__ == '__main__':
    main()
