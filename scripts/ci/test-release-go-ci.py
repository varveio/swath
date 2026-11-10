#!/usr/bin/env python3
"""Release must not infer full validation from ancestry or an unrelated green run."""
import copy
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('release_go_ci',
                                            Path(__file__).with_name('verify-release-go-ci.py'))
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)

SHA = 'a' * 40
REPO = 'varveio/swath'


class ReleaseGateTests(unittest.TestCase):
    def setUp(self):
        self.run = dict(id=42, run_attempt=2, head_sha=SHA, head_branch='main', event='push',
                        path='.github/workflows/ci.yml', head_repository={'full_name': REPO},
                        status='completed', html_url='https://example.invalid/run/42')
        self.gate = dict(name='go-full-gate', status='completed', conclusion='success')

    def check(self, runs=None, jobs=None):
        def api(endpoint):
            if '/jobs?' in endpoint:
                self.assertIn('/42/attempts/2/jobs?', endpoint)
                return [{'jobs': jobs if jobs is not None else [self.gate]}]
            self.assertIn(f'event=push&branch=main&head_sha={SHA}', endpoint)
            self.assertIn('/actions/workflows/ci.yml/runs?', endpoint)
            # Exercise pagination: the candidate need not be in the first page.
            return [{'workflow_runs': []},
                    {'workflow_runs': runs if runs is not None else [self.run]}]
        return guard.qualification(REPO, SHA, api)

    def test_exact_full_result_and_current_attempt(self):
        self.assertEqual(self.check(), self.run['html_url'])
        self.run['status'] = 'in_progress'  # unrelated Java deep tier may still run
        self.assertEqual(self.check(), self.run['html_url'])

    def test_wrong_sha_event_branch_or_repository_does_not_qualify(self):
        for key, value in [('head_sha', 'b' * 40), ('event', 'pull_request'),
                           ('event', 'workflow_dispatch'), ('head_branch', 'feature'),
                           ('head_repository', {'full_name': 'fork/swath'})]:
            with self.subTest(key=key, value=value):
                run = dict(self.run, **{key: value})
                self.assertIsNone(self.check([run]))

    def test_missing_pending_skipped_failed_cancelled_and_ambiguous(self):
        self.assertIsNone(self.check([]))
        self.run['status'] = 'in_progress'
        self.assertIsNone(self.check(jobs=[]))
        self.gate['status'] = 'queued'
        self.assertIsNone(self.check())
        self.run['status'] = self.gate['status'] = 'completed'
        for conclusion in ['failure', 'cancelled', 'skipped', 'timed_out', None]:
            with self.subTest(conclusion=conclusion), self.assertRaises(guard.Rejected):
                self.gate['conclusion'] = conclusion
                self.check()
        with self.assertRaises(guard.Rejected):
            self.check(jobs=[dict(self.gate, name='ci-gate')])
        with self.assertRaises(guard.Rejected):
            self.check(jobs=[self.gate, self.gate])

    def test_newest_run_cannot_be_hidden_by_an_old_success(self):
        old = copy.deepcopy(self.run)
        old['id'] = 41
        self.gate['conclusion'] = 'failure'
        with self.assertRaises(guard.Rejected):
            self.check([self.run, old])


if __name__ == '__main__':
    unittest.main()
