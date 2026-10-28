#!/usr/bin/env python3
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import check_review as checks


class CheckEvidenceTests(unittest.TestCase):
    def events(self, rows):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / 'events.jsonl'
        path.write_text(''.join(json.dumps(row) + '\n' for row in rows))
        return path

    def test_skip_is_retained_and_not_counted_as_pass(self):
        result = checks.test_results(self.events([
            {'Action': 'skip', 'Package': 'p', 'Test': 'TestRealCloud'},
            {'Action': 'pass', 'Package': 'p', 'Test': 'TestLocal'},
            {'Action': 'pass', 'Package': 'p'}]))
        self.assertEqual(1, result['test_pass_events'])
        self.assertEqual(1, result['package_pass'])
        self.assertEqual([{'package': 'p', 'test': 'TestRealCloud'}], result['skipped'])

    def test_all_skipped_or_empty_is_not_green(self):
        for rows in ([], [{'Action': 'skip', 'Package': 'p'}],
                     [{'Action': 'pass', 'Package': 'p'}]):
            with self.subTest(rows=rows), self.assertRaises(checks.builder.BuildError):
                checks.test_results(self.events(rows))

    def test_failure_cannot_be_hidden_by_later_pass(self):
        with self.assertRaises(checks.builder.BuildError):
            checks.test_results(self.events([
                {'Action': 'fail', 'Package': 'p', 'Test': 'TestFailure'},
                {'Action': 'pass', 'Package': 'p', 'Test': 'TestOther'},
                {'Action': 'pass', 'Package': 'p'}]))

    def test_malformed_stream_is_not_green(self):
        for row in ('not-json\n', '[]\n', '{}\n'):
            path = self.events([])
            path.write_text(row)
            with self.subTest(row=row), self.assertRaises(checks.builder.BuildError):
                checks.test_results(path)

    def test_environment_does_not_enable_runtime_or_forward_credentials(self):
        with patch.dict(os.environ, {'PATH': '/usr/bin:/bin',
                                    'TENCENTCLOUD_SECRET_KEY': 'synthetic',
                                    'NOMAD_TOKEN': 'synthetic',
                                    'NOMAD_REAL_BINARY': '/synthetic/runtime',
                                    'NOMAD_AUTOSCALER_TEST_BIN': '/synthetic/runtime',
                                    'GOFLAGS': '-toolexec=unexpected'}, clear=True):
            env = checks.check_environment()
        self.assertFalse(any(n.startswith(('TENCENT', 'NOMAD_')) for n in env))
        self.assertEqual('-p=2', env['GOFLAGS'])
        self.assertEqual('1', env['CGO_ENABLED'])


if __name__ == '__main__':
    unittest.main()
