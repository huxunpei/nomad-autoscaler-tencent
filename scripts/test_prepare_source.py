import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('prepare_source', str(Path(__file__).with_name('prepare_source.py')))
exporter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(exporter)


class PrepareSourceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / 'source'
        self.source.mkdir()
        self.output = self.root / 'output'
        for directory in exporter.PACKAGES:
            (self.source / directory).mkdir(parents=True, exist_ok=True)
        for relative in exporter.ROOT_FILES + exporter.EXAMPLES:
            path = self.source / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('fixture\n')
        (self.source / 'go.mod').write_text('module private.example/team/target\n\ngo 1.25.10\n')
        (self.source / 'main.go').write_text('package main\nimport "private.example/team/target/plugin"\n')
        (self.source / 'plugin' / 'plugin.go').write_text('package plugin\n')
        self.privacy_policy = self.root / 'privacy.json'
        self.privacy_policy.write_text(json.dumps({'format': 1, 'deny_literals':
            ['private.example', 'pages.example.invalid/workspace', '@corp.example.invalid']}))

    def prepare(self, *args, **kwargs):
        kwargs.setdefault('privacy_policy', self.privacy_policy)
        return exporter.prepare(*args, **kwargs)

    def test_allowlist_excludes_git_state_env_and_evidence_even_when_go(self):
        for relative in ('.env', '.git/private.go', 'state/create.go', 'docs/evidence/private.go', 'internal/unreviewed/private.go'):
            path = self.source / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('not for export')
        result = self.prepare(self.source, self.output)
        self.assertEqual('REVIEW_ONLY', result['status'])
        self.assertNotIn('private.example', (self.output / 'main.go').read_text())
        self.assertIn(exporter.DEFAULT_MODULE, (self.output / 'main.go').read_text())
        for relative in ('.env', '.git', 'state', 'docs/evidence', 'internal/unreviewed'):
            self.assertFalse((self.output / relative).exists())
        manifest = json.loads((self.output / 'SOURCE-MANIFEST.json').read_text())
        self.assertFalse(manifest['publication_authorized'])
        self.assertFalse(manifest['license_file_present'])

    def test_existing_output_is_not_replaced(self):
        self.output.mkdir()
        sentinel = self.output / 'keep'
        sentinel.write_text('unchanged')
        with self.assertRaises(exporter.ExportError):
            self.prepare(self.source, self.output)
        self.assertEqual('unchanged', sentinel.read_text())

    def test_referenced_simulation_package_is_exported_with_its_tests(self):
        model = self.source / 'internal' / 'simulation'
        model.mkdir(parents=True, exist_ok=True)
        (model / 'engine.go').write_text('package simulation\n')
        (model / 'engine_test.go').write_text('package simulation\n')
        (self.source / 'main.go').write_text(
            'package main\nimport "private.example/team/target/internal/simulation"\n')
        self.prepare(self.source, self.output)
        self.assertTrue((self.output / 'internal/simulation/engine.go').is_file())
        self.assertTrue((self.output / 'internal/simulation/engine_test.go').is_file())
        self.assertIn(exporter.DEFAULT_MODULE + '/internal/simulation',
                      (self.output / 'main.go').read_text())

    def test_output_inside_source_is_rejected(self):
        with self.assertRaises(exporter.ExportError):
            self.prepare(self.source, self.source / 'nested')

    def test_selected_symlink_is_rejected(self):
        path = self.source / 'plugin' / 'bad.go'
        path.symlink_to(self.source / '.env')
        with self.assertRaises(exporter.ExportError):
            self.prepare(self.source, self.output)
        self.assertFalse(self.output.exists())

    def test_unknown_resource_identifier_blocks_before_output(self):
        (self.source / 'plugin' / 'plugin.go').write_text('package plugin\n// ins-' + '1234abcd\n')
        with self.assertRaisesRegex(exporter.ExportError, 'resource identifier'):
            self.prepare(self.source, self.output)
        self.assertFalse(self.output.exists())

    def test_credential_and_private_reference_are_not_echoed(self):
        for text in ('AKID' + 'X' * 32, 'https://pages.example.invalid/workspace/private'):
            (self.source / 'plugin' / 'plugin.go').write_text('package plugin\n// ' + text)
            with self.assertRaises(exporter.ExportError) as caught:
                self.prepare(self.source, self.output)
            self.assertNotIn(text, str(caught.exception))
            self.assertFalse(self.output.exists())

    def test_explicit_destination_still_does_not_authorize_release(self):
        result = self.prepare(self.source, self.output, 'example.org/team/target')
        self.assertEqual('example.org/team/target', result['module'])
        self.assertFalse(result['publication_authorized'])
        self.assertTrue((self.output / 'REVIEW-ONLY.md').exists())

    def test_module_rewrite_requires_policy_and_policy_is_not_exported(self):
        with self.assertRaisesRegex(exporter.ExportError, 'explicit privacy policy'):
            exporter.prepare(self.source, self.output)
        self.assertFalse(self.output.exists())
        private = self.source / '.source-privacy.json'
        private.write_bytes(self.privacy_policy.read_bytes())
        self.prepare(self.source, self.output, privacy_policy=private)
        self.assertFalse((self.output / private.name).exists())
        self.assertNotIn('deny_literals', (self.output / 'SOURCE-MANIFEST.json').read_text())

    def test_missing_malformed_empty_and_symlink_policy_fail_closed(self):
        missing = self.root / 'missing-policy.json'
        malformed = self.root / 'malformed-policy.json'
        malformed.write_text('{')
        empty = self.root / 'empty-policy.json'
        empty.write_text('{"format": 1, "deny_literals": []}')
        duplicate = self.root / 'duplicate-policy.json'
        duplicate.write_text('{"format": 1, "format": 1, "deny_literals": ["private.example"]}')
        link = self.root / 'policy-link.json'
        link.symlink_to(self.privacy_policy)
        for path in (missing, malformed, empty, duplicate, link):
            with self.subTest(path=path.name), self.assertRaises(exporter.ExportError):
                self.prepare(self.source, self.output, privacy_policy=path)
            self.assertFalse(self.output.exists())

    def test_policy_cannot_be_exported_or_allow_private_destination(self):
        path = self.source / 'README.md'
        path.write_bytes(self.privacy_policy.read_bytes())
        with self.assertRaisesRegex(exporter.ExportError, 'outside exported inputs'):
            self.prepare(self.source, self.output, privacy_policy=path)
        with self.assertRaisesRegex(exporter.ExportError, 'private source host'):
            self.prepare(self.source, self.output, 'private.example/team/new')
        self.assertFalse(self.output.exists())

    def test_private_literals_match_case_insensitively_without_echo(self):
        for payload in ('https://PRIVATE.EXAMPLE/project', 'user@CORP.EXAMPLE.INVALID',
                        'https://PAGES.EXAMPLE.INVALID/WORKSPACE/page'):
            (self.source / 'README.md').write_text(payload)
            with self.subTest(payload=payload), self.assertRaises(exporter.ExportError) as caught:
                self.prepare(self.source, self.output)
            self.assertNotIn(payload, str(caught.exception))
            self.assertFalse(self.output.exists())

    def test_private_addresses_accounts_and_credentials_block_even_in_tests(self):
        payloads = ['address=' + '.'.join(parts) for parts in
                    (('10', '20', '30', '40'), ('172', '16', '0', '1'), ('192', '168', '1', '1'))]
        payloads += ['AccountId=' + '1234567890', '"account_id": "' + '123456789012' + '"',
                     'secret_key="' + 'new-unreviewed-value' + '"', '/' + 'home/synthetic/work']
        for payload in payloads:
            (self.source / 'plugin' / 'probe_test.go').write_text('package plugin\n// ' + payload)
            with self.subTest(payload=payload), self.assertRaises(exporter.ExportError) as caught:
                self.prepare(self.source, self.output)
            self.assertNotIn(payload, str(caught.exception))
            self.assertFalse(self.output.exists())

    def test_documentation_addresses_and_exact_synthetic_credentials_are_allowed(self):
        text = 'package plugin\n// http://192.0.2.1:4646 http://127.0.0.1:4646\n'
        text += '// nomad_token="synthetic-token"\n'
        (self.source / 'plugin' / 'probe_test.go').write_text(text)
        self.prepare(self.source, self.output)
        with self.assertRaises(exporter.ExportError):
            exporter.scan('plugin/probe.go', text, None)

    def test_policy_drift_is_rejected_before_output(self):
        original = exporter.read_file
        def changing(root, relative):
            raw = original(root, relative)
            if relative == 'main.go':
                self.privacy_policy.write_text('{"format": 1, "deny_literals": ["changed.example"]}')
            return raw
        exporter.read_file = changing
        try:
            with self.assertRaisesRegex(exporter.ExportError, 'policy changed'):
                self.prepare(self.source, self.output)
            self.assertFalse(self.output.exists())
        finally:
            exporter.read_file = original

    def test_public_checkout_can_export_same_module_without_private_files(self):
        module = 'github.com/huxunpei/nomad-autoscaler-tencent'
        (self.source / 'go.mod').write_text('module ' + module + '\n\ngo 1.25.10\n')
        (self.source / 'main.go').write_text('package main\nimport "' + module + '/plugin"\n')
        result = self.prepare(self.source, self.output, module, privacy_policy=None)
        self.assertEqual(module, result['module'])
        self.assertIn(module, (self.output / 'main.go').read_text())
        self.assertFalse(result['publication_authorized'])

    def test_module_declaration_with_tabs_is_rewritten(self):
        (self.source / 'go.mod').write_text('module\tprivate.example/team/target\n\ngo 1.25.10\n')
        self.prepare(self.source, self.output)
        self.assertIn('module\t' + exporter.DEFAULT_MODULE,
                      (self.output / 'go.mod').read_text())

    def test_source_drift_is_rejected_before_output(self):
        original = exporter.read_file
        counts = {}
        def changing(root, relative):
            counts[relative] = counts.get(relative, 0) + 1
            raw = original(root, relative)
            return raw + b'changed' if relative == 'main.go' and counts[relative] > 1 else raw
        exporter.read_file = changing
        try:
            with self.assertRaisesRegex(exporter.ExportError, 'source changed'):
                self.prepare(self.source, self.output)
            self.assertFalse(self.output.exists())
        finally:
            exporter.read_file = original

    def test_added_allowlisted_file_is_detected_during_capture(self):
        original = exporter.read_file
        def adding(root, relative):
            raw = original(root, relative)
            if relative == 'main.go':
                (root / 'plugin' / 'added.go').write_text('package plugin\n')
            return raw
        exporter.read_file = adding
        try:
            with self.assertRaisesRegex(exporter.ExportError, 'source changed'):
                self.prepare(self.source, self.output)
            self.assertFalse(self.output.exists())
        finally:
            exporter.read_file = original

    def test_formatter_runs_after_rewrite_before_hashing(self):
        tool = self.root / 'formatter'
        tool.write_text('#!/usr/bin/python3\nimport sys\ns=sys.stdin.read()\n'
                        'assert "private.example/" not in s\n'
                        'sys.stdout.write(s.replace("package main", "package main // formatted"))\n')
        tool.chmod(0o700)
        original = (self.source / 'main.go').read_bytes()
        self.prepare(self.source, self.output, gofmt=str(tool))
        raw = (self.output / 'main.go').read_bytes()
        self.assertIn(b'// formatted', raw)
        self.assertEqual(original, (self.source / 'main.go').read_bytes())
        manifest = json.loads((self.output / 'SOURCE-MANIFEST.json').read_text())
        self.assertTrue(manifest['gofmt_applied'])
        self.assertEqual(exporter.hashlib.sha256(raw).hexdigest(), manifest['files']['main.go'])

    def test_formatter_failure_does_not_create_partial_export_or_echo_stderr(self):
        tool = self.root / 'formatter'
        tool.write_text('#!/bin/sh\necho synthetic-private-diagnostic >&2\nexit 3\n')
        tool.chmod(0o700)
        with self.assertRaisesRegex(exporter.ExportError, 'formatter execution failed') as caught:
            self.prepare(self.source, self.output, gofmt=str(tool))
        self.assertNotIn('synthetic-private-diagnostic', str(caught.exception))
        self.assertFalse(self.output.exists())


    def seed_release_inputs(self):
        (self.source / 'LICENSE').write_text('Mozilla Public License Version 2.0\n')
        (self.source / 'NOTICE').write_text('synthetic notice\n')

    def approval_for(self, manifest):
        approval = self.root / 'approval.json'
        approval.write_bytes(exporter.release.encoded({
            'format': 1, 'decision': 'approve-release', 'module': manifest['module'],
            'version': manifest['release_version'],
            'candidate_manifest_sha256': exporter.release.sha(exporter.release.encoded(manifest)),
            'approved_by': 'synthetic-maintainer', 'approved_at': '2026-10-27T00:00:00Z',
            'source_publication_confirmed': True}))
        return approval

    def test_candidate_has_license_without_review_marker_or_publication_approval(self):
        self.seed_release_inputs()
        result = self.prepare(self.source, self.output, 'example.org/team/target',
                                  mode='candidate', version='0.1.0')
        self.assertEqual('RELEASE_CANDIDATE', result['status'])
        self.assertFalse(result['publication_authorized'])
        self.assertFalse((self.output / 'REVIEW-ONLY.md').exists())
        self.assertTrue((self.output / 'LICENSE').is_file())

    def test_release_requires_approval_bound_to_exact_candidate(self):
        self.seed_release_inputs()
        args = {'module': 'example.org/team/target', 'mode': 'release', 'version': '0.1.0'}
        with self.assertRaisesRegex(exporter.ExportError, 'approval record'):
            self.prepare(self.source, self.output, **args)
        self.assertFalse(self.output.exists())
        candidate = self.root / 'candidate'
        self.prepare(self.source, candidate, 'example.org/team/target', mode='candidate', version='0.1.0')
        manifest = json.loads((candidate / 'SOURCE-MANIFEST.json').read_text())
        approval = self.approval_for(manifest)
        result = self.prepare(self.source, self.output, approval=approval, **args)
        self.assertEqual('RELEASE_APPROVED', result['status'])
        self.assertTrue(result['publication_authorized'])
        self.assertFalse((self.output / 'REVIEW-ONLY.md').exists())
        (self.source / 'README.md').write_text('changed after approval\n')
        with self.assertRaisesRegex(exporter.ExportError, 'does not match'):
            self.prepare(self.source, self.root / 'changed-release', approval=approval, **args)
        self.assertFalse((self.root / 'changed-release').exists())

    def test_missing_license_placeholder_and_bad_version_block_candidate(self):
        with self.assertRaisesRegex(exporter.ExportError, 'license inputs'):
            self.prepare(self.source, self.output, 'example.org/team/target', mode='candidate', version='0.1.0')
        self.seed_release_inputs()
        with self.assertRaisesRegex(exporter.ExportError, 'public module'):
            self.prepare(self.source, self.output, mode='candidate', version='0.1.0')
        with self.assertRaisesRegex(exporter.ExportError, 'semantic version'):
            self.prepare(self.source, self.output, 'example.org/team/target', mode='candidate', version='../escape')
        self.assertFalse(self.output.exists())

    def test_approval_cannot_be_reused_for_other_module_version_or_without_scope(self):
        self.seed_release_inputs()
        candidate = self.root / 'candidate'
        self.prepare(self.source, candidate, 'example.org/team/target', mode='candidate', version='0.1.0')
        manifest = json.loads((candidate / 'SOURCE-MANIFEST.json').read_text())
        approval = self.approval_for(manifest)
        for updates in ({'version': '0.2.0'}, {'module': 'example.org/other/target'},
                        {'source_publication_confirmed': False}, {'approved_by': ''},
                        {'approved_at': 'not-a-date'}, {'decision': 'pending'}):
            self.approval_for(manifest)
            value = json.loads(approval.read_text()); value.update(updates)
            approval.write_bytes(exporter.release.encoded(value))
            with self.subTest(updates=updates), self.assertRaises(exporter.ExportError):
                self.prepare(self.source, self.output, 'example.org/team/target',
                                 mode='release', version='0.1.0', approval=approval)
            self.assertFalse(self.output.exists())


if __name__ == '__main__':
    unittest.main()
