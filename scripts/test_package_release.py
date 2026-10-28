import json
from pathlib import Path
import tarfile
import unittest

import build_review as builder
import package_release as packager
import prepare_source as exporter
import test_prepare_source as fixtures


class PackageReleaseGuards(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.PrepareSourceTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.seed_release_inputs()
        self.root = self.fixture.root
        self.source = self.root / 'candidate'
        self.fixture.prepare(self.fixture.source, self.source, 'example.org/team/target',
                         mode='candidate', version='0.1.0')
        self.artifacts = self.root / 'artifacts'
        self.artifacts.mkdir()
        self.output = self.root / 'packages'
        self.validation = self.root / 'validation.json'
        self.seed_build(self.source)

    def seed_build(self, source):
        manifest, raw, files = builder.snapshot(source)
        hashes, info_hashes = {}, {}
        for arch in ('amd64', 'arm64'):
            for binary in ('tencent-as', 'tencent-recover'):
                name = 'linux-' + arch + '/' + binary
                path = self.artifacts / name
                path.parent.mkdir(exist_ok=True)
                path.write_bytes(('synthetic binary ' + name).encode())
                hashes[name] = builder.digest(path.read_bytes())
                info = name + '.buildinfo.txt'
                (self.artifacts / info).write_text('synthetic build info\n')
                info_hashes[info] = builder.digest((self.artifacts / info).read_bytes())
        deps = {'status': 'NOTICES_VERIFIED', 'modules': [], 'go_toolchain_root_candidates': []}
        (self.artifacts / 'DEPENDENCIES.json').write_text(json.dumps(deps))
        self.build = {'status': manifest['status'], 'publication_authorized': manifest['publication_authorized'],
                      'module': manifest['module'], 'declared_version': '0.1.0',
                      'source_manifest_sha256': builder.digest(raw), 'source_file_hashes': files,
                      'license_review': 'NOTICES_VERIFIED', 'artifacts': hashes,
                      'build_info_sha256': info_hashes,
                      'dependency_inventory_sha256': builder.digest((self.artifacts / 'DEPENDENCIES.json').read_bytes())}
        self.save_build()
        self.validation.write_text(json.dumps({'format': 2, 'source_manifest_sha256': builder.digest(raw),
                                              'module': manifest['module'], 'declared_version': '0.1.0',
                                              'github_ci_run': False, 'installation_verified': False,
                                              'remaining': ['installation_verification']}))

    def save_build(self):
        (self.artifacts / 'BUILD-MANIFEST.json').write_text(json.dumps(self.build))

    def package(self, **kwargs):
        return packager.package(self.source, self.artifacts, self.output, self.validation, **kwargs)

    def test_candidate_archives_have_normal_names_license_and_no_review_marker(self):
        report = self.package()
        self.assertFalse(report['publication_authorized'])
        self.assertFalse(report['published_by_this_command'])
        for archive in self.output.glob('*.tar.gz'):
            self.assertNotIn('-review', archive.name)
            with tarfile.open(archive) as tar:
                names = tar.getnames()
                self.assertIn('nomad-autoscaler-tencent-0.1.0/LICENSE', names)
                self.assertFalse(any(name.endswith('REVIEW-ONLY.md') for name in names))
                if 'linux_' in archive.name:
                    info = json.load(tar.extractfile('nomad-autoscaler-tencent-0.1.0/VALIDATION.json'))
                    self.assertFalse(info['github_ci_run'])
                    self.assertFalse(info['installation_verified'])
        for line in (self.output / 'SHA256SUMS').read_text().splitlines():
            expected, name = line.split('  ', 1)
            self.assertEqual(expected, builder.digest((self.output / name).read_bytes()))
        with self.assertRaises(builder.BuildError): self.package()

    def test_changed_binary_buildinfo_inventory_and_source_are_rejected(self):
        for name in ('linux-amd64/tencent-as', 'linux-arm64/tencent-recover.buildinfo.txt', 'DEPENDENCIES.json'):
            path = self.artifacts / name; original = path.read_bytes(); path.write_bytes(b'changed')
            with self.subTest(name=name), self.assertRaises(builder.BuildError): self.package()
            self.assertFalse(self.output.exists()); path.write_bytes(original)
        (self.source / 'README.md').write_text('changed source')
        with self.assertRaises(builder.BuildError): self.package()
        self.assertFalse(self.output.exists())

    def test_stale_evidence_or_unverified_notices_cannot_be_packed(self):
        original = self.validation.read_bytes()
        value = json.loads(original); value['source_manifest_sha256'] = '0' * 64
        self.validation.write_text(json.dumps(value))
        with self.assertRaisesRegex(builder.BuildError, 'another source'): self.package()
        self.validation.write_bytes(original)
        self.build['license_review'] = 'UNREVIEWED'; self.save_build()
        with self.assertRaises(builder.BuildError): self.package()
        self.assertFalse(self.output.exists())

    def test_symlink_binary_does_not_pass_matching_content_hash(self):
        path = self.artifacts / 'linux-amd64/tencent-as'
        other = self.root / 'other'; other.write_bytes(path.read_bytes())
        path.unlink(); path.symlink_to(other)
        with self.assertRaises(builder.BuildError): self.package()
        self.assertFalse(self.output.exists())

    def test_validation_rejects_unknown_fields_legacy_schema_and_freeform_remaining(self):
        original = json.loads(self.validation.read_text())
        for update in ({'format': 1}, {'private_notes': 'not for publication'},
                       {'remaining': ['unreviewed project context']},
                       {'installation_verified': 'false'}):
            self.validation.write_text(json.dumps(dict(original, **update)))
            with self.subTest(update=update), self.assertRaisesRegex(builder.BuildError, 'incomplete'):
                self.package()
            self.assertFalse(self.output.exists())

    def test_validation_private_rules_block_metadata_without_echo(self):
        value = json.loads(self.validation.read_text())
        payload = 'https://PRIVATE.EXAMPLE/validation'
        value['cloud_evidence'] = payload
        self.validation.write_text(json.dumps(value))
        with self.assertRaisesRegex(builder.BuildError, 'private material') as caught:
            self.package(privacy_policy=self.fixture.privacy_policy)
        self.assertNotIn(payload, str(caught.exception))
        self.assertFalse(self.output.exists())

    def test_validation_sensitive_identifiers_are_blocked_without_private_rules(self):
        original = json.loads(self.validation.read_text())
        for payload in ('AccountId=' + '1234567890', 'address=' + '.'.join(('10', '20', '30', '40'))):
            self.validation.write_text(json.dumps(dict(original, cloud_evidence=payload)))
            with self.subTest(payload=payload), self.assertRaisesRegex(builder.BuildError, 'private material'):
                self.package()
            self.assertFalse(self.output.exists())

    def test_release_requires_matching_approval_and_never_claims_publication(self):
        with self.assertRaises(builder.BuildError): self.package(mode='release')
        manifest = json.loads((self.source / 'SOURCE-MANIFEST.json').read_text())
        approval = self.fixture.approval_for(manifest)
        approved = self.root / 'approved'
        self.fixture.prepare(self.fixture.source, approved, 'example.org/team/target',
                         mode='release', version='0.1.0', approval=approval)
        self.source = approved; self.seed_build(approved)
        with self.assertRaisesRegex(builder.BuildError, 'approval record'): self.package(mode='release')
        report = self.package(mode='release', approval=approval)
        self.assertTrue(report['publication_authorized'])
        self.assertFalse(report['published_by_this_command'])
        self.assertEqual('RELEASE_APPROVED', report['status'])


if __name__ == '__main__':
    unittest.main()
