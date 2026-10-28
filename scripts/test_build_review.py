import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location('build_review', str(Path(__file__).with_name('build_review.py')))
builder = importlib.util.module_from_spec(spec)
spec.loader.exec_module(builder)


class ReviewBuildGuards(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / 'source'
        self.source.mkdir()
        for name in ('main.go', 'go.mod', 'go.sum'):
            (self.source / name).write_text('fixture\n')
        self.manifest = {'format': 1, 'status': 'REVIEW_ONLY', 'publication_authorized': False,
                         'module': 'example.invalid/target',
                         'files': {name: builder.digest((self.source / name).read_bytes())
                                   for name in ('main.go', 'go.mod', 'go.sum')}}
        self.write_manifest()
        (self.source / 'REVIEW-ONLY.md').write_text('review only\n')
        self.output = self.root / 'output'

    def write_manifest(self):
        (self.source / 'SOURCE-MANIFEST.json').write_text(json.dumps(self.manifest))

    def test_exact_snapshot_and_unapproved_status(self):
        manifest, raw, files = builder.snapshot(self.source)
        self.assertFalse(manifest['publication_authorized'])
        self.assertEqual(5, len(files))
        self.assertEqual(raw, (self.source / 'SOURCE-MANIFEST.json').read_bytes())

    def test_changed_missing_and_extra_files_are_rejected_before_go(self):
        for mutation in ('changed', 'missing', 'extra'):
            with self.subTest(mutation=mutation):
                path = self.source / 'go.sum'
                path.write_text('fixture\n')
                extra = self.source / 'unexpected.env'
                if mutation == 'changed':
                    path.write_text('changed')
                elif mutation == 'missing':
                    path.unlink()
                else:
                    extra.write_text('not a source input')
                with mock.patch.object(builder, 'run') as run:
                    with self.assertRaises(builder.BuildError):
                        builder.build(self.source, self.output)
                    run.assert_not_called()
                self.assertFalse(self.output.exists())
                if extra.exists():
                    extra.unlink()

    def test_unsafe_paths_and_duplicate_json_fields_are_rejected(self):
        for path in ('../escape', '/absolute', 'a/../b', 'a//b', 'SOURCE-MANIFEST.json'):
            with self.subTest(path=path):
                self.manifest['files'][path] = 'a' * 64
                self.write_manifest()
                with self.assertRaises(builder.BuildError):
                    builder.snapshot(self.source)
                del self.manifest['files'][path]
        with self.assertRaises(builder.BuildError):
            builder.json_stream(b'{"field":1,"field":2}')

    def test_symlink_and_existing_or_nested_outputs_are_rejected(self):
        (self.source / 'link').symlink_to(self.source / 'main.go')
        with self.assertRaises(builder.BuildError):
            builder.snapshot(self.source)
        self.output.mkdir()
        (self.output / 'keep').write_text('unchanged')
        with self.assertRaises(builder.BuildError):
            builder.new_output(self.source, self.output)
        with self.assertRaises(builder.BuildError):
            builder.new_output(self.source, self.source / 'nested')
        self.assertEqual('unchanged', (self.output / 'keep').read_text())

    def test_build_environment_drops_auth_and_ambient_go_overrides(self):
        values = {'PATH': '/usr/bin', 'HOME': '/tmp/home', 'TENCENTCLOUD_SECRET_KEY': 'synthetic',
                  'GOFLAGS': '-overlay=unexpected', 'GOPRIVATE': 'private.example',
                  'GOAUTH': 'netrc', 'GOWORK': 'unexpected.work', 'GOAMD64': 'v3'}
        with mock.patch.dict(builder.os.environ, values, clear=True):
            env = builder.build_environment()
        self.assertNotIn('TENCENTCLOUD_SECRET_KEY', env)
        self.assertNotIn('GOAMD64', env)
        self.assertEqual('', env['GOPRIVATE'])
        self.assertEqual('off', env['GOAUTH'])
        self.assertEqual('off', env['GOWORK'])
        self.assertEqual('-p=2', env['GOFLAGS'])

    def test_license_inventory_copies_root_candidates_without_approving(self):
        package = self.root / 'dependency'
        package.mkdir()
        (package / 'LICENSE.txt').write_text('synthetic license candidate')
        (package / 'README.md').write_text('not a license candidate')
        (package / 'copyright').mkdir()
        (package / 'copyright' / 'utility.go').write_text('package copyright\n')
        self.output.mkdir()
        result = builder.license_candidates(package, self.output, '0001')
        self.assertEqual(1, len(result))
        self.assertEqual('dependency-licenses/0001/LICENSE.txt', result[0]['file'])
        raw = (self.output / result[0]['file']).read_bytes()
        self.assertEqual(builder.digest(raw), result[0]['sha256'])
        (package / 'NOTICE').symlink_to(package / 'LICENSE.txt')
        with self.assertRaises(builder.BuildError):
            builder.license_candidates(package, self.output, '0002')


    def test_candidate_snapshot_rejects_review_marker_and_mode_mismatch_before_go(self):
        self.manifest.update(status='RELEASE_CANDIDATE', release_version='0.1.0', license='MPL-2.0')
        for name in builder.release.REQUIRED_LEGAL:
            path = self.source / name; path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('synthetic\n'); self.manifest['files'][name] = builder.digest(path.read_bytes())
        self.write_manifest()
        with self.assertRaises(builder.BuildError): builder.snapshot(self.source)
        (self.source / 'REVIEW-ONLY.md').unlink()
        self.assertEqual('RELEASE_CANDIDATE', builder.snapshot(self.source)[0]['status'])
        for mode, version in (('review', '0.1.0'), ('release', '0.1.0'), ('candidate', '0.2.0')):
            with mock.patch.object(builder, 'run') as run, self.assertRaises(builder.BuildError):
                builder.build(self.source, self.output, mode=mode, version=version)
            run.assert_not_called()
        self.assertFalse(self.output.exists())
        self.manifest.update(status='RELEASE_APPROVED', publication_authorized=True)
        self.write_manifest()
        with self.assertRaises(builder.BuildError): builder.snapshot(self.source)

    def test_notice_inventory_detects_module_text_and_checksum_drift(self):
        module = self.root / 'module'; module.mkdir()
        goroot = self.root / 'go'; goroot.mkdir(); (goroot / 'VERSION').write_text('go1.26.6\n')
        raw = b'synthetic original notice\r\nsecond line\r\n'; digest = builder.digest(raw)
        for root in (module, goroot): (root / 'LICENSE').write_bytes(raw)
        index = {'go_sum_sha256': builder.digest((self.source / 'go.sum').read_bytes()), 'modules': [
            {'module': m, 'version': v, 'files': [{'path': 'LICENSE', 'sha256': digest, 'bytes': len(raw)}]}
            for m, v in (('example.org/module', 'v1.0.0'), ('go-toolchain', 'go1.26.6'))]}
        (self.source / 'docs').mkdir()
        path = self.source / 'docs/dependency-licenses.json'; path.write_text(json.dumps(index))
        notices = self.source / 'THIRD_PARTY_NOTICES.md'
        original = '# Notices\n\n## ' + digest + '\n\n```text\n' + raw.decode().replace('\r\n', '\n') + '```\n'
        notices.write_text(original)
        modules = [{'Path': 'example.org/module', 'Version': 'v1.0.0'}]
        downloads = {('example.org/module', 'v1.0.0'): {'Dir': str(module)}}
        builder.verify_notice_inventory(self.source, modules, downloads, goroot)
        notices.write_text(original.replace('synthetic original', 'modified'))
        with self.assertRaisesRegex(builder.BuildError, 'notice text'): builder.verify_notice_inventory(self.source, modules, downloads, goroot)
        notices.write_text(original); (module / 'LICENSE').write_text('changed')
        with self.assertRaisesRegex(builder.BuildError, 'notice bytes'): builder.verify_notice_inventory(self.source, modules, downloads, goroot)
        (module / 'LICENSE').write_bytes(raw); (self.source / 'go.sum').write_text('changed')
        with self.assertRaisesRegex(builder.BuildError, 'go.sum'): builder.verify_notice_inventory(self.source, modules, downloads, goroot)


if __name__ == '__main__':
    unittest.main()
