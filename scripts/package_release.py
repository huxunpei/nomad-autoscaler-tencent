#!/usr/bin/env python3
"""Package an exact candidate or approved release. Never upload, tag or publish."""
import argparse
import gzip
import io
import json
from pathlib import Path
import sys
import tarfile

import build_review as builder
import release_contract as release

PUBLIC_VALIDATION_FIELDS = frozenset((
    'format', 'source_manifest_sha256', 'module', 'declared_version',
    'github_ci_run', 'installation_verified', 'remaining', 'status',
    'publication_authorized', 'source_publication_confirmed',
    'agent_version', 'nomad_version', 'go_version', 'architecture_load',
    'binary_reproducibility', 'binary_hashes_match_previous_candidate',
    'cloud_evidence', 'cloud_response_loss_coverage', 'current_cloud_tests_rerun',
    'current_product_tests_rerun', 'default_registration_grace_full_path',
    'go_module_inputs_unchanged', 'go_test_packages_passed', 'go_test_pass_events',
    'license_notice_verification', 'product_validation_reused',
    'product_validation_source_manifest_sha256', 'product_source_change',
    'runtime_go_inputs_identical', 'skips_recorded', 'targeted_runtime',
    'tool_protection_test_suites', 'tool_protection_tests_passed',
    'vet_race_format_module_verify_passed'))
PUBLIC_REMAINING = frozenset(('source_publication_authorization', 'github_ci',
                              'installation_verification', 'publication'))


def file_bytes(path, limit=256 * 1024 * 1024):
    if path.is_symlink() or not path.is_file() or path.stat().st_size > limit:
        raise builder.BuildError('missing or unsafe package input')
    return path.read_bytes()


def write_archive(path, prefix, entries):
    with path.open('xb') as stream:
        with gzip.GzipFile(fileobj=stream, filename='', mode='wb', mtime=0) as zipped:
            with tarfile.open(fileobj=zipped, mode='w') as archive:
                for name, (raw, executable) in sorted(entries.items()):
                    info = tarfile.TarInfo(prefix + '/' + name)
                    info.size, info.mode = len(raw), 0o755 if executable else 0o644
                    info.uid = info.gid = info.mtime = 0
                    info.uname = info.gname = ''
                    archive.addfile(info, io.BytesIO(raw))


def package(source, artifacts, output, validation, mode='candidate', approval=None,
            privacy_policy=None):
    source, artifacts = Path(source).resolve(), Path(artifacts).resolve()
    manifest, source_raw, inputs = builder.snapshot(source)
    try:
        release.check_mode(manifest, mode, approval)
        build, build_raw = release.read_json(artifacts / 'BUILD-MANIFEST.json')
        evidence, _ = release.read_json(validation)
    except release.ReleaseError as error:
        raise builder.BuildError(str(error))
    if mode not in ('candidate', 'release'):
        raise builder.BuildError('packaging requires candidate or release mode')
    if (build.get('status') != manifest['status'] or
            build.get('publication_authorized') is not manifest['publication_authorized'] or
            build.get('source_manifest_sha256') != builder.digest(source_raw) or
            build.get('module') != manifest['module'] or
            build.get('declared_version') != manifest['release_version'] or
            build.get('source_file_hashes') != inputs or
            build.get('license_review') != 'NOTICES_VERIFIED'):
        raise builder.BuildError('build does not match the exact source or verified notices')
    if evidence.get('source_manifest_sha256') != builder.digest(source_raw):
        raise builder.BuildError('validation record belongs to another source snapshot')
    if (type(evidence.get('format')) is not int or evidence['format'] != 2 or
            not set(evidence) <= PUBLIC_VALIDATION_FIELDS or
            evidence.get('module') != manifest['module'] or
            evidence.get('declared_version') != manifest['release_version'] or
            not isinstance(evidence.get('github_ci_run'), bool) or
            not isinstance(evidence.get('installation_verified'), bool) or
            not isinstance(evidence.get('remaining'), list) or
            any(not isinstance(x, str) or x not in PUBLIC_REMAINING for x in evidence['remaining'])):
        raise builder.BuildError('validation record is incomplete')
    # Package status describes approval, never an upload that this tool cannot perform.
    evidence.update(status=manifest['status'], publication_authorized=manifest['publication_authorized'])
    evidence_raw = release.encoded(evidence)
    import prepare_source as exporter
    try:
        deny_literals, _ = exporter.read_privacy_policy(privacy_policy)
        exporter.scan('VALIDATION.json', evidence_raw.decode('utf-8'), None, deny_literals)
    except exporter.ExportError:
        raise builder.BuildError('validation record contains private material')
    expected_artifacts = {'linux-' + arch + '/' + name
                          for arch in ('amd64', 'arm64') for name, _ in builder.TARGETS}
    if set(build.get('artifacts', {})) != expected_artifacts:
        raise builder.BuildError('build must contain the four expected artifacts')
    binary = {}
    for name in sorted(expected_artifacts):
        path = artifacts / name
        if path.parent.is_symlink():
            raise builder.BuildError('symlink artifact directory')
        raw = file_bytes(path)
        if builder.digest(raw) != build['artifacts'][name]:
            raise builder.BuildError('binary hash differs from build manifest')
        binary[name] = (raw, True)
        info = name + '.buildinfo.txt'
        raw = file_bytes(artifacts / info, 2 * 1024 * 1024)
        if builder.digest(raw) != build.get('build_info_sha256', {}).get(info):
            raise builder.BuildError('build-info hash differs from manifest')
        binary[info] = (raw, False)
    dependencies_raw = file_bytes(artifacts / 'DEPENDENCIES.json', 2 * 1024 * 1024)
    if builder.digest(dependencies_raw) != build.get('dependency_inventory_sha256'):
        raise builder.BuildError('dependency inventory hash differs from manifest')
    source_files = {name: (file_bytes(source / name, 2 * 1024 * 1024), False) for name in inputs}
    common = {name: value for name, value in source_files.items()
              if name.startswith(('docs/', 'examples/')) or name in
              ('README.md', 'README.zh-CN.md', 'CHANGELOG.md', 'LICENSE', 'NOTICE', 'THIRD_PARTY_NOTICES.md')}
    common.update({'BUILD-MANIFEST.json': (build_raw, False),
                   'SOURCE-MANIFEST.json': (source_raw, False),
                   'DEPENDENCIES.json': (dependencies_raw, False),
                   'VALIDATION.json': (evidence_raw, False)})
    # Pin every copied raw root notice, not arbitrary files from the build directory.
    dependencies = json.loads(dependencies_raw.decode('utf-8'))
    records = [f for row in dependencies['modules'] for f in row['license_files']]
    records += dependencies['go_toolchain_root_candidates']
    for record in records:
        name = record['file']
        if (not name.startswith('dependency-licenses/') or not builder.SAFE_PATH.fullmatch(name) or
                any(x in ('', '.', '..') for x in name.split('/'))):
            raise builder.BuildError('unsafe notice package path')
        path = artifacts / name
        if any(p.is_symlink() for p in path.parents if artifacts in p.parents):
            raise builder.BuildError('symlink notice directory')
        raw = file_bytes(path, 2 * 1024 * 1024)
        if builder.digest(raw) != record['sha256']:
            raise builder.BuildError('raw notice hash differs from inventory')
        common[name] = (raw, False)
    if builder.snapshot(source)[2] != inputs:
        raise builder.BuildError('source changed before packaging')
    output = builder.new_output(source, output)
    if output == artifacts or artifacts in output.parents:
        raise builder.BuildError('package output must be outside build inputs')
    output.mkdir(mode=0o700)
    prefix = 'nomad-autoscaler-tencent-' + manifest['release_version']
    base = 'nomad-autoscaler-tencent_' + manifest['release_version']
    write_archive(output / (base + '_source.tar.gz'), prefix, source_files)
    for arch in ('amd64', 'arm64'):
        entries = dict(common)
        entries.update({name: value for name, value in binary.items() if name.startswith('linux-' + arch + '/')})
        sums = ''.join(build['artifacts'][name] + '  ' + name + '\n' for name in sorted(expected_artifacts)
                       if name.startswith('linux-' + arch + '/'))
        entries['SHA256SUMS'] = (sums.encode('utf-8'), False)
        write_archive(output / (base + '_linux_' + arch + '.tar.gz'), prefix, entries)
    for name, raw in (('SOURCE-MANIFEST.json', source_raw), ('BUILD-MANIFEST.json', build_raw),
                      ('VALIDATION.json', evidence_raw)):
        (output / name).write_bytes(raw)
    hashes = {p.name: builder.digest(p.read_bytes()) for p in sorted(output.iterdir())}
    report = {'format': 1, 'status': manifest['status'], 'module': manifest['module'],
              'declared_version': manifest['release_version'], 'publication_authorized': manifest['publication_authorized'],
              'published_by_this_command': False, 'source_manifest_sha256': builder.digest(source_raw),
              'archive_files': hashes, 'binary_hashes': build['artifacts']}
    (output / 'PACKAGE-REPORT.json').write_bytes(release.encoded(report))
    hashes['PACKAGE-REPORT.json'] = builder.digest((output / 'PACKAGE-REPORT.json').read_bytes())
    (output / 'SHA256SUMS').write_text(''.join(value + '  ' + name + '\n' for name, value in sorted(hashes.items())))
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('source', 'artifacts', 'output', 'validation'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--mode', choices=('candidate', 'release'), default='candidate')
    parser.add_argument('--approval')
    parser.add_argument('--privacy-policy', help='external JSON deny rules for validation metadata')
    args = parser.parse_args(argv)
    try:
        result = package(args.source, args.artifacts, args.output, args.validation,
                         args.mode, args.approval, args.privacy_policy)
    except (builder.BuildError, OSError, ValueError) as error:
        print('packaging failed: ' + str(error), file=sys.stderr)
        return 1
    print(json.dumps({'status': result['status'], 'publication_authorized': result['publication_authorized'],
                      'published_by_this_command': False}))
    return 0


if __name__ == '__main__':
    sys.exit(main())
