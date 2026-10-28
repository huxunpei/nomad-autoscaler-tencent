#!/usr/bin/env python3
"""Build artifacts from an exact exported snapshot; never publish."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys

import release_contract as release

SAFE_PATH = re.compile(r'^[A-Za-z0-9._/-]+$')
HASH = re.compile(r'^[0-9a-f]{64}$')
MODULE = re.compile(r'^[A-Za-z0-9][A-Za-z0-9._/-]+$')
VERSION = re.compile(r'^v[0-9][A-Za-z0-9.+-]*$')
LEGAL_FILE = re.compile(r'^(?:LICENSE|LICENCE|COPYING|NOTICE|COPYRIGHT)(?:[._-].*)?$', re.I)
TARGETS = (('tencent-as', '.'), ('tencent-recover', './cmd/tencent-recover'))


class BuildError(Exception):
    pass


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise BuildError('duplicate JSON field')
        result[key] = value
    return result


def json_stream(raw):
    decoder = json.JSONDecoder(object_pairs_hook=unique_object)
    text, result = raw.decode('utf-8'), []
    while text.strip():
        value, end = decoder.raw_decode(text.lstrip())
        result.append(value)
        text = text.lstrip()[end:]
    return result


def snapshot(source):
    manifest_path = source / 'SOURCE-MANIFEST.json'
    if manifest_path.is_symlink():
        raise BuildError('source manifest must not be a symlink')
    raw = manifest_path.read_bytes()
    manifest = json.loads(raw.decode('utf-8'), object_pairs_hook=unique_object)
    if (manifest.get('format') != 1 or not isinstance(manifest.get('module'), str) or
            not MODULE.fullmatch(manifest['module'])):
        raise BuildError('a valid source manifest is required')
    try:
        mode = release.validate_manifest(manifest)
    except release.ReleaseError as error:
        raise BuildError(str(error))
    files = manifest.get('files')
    if not isinstance(files, dict) or not {'main.go', 'go.mod', 'go.sum'} <= set(files):
        raise BuildError('source file manifest is incomplete')
    for name, expected in files.items():
        if (not isinstance(name, str) or not SAFE_PATH.fullmatch(name) or
                Path(name).is_absolute() or any(x in ('', '.', '..') for x in name.split('/')) or
                name in ('SOURCE-MANIFEST.json', 'REVIEW-ONLY.md') or
                not isinstance(expected, str) or not HASH.fullmatch(expected)):
            raise BuildError('unsafe source manifest entry')
    actual = {}
    for path in source.rglob('*'):
        if path.is_symlink():
            raise BuildError('symlinks are not source inputs')
        if path.is_file():
            actual[str(path.relative_to(source))] = digest(path.read_bytes())
        elif not path.is_dir():
            raise BuildError('nonregular source input')
    expected_names = set(files) | {'SOURCE-MANIFEST.json'}
    if mode == 'review':
        expected_names.add('REVIEW-ONLY.md')
    if set(actual) != expected_names or any(actual[name] != sha for name, sha in files.items()):
        raise BuildError('source snapshot contains changed, missing or extra files')
    return manifest, raw, actual


def new_output(source, output):
    source = Path(source).resolve()
    output = Path(output).absolute()
    if output.exists() or output.is_symlink() or not output.parent.is_dir():
        raise BuildError('output must be NEW with an existing parent')
    output = output.parent.resolve() / output.name
    try:
        output.relative_to(source)
    except ValueError:
        return output
    raise BuildError('output must be outside source')


def build_environment():
    # Do not forward cloud credentials, arbitrary Go flags, private proxy auth,
    # workspace files or user Go configuration into this public build.
    allowed = ('PATH', 'HOME', 'GOCACHE', 'GOMODCACHE', 'GOPATH', 'TMPDIR',
               'GOMAXPROCS', 'GOMEMLIMIT')
    env = {key: os.environ[key] for key in allowed if key in os.environ}
    env.update({'GOENV': 'off', 'GOWORK': 'off', 'GOTOOLCHAIN': 'local',
                'GOPROXY': 'https://proxy.golang.org', 'GOSUMDB': 'sum.golang.org',
                'GOPRIVATE': '', 'GONOPROXY': '', 'GONOSUMDB': '', 'GOAUTH': 'off',
                'GOVCS': '*:off', 'GOFLAGS': '-p=2', 'CGO_ENABLED': '0',
                'GOOS': 'linux', 'GOARCH': 'amd64'})
    return env


def run(argv, cwd, env, output):
    try:
        result = subprocess.run(argv, cwd=str(cwd), env=env, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, timeout=600)
    except subprocess.TimeoutExpired as error:
        (output / 'command-failure.log').write_bytes((error.stdout or b'') + (error.stderr or b''))
        raise BuildError('build command timed out; partial output retained')
    if result.returncode:
        (output / 'command-failure.log').write_bytes(result.stdout + result.stderr)
        raise BuildError('build command failed; see command-failure.log')
    return result.stdout


def license_candidates(directory, output, prefix):
    result = []
    for path in sorted(directory.iterdir()):
        if not LEGAL_FILE.fullmatch(path.name):
            continue
        # For example, x/tools/copyright is a Go package directory, not a
        # root license file. Nested notices remain a separate review scope.
        if path.is_dir() and not path.is_symlink():
            continue
        if (path.is_symlink() or not path.is_file() or path.stat().st_size > 2 * 1024 * 1024 or
                not re.fullmatch(r'[A-Za-z0-9._-]+', path.name)):
            raise BuildError('unsupported root license candidate: ' + path.name)
        raw = path.read_bytes()
        relative = 'dependency-licenses/' + prefix + '/' + path.name
        target = output / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(raw)
        result.append({'file': relative, 'sha256': digest(raw), 'bytes': len(raw)})
    return result


def dependencies(go, source, output, env, expected_module, verify_notices=False):
    rows = json_stream(run([go, 'list', '-m', '-mod=readonly', '-json', 'all'], source, env, output))
    mains = [x for x in rows if x.get('Main')]
    if len(mains) != 1 or mains[0].get('Path') != expected_module:
        raise BuildError('actual main module differs from the source manifest')
    modules = sorted((x for x in rows if not x.get('Main')), key=lambda x: x['Path'])
    for row in modules:
        if (row.get('Replace') or not MODULE.fullmatch(row.get('Path', '')) or
                not VERSION.fullmatch(row.get('Version', ''))):
            raise BuildError('unversioned or replaced dependency needs separate review')
    # Explicit path@version downloads run outside the source tree; never edit its sums.
    argv = [go, 'mod', 'download', '-json'] + [x['Path'] + '@' + x['Version'] for x in modules]
    downloads = json_stream(run(argv, output, env, output))
    verified = run([go, 'mod', 'verify'], source, env, output)
    (output / 'MODULE-CACHE-VERIFY.txt').write_bytes(verified)
    index = {(x['Path'], x['Version']): x for x in downloads}
    result = []
    for number, row in enumerate(modules, 1):
        item = index.get((row['Path'], row['Version']), {})
        if item.get('Error') or not item.get('Dir') or not item.get('Sum') or not item.get('GoModSum'):
            raise BuildError('dependency download/provenance is incomplete')
        candidates = license_candidates(Path(item['Dir']), output, '{:04d}'.format(number))
        result.append({'module': row['Path'], 'version': row['Version'],
                       'sum': item['Sum'], 'go_mod_sum': item['GoModSum'],
                       'indirect': bool(row.get('Indirect')), 'license_files': candidates,
                       'license_review': 'UNREVIEWED' if candidates else 'MISSING_ROOT_CANDIDATE'})
    goroot = Path(run([go, 'env', 'GOROOT'], source, env, output).decode().strip())
    toolchain = license_candidates(goroot, output, 'go-toolchain')
    inventory = {'format': 1, 'status': 'UNREVIEWED', 'modules': result,
                 'go_toolchain_root_candidates': toolchain,
                 'scope': 'selected module graph and root legal files only; not a license clearance or complete NOTICE'}
    if verify_notices:
        verify_notice_inventory(source, modules, index, goroot)
        inventory['status'] = 'NOTICES_VERIFIED'
        inventory['scope'] = 'Pinned module/toolchain license and notice bytes checked against source inventory; not legal clearance'
        inventory['source_notice_index_sha256'] = digest((source / 'docs/dependency-licenses.json').read_bytes())
        for item in result:
            item['license_review'] = 'NOTICES_VERIFIED'
    (output / 'DEPENDENCIES.json').write_text(json.dumps(inventory, indent=2, sort_keys=True) + '\n')
    return inventory


def verify_notice_inventory(source, modules, downloads, goroot):
    try:
        notice_index, _ = release.read_json(source / 'docs/dependency-licenses.json')
    except release.ReleaseError as error:
        raise BuildError(str(error))
    if notice_index.get('go_sum_sha256') != digest((source / 'go.sum').read_bytes()):
        raise BuildError('notice inventory does not match go.sum')
    rows = notice_index.get('modules')
    if not isinstance(rows, list) or any(not isinstance(x, dict) for x in rows):
        raise BuildError('invalid notice inventory')
    expected = {(x['Path'], x['Version']): Path(downloads[(x['Path'], x['Version'])]['Dir'])
                for x in modules}
    expected[('go-toolchain', (goroot / 'VERSION').read_text().splitlines()[0])] = goroot
    actual = {(x.get('module'), x.get('version')): x for x in rows}
    if len(actual) != len(rows) or set(actual) != set(expected):
        raise BuildError('notice inventory module/toolchain set differs from build inputs')
    notices = (source / 'THIRD_PARTY_NOTICES.md').read_text()
    sections = {}
    for section in notices.split('\n## ')[1:]:
        heading, _, body = section.partition('\n')
        if HASH.fullmatch(heading.strip()) and '\n```text\n' in body:
            text = body.split('\n```text\n', 1)[1]
            if '\n```' in text:
                sections[heading.strip()] = text.rsplit('\n```', 1)[0].rstrip()
    for key, row in actual.items():
        files = row.get('files')
        if not isinstance(files, list) or not files:
            raise BuildError('module has no indexed notice files')
        seen = set()
        for item in files:
            name = item.get('path') if isinstance(item, dict) else None
            if (not isinstance(name, str) or not SAFE_PATH.fullmatch(name) or
                    Path(name).is_absolute() or any(x in ('', '.', '..') for x in name.split('/')) or name in seen):
                raise BuildError('unsafe or duplicate notice file')
            seen.add(name)
            root = expected[key]
            path = root / name
            if (any(p.is_symlink() for p in (path,) + tuple(path.parents) if p != root and root in p.parents) or
                    not path.is_file() or path.stat().st_size > 2 * 1024 * 1024):
                raise BuildError('invalid notice file in module cache/toolchain')
            raw = path.read_bytes()
            if digest(raw) != item.get('sha256') or len(raw) != item.get('bytes'):
                raise BuildError('indexed notice bytes differ from module cache/toolchain')
            try:
                included = sections.get(item['sha256']) == raw.decode('utf-8').replace('\r\n', '\n').replace('\r', '\n').rstrip()
            except UnicodeError:
                included = False
            if not included:
                raise BuildError('third-party notice text is missing or modified')


def build(source, output, go='go', version='0.0.0-review', revision=None,
          mode='review', approval=None):
    source = Path(source).resolve()
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9.+_-]{0,63}', version):
        raise BuildError('invalid declared version label')
    if revision is not None and not re.fullmatch(r'(?:[0-9a-f]{40}|[0-9a-f]{64})', revision):
        raise BuildError('invalid declared revision')
    manifest, raw, inputs = snapshot(source)
    try:
        release.check_mode(manifest, mode, approval)
    except release.ReleaseError as error:
        raise BuildError(str(error))
    if mode != 'review' and version != manifest['release_version']:
        raise BuildError('build version differs from the approved/candidate source')
    output = new_output(source, output)
    output.mkdir(mode=0o700)
    env = build_environment()
    go_version = run([go, 'version'], source, env, output).decode().strip()
    inventory = dependencies(go, source, output, env, manifest['module'], mode != 'review')
    artifacts, commands, build_info = {}, [], {}
    for arch in ('amd64', 'arm64'):
        env['GOARCH'] = arch
        for name, package in TARGETS:
            relative = 'linux-' + arch + '/' + name
            target = output / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            flags = ['build', '-mod=readonly', '-trimpath', '-buildvcs=false', '-pgo=off', '-o']
            run([go] + flags + [str(target), package], source, env, output)
            info = run([go, 'version', '-m', str(target)], source, env, output).decode().splitlines()
            if not info or '\tbuild\tGOARCH=' + arch not in info or '\tbuild\tCGO_ENABLED=0' not in info:
                raise BuildError('artifact build information differs from requested target')
            info_path = relative + '.buildinfo.txt'
            (output / info_path).write_text(relative + ': ' + info[0].rsplit(': ', 1)[-1] + '\n' + '\n'.join(info[1:]) + '\n')
            build_info[info_path] = digest((output / info_path).read_bytes())
            artifacts[relative] = digest(target.read_bytes())
            commands.append({'argv': ['go'] + flags + [relative, package], 'GOARCH': arch})
    if snapshot(source)[2] != inputs:
        raise BuildError('source changed during build; artifacts are not validated')
    result = {'format': 1, 'status': manifest['status'],
              'publication_authorized': manifest['publication_authorized'],
              'module': manifest['module'], 'declared_version': version,
              'declared_revision': revision, 'source_commit_verified': False,
              'source_manifest_sha256': digest(raw), 'source_file_hashes': inputs,
              'go_version': go_version, 'build_commands': commands,
              'build_policy': {x: env[x] for x in ('CGO_ENABLED', 'GOOS', 'GOTOOLCHAIN', 'GOENV', 'GOWORK', 'GOPROXY', 'GOVCS')},
              'dependency_modules': len(inventory['modules']), 'license_review': inventory['status'],
              'dependency_inventory_sha256': digest((output / 'DEPENDENCIES.json').read_bytes()),
              'module_cache_verified': True,
              'module_cache_verification_sha256': digest((output / 'MODULE-CACHE-VERIFY.txt').read_bytes()),
              'build_info_sha256': build_info,
              'tests_executed_by_this_command': False, 'artifacts': artifacts}
    (output / 'BUILD-MANIFEST.json').write_text(json.dumps(result, indent=2, sort_keys=True) + '\n')
    (output / 'INPUT-SOURCE-MANIFEST.json').write_bytes(raw)
    (output / 'SHA256SUMS').write_text(''.join(sha + '  ' + name + '\n' for name, sha in sorted(artifacts.items())))
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--go', default='go')
    parser.add_argument('--version', default='0.0.0-review')
    parser.add_argument('--revision')
    parser.add_argument('--mode', choices=sorted(release.STATUSES), default='review')
    parser.add_argument('--approval')
    args = parser.parse_args(argv)
    try:
        result = build(args.source, args.output, args.go, args.version, args.revision, args.mode, args.approval)
    except (BuildError, OSError, ValueError) as error:
        print('review build failed: ' + str(error), file=sys.stderr)
        return 1
    print(json.dumps({'status': result['status'], 'artifacts': len(result['artifacts']),
                      'dependency_modules': result['dependency_modules'], 'publication_authorized': result['publication_authorized']}))
    return 0


if __name__ == '__main__':
    sys.exit(main())
