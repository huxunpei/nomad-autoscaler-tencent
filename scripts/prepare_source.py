#!/usr/bin/env python3
"""Prepare a NEW review, candidate or approved-release source. Never publish or alter Git."""
import argparse
import hashlib
import ipaddress
import os
import subprocess
import json
from pathlib import Path
import re
import sys

import release_contract as release

PACKAGES = ('plugin', 'internal/live', 'internal/nomadclient', 'internal/nomadsdk',
            'internal/nomadtest', 'internal/targetconfig', 'internal/telemetry', 'internal/policybounds',
            'internal/tencentcloud', 'internal/simulation', 'cmd/tencent-recover', 'cmd/tencent-contract',
            'cmd/tencent-snapshot', 'cmd/nomad-node-probe')
ROOT_FILES = ('main.go', 'go.mod', 'go.sum', 'Makefile', '.gitignore', '.gitattributes', 'README.md',
              'CHANGELOG.md', 'THIRD_PARTY_NOTICES.md', 'docs/dependency-licenses.json',
              'docs/release.md', '.github/workflows/ci.yml',
              'scripts/prepare_source.py', 'scripts/test_prepare_source.py',
              'scripts/release_contract.py', 'scripts/package_release.py', 'scripts/test_package_release.py',
              'README.zh-CN.md', 'scripts/agent_smoke.py', 'docs/configuration.md',
              'docs/permissions.md', 'docs/maintenance.md',
              'docs/development.md', 'docs/operations.md', 'docs/building.md', 'docs/registration-grace.md', 'docs/safe-policy-updates.md',
              'scripts/build_review.py', 'scripts/test_build_review.py',
              'scripts/test_agent_smoke.py', 'docs/architecture-smoke.md',
              'scripts/check_review.py', 'scripts/test_check_review.py', 'docs/checks.md')
EXAMPLES = ('examples/autoscaler.hcl', 'examples/cluster-fixed.hcl',
            'examples/cluster-prometheus.hcl')
# These eight-character identifiers are explicit semantic test values, not an
# allowance for arbitrary real-looking resources or a blanket test-data waiver.
SYNTHETIC_IDS = frozenset(('sg-replaced', 'img-replaced', 'ins-replaced',
                          'asa-followup', 'asa-existing', 'asa-conflict'))
RESOURCE = re.compile(r'\b(?:asg|ins|asc|sg|img|vpc|subnet|asa)-[A-Za-z0-9]{8}\b')
CREDENTIAL = re.compile(r'AKID[A-Za-z0-9]{24,}|-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----')
PRIVATE_PATH = re.compile(r'/(?:Users|home|root)/[^\s\"\']+', re.I)
IPV4 = re.compile(r'(?<![\w.])(?:[0-9]{1,3}\.){3}[0-9]{1,3}(?![\w.])')
PRIVATE_NETWORKS = tuple(ipaddress.ip_network(value) for value in
                         ((0x0a000000, 8), (0xac100000, 12), (0xc0a80000, 16)))
ACCOUNT = re.compile(r'''\b(?:account[_-]?id|uin|appid)["']?\s*[:=]\s*["']?[0-9]{10,20}\b''', re.I)
SECRET_LITERAL = re.compile(r'''\b(?:secret[_-]?key|secret[_-]?id|password|access[_-]?token|api[_-]?key|nomad_token)["']?\s*[:=]\s*["']([A-Za-z0-9/+_=.-]{8,})["']''', re.I)
# Exact semantic fixtures only; test files are still scanned for every other value.
SYNTHETIC_SECRETS = frozenset(('PRIVATE_VALUE', 'test-key', 'synthetic-id', 'synthetic-key',
    'synthetic-file-key', 'synthetic-secret-body', 'synthetic-ignored-secret',
    'synthetic-token', 'synthetic-env-token', 'synthetic-private-token',
    'synthetic-inherited-token', 'synthetic'))
MODULE = re.compile(r'^[a-z0-9][a-z0-9.-]*\.[a-z0-9.-]+/[A-Za-z0-9._/-]+$')
DEFAULT_MODULE = 'example.invalid/nomad-autoscaler-tencent'


class ExportError(Exception):
    pass


def read_file(root, relative):
    path = root / relative
    for parent in (path,) + tuple(path.parents):
        if parent == root:
            break
        if parent.is_symlink():
            raise ExportError('symlink in allowlisted input: ' + relative)
    if not path.is_file() or path.stat().st_size > 2 * 1024 * 1024:
        raise ExportError('missing/oversized allowlisted input: ' + relative)
    raw = path.read_bytes()
    try:
        text = raw.decode('utf-8')
    except UnicodeError:
        raise ExportError('non-text allowlisted input: ' + relative)
    if '\x00' in text:
        raise ExportError('NUL in allowlisted input: ' + relative)
    return raw


def selected_paths(root):
    selected = set(ROOT_FILES + EXAMPLES)
    for directory in PACKAGES:
        base = root / directory
        if base.is_symlink() or not base.is_dir():
            raise ExportError('missing/symlink package: ' + directory)
        selected.update(str(p.relative_to(root)) for p in base.glob('*.go'))
        data = base / 'testdata'
        if data.is_symlink():
            raise ExportError('symlink testdata directory: ' + directory)
        if data.exists():
            selected.update(str(p.relative_to(root)) for p in data.rglob('*')
                            if p.suffix == '.json' or p.name == 'README.md')
    for name in ('LICENSE', 'NOTICE'):
        if (root / name).exists() or (root / name).is_symlink():
            selected.add(name)
    return sorted(selected)


def read_privacy_policy(path):
    if path is None:
        return (), None
    try:
        value, raw = release.read_json(path)
    except (OSError, release.ReleaseError):
        raise ExportError('missing or invalid privacy policy')
    if (set(value) != {'format', 'deny_literals'} or type(value['format']) is not int or
            value['format'] != 1 or not isinstance(value['deny_literals'], list) or
            not 1 <= len(value['deny_literals']) <= 128 or
            any(not isinstance(x, str) or not x.strip() or len(x) > 256
                for x in value['deny_literals'])):
        raise ExportError('invalid privacy policy schema')
    return tuple(x.casefold() for x in value['deny_literals']), raw


def scan(relative, text, old_module, deny_literals=()):
    folded = text.casefold()
    if ((old_module and old_module.casefold() in folded) or
            any(value.casefold() in folded for value in deny_literals) or PRIVATE_PATH.search(text)):
        raise ExportError('private reference requires review: ' + relative)
    if CREDENTIAL.search(text):
        raise ExportError('credential/private-key pattern requires review: ' + relative)
    if any(value not in SYNTHETIC_IDS for value in RESOURCE.findall(text)):
        raise ExportError('resource identifier requires review: ' + relative)
    for value in IPV4.findall(text):
        try:
            address = ipaddress.ip_address(value)
        except ValueError:
            continue
        if any(address in network for network in PRIVATE_NETWORKS):
            raise ExportError('private address requires review: ' + relative)
    if ACCOUNT.search(text):
        raise ExportError('account identifier requires review: ' + relative)
    fixture = relative.endswith('_test.go') or Path(relative).name.startswith('test_')
    for value in SECRET_LITERAL.findall(text):
        if fixture and value in SYNTHETIC_SECRETS:
            continue
        if relative == 'internal/nomadclient/config.go' and value == 'NOMAD_TOKEN':
            continue  # Environment variable name in the configuration mapping.
        raise ExportError('credential literal requires review: ' + relative)


def prepare(source, output, module=DEFAULT_MODULE, gofmt=None, mode="review",
            version=None, approval=None, privacy_policy=None):
    if mode not in release.STATUSES:
        raise ExportError('invalid preparation mode')
    if mode != 'review' and (not isinstance(version, str) or not release.VERSION.fullmatch(version)):
        raise ExportError('candidate/release mode requires an explicit semantic version')
    if mode != 'review' and module.startswith('example.invalid/'):
        raise ExportError('candidate/release mode requires a public module')
    if mode != 'release' and approval:
        raise ExportError('approval records are only accepted in release mode')
    source = Path(source).resolve()
    output = Path(output).absolute()
    if not MODULE.fullmatch(module) or '..' in module.split('/') or module.endswith('/'):
        raise ExportError('invalid destination module')
    deny_literals, policy_raw = read_privacy_policy(privacy_policy)
    if module.startswith('localhost/') or any(value in module.casefold() for value in deny_literals):
        raise ExportError('destination must not use a private source host')
    if not output.parent.is_dir() or output.is_symlink() or output.exists():
        raise ExportError('output parent must exist and output must be NEW')
    output = output.parent.resolve() / output.name
    try:
        output.relative_to(source)
    except ValueError:
        pass
    else:
        raise ExportError('output must be outside source')
    formatter = None
    formatter_hash = None
    if gofmt is not None:
        if not Path(gofmt).is_absolute():
            raise ExportError('formatter path must be absolute')
        formatter = Path(gofmt).resolve(strict=True)
        if not formatter.is_file():
            raise ExportError('formatter must be a regular file')
        formatter_hash = hashlib.sha256(formatter.read_bytes()).hexdigest()
    paths = selected_paths(source)
    if privacy_policy is not None and Path(privacy_policy).resolve() in {source / p for p in paths}:
        raise ExportError('privacy policy must be outside exported inputs')
    original = {name: read_file(source, name) for name in paths}
    match = re.search(r'^module\s+(\S+)\s*$', original['go.mod'].decode(), re.M)
    if not match:
        raise ExportError('source go.mod must identify one module')
    old_module = match.group(1)
    if old_module != module and privacy_policy is None:
        raise ExportError('module rewrite requires an explicit privacy policy')
    transformed = {}
    for name, raw in original.items():
        text = raw.decode('utf-8')
        if name == 'go.mod':
            text = text[:match.start(1)] + module + text[match.end(1):]
        elif name.endswith('.go'):
            text = text.replace(old_module + '/', module + '/')
        scan(name, text, old_module if old_module != module else None, deny_literals)
        value = text.encode('utf-8')
        if formatter is not None and name.endswith('.go'):
            try:
                result = subprocess.run([str(formatter)], input=value,
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                        env={'PATH': os.defpath}, timeout=30)
            except (OSError, subprocess.TimeoutExpired):
                raise ExportError('formatter execution failed')
            if result.returncode or not result.stdout or len(result.stdout) > 4 * 1024 * 1024:
                raise ExportError('formatter execution failed')
            value = result.stdout
            try:
                formatted = value.decode('utf-8')
            except UnicodeError:
                raise ExportError('formatter returned non-UTF8 output')
            if '\x00' in formatted:
                raise ExportError('formatter returned invalid output')
            scan(name, formatted, old_module if old_module != module else None, deny_literals)
        transformed[name] = value
    # A dirty working tree is permitted for preparation, but it must not drift
    # while captured. This is not a claim of an approved/frozen Git revision.
    if (selected_paths(source) != paths or
            any(read_file(source, name) != raw for name, raw in original.items()) or
            selected_paths(source) != paths):
        raise ExportError('source changed during capture')
    if formatter is not None and hashlib.sha256(formatter.read_bytes()).hexdigest() != formatter_hash:
        raise ExportError('formatter changed during capture')
    if privacy_policy is not None and read_privacy_policy(privacy_policy)[1] != policy_raw:
        raise ExportError('privacy policy changed during capture')
    hashes = {name: hashlib.sha256(raw).hexdigest()
              for name, raw in sorted(transformed.items())}
    manifest = {'format': 1, 'status': 'REVIEW_ONLY', 'module': module,
                'public_destination_approved': False,
                'license_file_present': 'LICENSE' in transformed,
                'publication_authorized': False,
                'input_snapshot_approved': False,
                'scope': 'allowlisted files only; no Git history or lab evidence',
                'privacy_scan': 'bounded patterns; manual review still required',
                'gofmt_applied': formatter is not None, 'gofmt_sha256': formatter_hash,
                'files': hashes}
    if mode != 'review':
        if any(name not in transformed for name in release.REQUIRED_LEGAL):
            raise ExportError('candidate/release mode requires complete license inputs')
        if b'Mozilla Public License Version 2.0' not in transformed['LICENSE']:
            raise ExportError('candidate/release license must be MPL-2.0')
        manifest.update(status=release.STATUSES['candidate'], release_version=version, license='MPL-2.0')
        if mode == 'release':
            try:
                manifest['release_authorization'] = release.approve(manifest, approval)
            except release.ReleaseError as error:
                raise ExportError(str(error))
            manifest.update(status=release.STATUSES['release'], publication_authorized=True,
                            input_snapshot_approved=True, public_destination_approved=True)
    transformed['SOURCE-MANIFEST.json'] = release.encoded(manifest)
    if mode == 'review':
        transformed['REVIEW-ONLY.md'] = ('''# Review-only source snapshot

This snapshot is for review. Its source license is defined by LICENSE when
present; this preparation tool does not grant publication approval. A reserved example.invalid module is a build-check namespace,
not an invented public repository. A supplied --module is still review-only.

Source approval, archive review, builds, architecture loading and installation
verification remain separate checks. Only allowlisted files are included.
The pattern scan does not prove absence of every kind of sensitive information.
''').encode()
    # Create once, never replace another directory or remove existing contents.
    output.mkdir(mode=0o700)
    for name, raw in sorted(transformed.items()):
        path = output / name
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with path.open('xb') as handle:
            handle.write(raw)
    return {'status': manifest['status'], 'files': len(transformed),
            'module': module, 'publication_authorized': manifest['publication_authorized']}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--module', default=DEFAULT_MODULE)
    parser.add_argument('--gofmt', help='explicit trusted formatter after module rewriting')
    parser.add_argument('--mode', choices=sorted(release.STATUSES), default='review')
    parser.add_argument('--version')
    parser.add_argument('--approval', help='explicit human approval record for this exact candidate')
    parser.add_argument('--privacy-policy', help='external JSON deny rules; required when rewriting the module')
    args = parser.parse_args(argv)
    try:
        result = prepare(args.source, args.output, args.module, args.gofmt, args.mode,
                         args.version, args.approval, args.privacy_policy)
    except (ExportError, OSError) as error:
        # Paths/codes aid review; never echo matched credential/resource values.
        print('source preparation failed: ' + str(error), file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == '__main__':
    sys.exit(main())
