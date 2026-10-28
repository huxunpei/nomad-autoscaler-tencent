"""Release metadata checks. An approval record records, never grants, human consent."""
import copy
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re

STATUSES = {'review': 'REVIEW_ONLY', 'candidate': 'RELEASE_CANDIDATE',
            'release': 'RELEASE_APPROVED'}
VERSION = re.compile(r'^[0-9]+\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9.-]+)?$')
HASH = re.compile(r'^[0-9a-f]{64}$')
REQUIRED_LEGAL = ('LICENSE', 'NOTICE', 'THIRD_PARTY_NOTICES.md',
                  'docs/dependency-licenses.json')


class ReleaseError(Exception):
    pass


def encoded(value):
    return (json.dumps(value, indent=2, sort_keys=True) + '\n').encode('utf-8')


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def unique_object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ReleaseError('duplicate JSON field')
        value[key] = item
    return value


def read_json(path):
    path = Path(path)
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 2 * 1024 * 1024:
        raise ReleaseError('invalid JSON input file')
    raw = path.read_bytes()
    try:
        value = json.loads(raw.decode('utf-8'), object_pairs_hook=unique_object)
    except (UnicodeError, ValueError):
        raise ReleaseError('invalid JSON input')
    if not isinstance(value, dict):
        raise ReleaseError('JSON object required')
    return value, raw


def candidate_manifest(manifest):
    result = copy.deepcopy(manifest)
    result.update(status=STATUSES['candidate'], publication_authorized=False,
                  input_snapshot_approved=False, public_destination_approved=False)
    result.pop('release_authorization', None)
    return result


def approve(manifest, approval_path):
    if not approval_path:
        raise ReleaseError('release mode requires an explicit approval record')
    candidate = candidate_manifest(manifest)
    value, raw = read_json(approval_path)
    required = {'format', 'decision', 'module', 'version', 'candidate_manifest_sha256',
                'approved_by', 'approved_at', 'source_publication_confirmed'}
    if set(value) != required or type(value['format']) is not int or value['format'] != 1:
        raise ReleaseError('invalid approval record schema')
    if (value['decision'] != 'approve-release' or value['source_publication_confirmed'] is not True or
            value['module'] != candidate.get('module') or value['version'] != candidate.get('release_version') or
            value['candidate_manifest_sha256'] != sha(encoded(candidate))):
        raise ReleaseError('approval does not match this candidate or publication scope')
    if not isinstance(value['approved_by'], str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,39}', value['approved_by']):
        raise ReleaseError('approval must identify the approving GitHub account')
    try:
        date = value['approved_at']
        if not isinstance(date, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z', date):
            raise ValueError()
        datetime.strptime(date, '%Y-%m-%dT%H:%M:%SZ')
    except ValueError:
        raise ReleaseError('approval must record its actual UTC time')
    return {'candidate_manifest_sha256': value['candidate_manifest_sha256'],
            'approval_record_sha256': sha(raw), 'approved_by': value['approved_by'],
            'approved_at': value['approved_at']}


def validate_manifest(manifest):
    status = manifest.get('status')
    if status not in STATUSES.values():
        raise ReleaseError('unsupported source status')
    if status == STATUSES['review']:
        if manifest.get('publication_authorized') is not False:
            raise ReleaseError('review source cannot authorize publication')
        return 'review'
    if (not isinstance(manifest.get('release_version'), str) or
            not VERSION.fullmatch(manifest['release_version']) or
            not all(x in manifest.get('files', {}) for x in REQUIRED_LEGAL) or
            manifest.get('license') != 'MPL-2.0'):
        raise ReleaseError('release source needs a version and complete license inputs')
    if status == STATUSES['candidate']:
        if manifest.get('publication_authorized') is not False or 'release_authorization' in manifest:
            raise ReleaseError('candidate must not claim release approval')
        return 'candidate'
    auth = manifest.get('release_authorization')
    if (manifest.get('publication_authorized') is not True or not isinstance(auth, dict) or
            set(auth) != {'candidate_manifest_sha256', 'approval_record_sha256', 'approved_by', 'approved_at'} or
            auth.get('candidate_manifest_sha256') != sha(encoded(candidate_manifest(manifest))) or
            not isinstance(auth.get('approval_record_sha256'), str) or not HASH.fullmatch(auth['approval_record_sha256'])):
        raise ReleaseError('release authorization is absent or stale')
    return 'release'


def check_mode(manifest, mode, approval_path=None):
    if validate_manifest(manifest) != mode:
        raise ReleaseError('explicit build mode does not match the source manifest')
    if mode == 'release':
        if approve(manifest, approval_path) != manifest['release_authorization']:
            raise ReleaseError('build approval differs from source approval')
    elif approval_path:
        raise ReleaseError('approval records are only accepted in release mode')
