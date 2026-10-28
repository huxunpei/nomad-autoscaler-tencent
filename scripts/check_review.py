#!/usr/bin/env python3
"""Run credential-free development checks on an exact review-source export."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys

sys.dont_write_bytecode = True
import build_review as builder


def now():
    return datetime.now(timezone.utc).isoformat()


def test_results(path):
    counts = {'package_pass': 0, 'test_pass_events': 0, 'skipped': []}
    failed = False
    with path.open() as stream:
        for raw in stream:
            if not raw.strip():
                continue
            try:
                event = json.loads(raw)
            except ValueError:
                raise builder.BuildError('invalid Go JSON event stream')
            if not isinstance(event, dict) or not isinstance(event.get('Action'), str):
                raise builder.BuildError('invalid Go JSON event')
            action = event['Action']
            failed |= action == 'fail'
            if action == 'pass':
                counts['test_pass_events' if event.get('Test') else 'package_pass'] += 1
            elif action == 'skip':
                counts['skipped'].append({'package': event.get('Package'),
                                          'test': event.get('Test')})
    if failed or not counts['package_pass'] or not counts['test_pass_events']:
        raise builder.BuildError('Go tests failed or produced no passing tests')
    return counts


def check_environment():
    env = builder.build_environment()
    # race requires the native Linux C compiler. No runtime-test or cloud
    # credential variables are forwarded from the caller.
    env.update({'CGO_ENABLED': '1', 'PYTHONDONTWRITEBYTECODE': '1'})
    env.pop('GOOS', None)
    env.pop('GOARCH', None)
    return env


def command(argv, source, env, output, name, records):
    record = {'name': name, 'started_at': now(), 'argv': argv}
    log = output / (name + '.log')
    with log.open('xb') as stream:
        child = subprocess.Popen(argv, cwd=str(source), env=env,
                                 stdin=subprocess.DEVNULL, stdout=stream,
                                 stderr=subprocess.STDOUT, start_new_session=True)
        try:
            code = child.wait(timeout=600)
        except subprocess.TimeoutExpired:
            os.killpg(child.pid, signal.SIGTERM)
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait()
            code = child.returncode
            record['timeout'] = True
    record.update({'finished_at': now(), 'exit': code,
                   'log': log.name,
                   'sha256': hashlib.sha256(log.read_bytes()).hexdigest()})
    records.append(record)
    if code or record.get('timeout'):
        raise builder.BuildError('check failed: ' + name)
    return log


def check(source, output, go):
    if not sys.platform.startswith('linux'):
        raise builder.BuildError('run the check suite on a dedicated Linux host')
    source = Path(source).resolve()
    go = str(Path(go).resolve(strict=True))
    gofmt = Path(go).with_name('gofmt')
    if not gofmt.is_file():
        raise builder.BuildError('gofmt must accompany the selected Go toolchain')
    manifest, raw, before = builder.snapshot(source)
    output = builder.new_output(source, output)
    output.mkdir(mode=0o700)
    report = {'format': 1, 'status': 'RUNNING', 'started_at': now(),
              'source_manifest_sha256': hashlib.sha256(raw).hexdigest(),
              'publication_authorized': False, 'installation_verified': False,
              'runtime_acceptance_executed': False, 'commands': []}
    env = check_environment()
    try:
        command([go, 'version'], source, env, output, 'go-version', report['commands'])
        formatted = command([str(gofmt), '-l'] + sorted(
            name for name in manifest['files'] if name.endswith('.go')),
            source, env, output, 'format', report['commands'])
        if formatted.read_text().strip():
            raise builder.BuildError('Go files require formatting; source left unchanged')
        for name in ('test_prepare_source.py', 'test_check_review.py', 'test_build_review.py',
                     'test_agent_smoke.py', 'test_package_release.py'):
            command([sys.executable, '-B', 'scripts/' + name], source, env, output,
                    name[:-3], report['commands'])
        command([go, 'vet', '-mod=readonly', './...'], source, env, output,
                'vet', report['commands'])
        # Keep stderr separate: go test emits JSON only on stdout.
        log = output / 'race.jsonl'
        err = output / 'race.stderr.log'
        record = {'name': 'race', 'started_at': now()}
        with log.open('xb') as stdout, err.open('xb') as stderr:
            child = subprocess.Popen([go, 'test', '-race', '-mod=readonly',
                                      '-count=1', '-json', '-timeout=12m', './...'],
                                     cwd=str(source), env=env, stdin=subprocess.DEVNULL,
                                     stdout=stdout, stderr=stderr, start_new_session=True)
            try:
                code = child.wait(timeout=900)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGTERM)
                try:
                    child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid, signal.SIGKILL)
                    child.wait()
                code = child.returncode
                record['timeout'] = True
        record.update({'finished_at': now(), 'exit': code, 'log': log.name,
                       'stderr': err.name,
                       'sha256': hashlib.sha256(log.read_bytes()).hexdigest()})
        report['commands'].append(record)
        if code or record.get('timeout'):
            raise builder.BuildError('race command failed or timed out')
        report['go_tests'] = test_results(log)
        command([go, 'mod', 'verify'], source, env, output,
                'module-verify', report['commands'])
        report['status'] = 'DEVELOPMENT_CHECKS_PASSED'
    except Exception as exc:
        report['status'] = 'FAILED'
        report['error_type'] = type(exc).__name__
        if isinstance(exc, builder.BuildError):
            report['reason'] = str(exc)
        raise
    finally:
        try:
            _, _, after = builder.snapshot(source)
            report['source_unchanged'] = before == after
        except Exception:
            report['source_unchanged'] = False
        if not report['source_unchanged']:
            report['status'] = 'FAILED'
        report['finished_at'] = now()
        (output / 'CHECK-REPORT.json').write_text(json.dumps(report, indent=2) + '\n')
    if report['status'] != 'DEVELOPMENT_CHECKS_PASSED':
        raise builder.BuildError('source changed during checks')
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--go', required=True)
    args = parser.parse_args()
    try:
        result = check(args.source, args.output, args.go)
    except (builder.BuildError, OSError) as exc:
        # Detailed command logs remain in the new private output directory.
        print('Development checks failed: ' + type(exc).__name__, file=sys.stderr)
        sys.exit(1)
    print(json.dumps({'status': result['status'], 'go_tests': result['go_tests'],
                      'source_unchanged': result['source_unchanged'],
                      'runtime_acceptance_executed': False}))
