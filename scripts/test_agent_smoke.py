import hashlib
import importlib.util
import os
from pathlib import Path
import shlex
import struct
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location('agent_smoke', str(Path(__file__).with_name('agent_smoke.py')))
smoke = importlib.util.module_from_spec(spec)
spec.loader.exec_module(smoke)


class SmokeGuards(unittest.TestCase):
    def test_credentials_are_dropped_and_existing_home_is_preserved(self):
        values = {'PATH': '/usr/bin', 'HOME': '/tmp/existing-home', 'TMPDIR': '/tmp',
                  'TENCENTCLOUD_SECRET_KEY': 'synthetic', 'NOMAD_TOKEN': 'synthetic',
                  'HTTP_PROXY': 'http://proxy.invalid', 'CHECKPOINT_DISABLE': '0'}
        with mock.patch.dict(os.environ, values, clear=True):
            env = smoke.child_environment()
        self.assertEqual('/tmp/existing-home', env['HOME'])
        self.assertEqual('1', env['CHECKPOINT_DISABLE'])
        self.assertNotIn('TENCENTCLOUD_SECRET_KEY', env)
        self.assertNotIn('NOMAD_TOKEN', env)
        self.assertNotIn('HTTP_PROXY', env)

    def test_hash_mismatch_is_rejected_without_echoing_the_input(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'binary'
            path.write_bytes(b'synthetic payload')
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), smoke.checked_hash(path))
            with self.assertRaisesRegex(ValueError, 'binary SHA256 mismatch') as caught:
                smoke.checked_hash(path, '0' * 64)
            self.assertNotIn('synthetic payload', str(caught.exception))

    def test_elf_architecture_detection(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'binary'
            for machine in (62, 183):
                raw = bytearray(20)
                raw[:6] = b'\x7fELF\x02\x01'
                raw[18:20] = struct.pack('<H', machine)
                path.write_bytes(raw)
                self.assertEqual(machine, smoke.elf_machine(path))
            path.write_bytes(b'not an ELF')
            self.assertIsNone(smoke.elf_machine(path))

    def test_launcher_preserves_literal_paths_and_arguments(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            emulator = root / 'emulator with space'
            binary = root / 'binary;literal'
            launcher = root / 'launcher'
            smoke.emulator_launcher(launcher, emulator, binary)
            argv = shlex.split(launcher.read_text().splitlines()[1])
            self.assertEqual(['exec', str(emulator), '-0', '$0', str(binary), '$@'], argv)
            self.assertEqual(0o700, launcher.stat().st_mode & 0o777)

    def test_existing_report_is_not_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            agent, plugin, report = (root / n for n in ('agent', 'plugin', 'report.json'))
            agent.write_text('not executed')
            plugin.write_text('not executed')
            report.write_text('keep original')
            result = subprocess.run([sys.executable, '-B', str(Path(smoke.__file__)),
                                     '--agent', str(agent), '--plugin', str(plugin), '--report', str(report)],
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self.assertEqual(2, result.returncode)
            self.assertIn(b'--report must be a new file', result.stderr)
            self.assertEqual('keep original', report.read_text())


if __name__ == '__main__':
    unittest.main()
