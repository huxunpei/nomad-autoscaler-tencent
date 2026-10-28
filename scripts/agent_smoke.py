#!/usr/bin/env python3
"""Load this simulation plugin in a standard Autoscaler; no cloud/Nomad writes."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shlex
import shutil
import signal
import socket
import struct
import subprocess
import tempfile
import time


def quoted(value):
    return json.dumps(str(value))


def child_environment():
    env = {k: os.environ[k] for k in ("PATH", "TMPDIR", "SYSTEMROOT", "HOME") if k in os.environ}
    env["CHECKPOINT_DISABLE"] = "1"
    return env


def checked_hash(path, expected=None):
    actual = hashlib.sha256(path.read_bytes()).hexdigest()
    if expected is not None and actual != expected:
        raise ValueError("binary SHA256 mismatch: " + path.name)
    return actual


def elf_machine(path):
    with path.open("rb") as handle:
        header = handle.read(20)
    if len(header) < 20 or header[:4] != b"\x7fELF" or header[5] != 1:
        return None
    return struct.unpack("<H", header[18:20])[0]


def emulator_launcher(path, emulator, binary):
    # Preserve a native executable launcher in argv[0] for any Agent re-exec.
    # The Agent/plugin payload is still the exact separately hashed ARM64 ELF.
    command = "exec " + shlex.quote(str(emulator)) + ' -0 "$0" ' + shlex.quote(str(binary)) + ' "$@"\n'
    path.write_text("#!/bin/sh\n" + command)
    path.chmod(0o700)


def events(path):
    found = []
    if path.exists():
        for line in path.read_text(errors="replace").splitlines():
            try:
                found.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return found


def run_agent(agent, root, state, target, dry_run, name):
    run = root / name
    run.mkdir()
    policies = run / "policies"
    policies.mkdir()
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    (policies / "smoke.hcl").write_text('''scaling "simulation-smoke" {
  enabled = true
  min = 1
  max = 5
  type = "cluster"
  policy {
    cooldown = "1s"
    evaluation_interval = "1s"
    check "fixed" {
      strategy "fixed-value" { value = "%d" }
    }
    target "tencent-as" {
      tencent_group_id = "asg-sim"
      dry-run = "%s"
    }
  }
}
''' % (target, str(dry_run).lower()))
    config = run / "agent.hcl"
    config.write_text('''log_level = "DEBUG"
log_json = true
plugin_dir = %s
http {
  bind_address = "127.0.0.1"
  bind_port = %d
}
nomad { address = "http://127.0.0.1:1" }
policy {
  dir = %s
  source "nomad" { enabled = false }
  source "file" { enabled = true }
}
strategy "fixed-value" { driver = "fixed-value" }
target "tencent-as" {
  driver = "tencent-as"
  config = {
    tencent_backend = "simulation"
    tencent_region = "ap-guangzhou"
    tencent_simulation_group_id = "asg-sim"
    tencent_state_dir = %s
  }
}
''' % (quoted(root / "plugins"), port, quoted(policies), quoted(state)))
    # Do not inherit cloud, Nomad or proxy credentials into the simulated test.
    env = child_environment()
    log = run / "agent.log"
    with log.open("w") as out:
        proc = subprocess.Popen([str(agent), "agent", "-config=" + str(config)], stdout=out, stderr=subprocess.STDOUT, env=env, start_new_session=True)
        try:
            deadline = time.monotonic() + 30
            wanted = "dry-run skipped simulation changes" if dry_run else "simulation scale complete"
            while time.monotonic() < deadline:
                if proc.poll() is not None:
                    raise RuntimeError(f"Agent exited before {wanted}: {proc.returncode}; evidence: {log}")
                if any(item.get("@message") == wanted for item in events(log)):
                    break
                time.sleep(0.1)
            else:
                raise RuntimeError(f"Agent did not reach {wanted}; evidence: {log}")
        finally:
            if proc.poll() is None:
                proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait()
    if proc.returncode != 0:
        raise RuntimeError(f"Agent shutdown failed: {proc.returncode}; evidence: {log}")
    data = json.loads((state / "simulation.json").read_text())
    expected = 1 if dry_run else target
    assert len(data["members"]) == expected, data
    assert data["desired"] == expected, data
    assert not data.get("pending"), data
    assert all(m["eligible"] and not m.get("drained_by") for m in data["members"].values()), data
    return {"scenario": name, "actual": len(data["members"]), "desired": data["desired"], "agent_exit": proc.returncode, "agent_pid": proc.pid, "log": str(log)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--agent", required=True, type=Path)
    parser.add_argument("--plugin", required=True, type=Path)
    parser.add_argument("--emulator", type=Path, help="Linux AArch64 user emulator; requires ARM64 Agent and plugin")
    parser.add_argument("--agent-sha256")
    parser.add_argument("--plugin-sha256")
    parser.add_argument("--emulator-sha256")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    agent, plugin = args.agent.resolve(), args.plugin.resolve()
    if not agent.is_file() or not plugin.is_file():
        parser.error("--agent and --plugin must be existing executable files")
    emulator = args.emulator.resolve() if args.emulator else None
    if emulator is not None and (not emulator.is_file() or elf_machine(agent) != 183 or elf_machine(plugin) != 183):
        parser.error("--emulator needs an existing emulator and ARM64 ELF Agent/plugin")
    if args.emulator_sha256 and emulator is None:
        parser.error("--emulator-sha256 requires --emulator")
    if args.report and (args.report.exists() or args.report.is_symlink()):
        parser.error("--report must be a new file")
    agent_sha = checked_hash(agent, args.agent_sha256)
    plugin_sha = checked_hash(plugin, args.plugin_sha256)
    emulator_sha = checked_hash(emulator, args.emulator_sha256) if emulator else None
    root = Path(tempfile.mkdtemp(prefix="nomad-tencent-smoke-"))
    (root / "plugins").mkdir()
    launcher = agent
    if emulator:
        launcher = root / "agent-launcher"
        emulator_launcher(launcher, emulator, agent)
        emulator_launcher(root / "plugins" / "tencent-as", emulator, plugin)
    else:
        shutil.copy2(plugin, root / "plugins" / "tencent-as")
    results = [run_agent(launcher, root, root / "dry-state", 5, True, "dry-run")]
    state = root / "scale-state"
    results.append(run_agent(launcher, root, state, 5, False, "one-to-five"))
    results.append(run_agent(launcher, root, state, 1, False, "five-to-one-after-restart"))
    assert checked_hash(agent) == agent_sha and checked_hash(plugin) == plugin_sha
    if emulator:
        assert checked_hash(emulator) == emulator_sha
    report = {
        "backend": "simulation",
        "cloud_api_calls": 0,
        "nomad_api_calls": 0,
        "agent_version": subprocess.check_output([str(launcher), "version"], env=child_environment(), universal_newlines=True).strip(),
        "agent_sha256": agent_sha,
        "plugin_sha256": plugin_sha,
        "execution_mode": "aarch64-user-emulation" if emulator else "native",
        "host_architecture": platform.machine(),
        "agent_elf_machine": elf_machine(agent),
        "plugin_elf_machine": elf_machine(plugin),
        "emulator_sha256": emulator_sha,
        "emulator_version": subprocess.check_output([str(emulator), "--version"], env=child_environment(), universal_newlines=True).strip() if emulator else None,
        "results": results,
        "evidence_directory": str(root),
        "limits": "Synthetic nodes and deletion outcomes; P1/P2/P3 and real Nomad drain remain unverified.",
    }
    rendered = json.dumps(report, indent=2) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(rendered)
    print(rendered, end="")


if __name__ == "__main__":
    main()
