#!/usr/bin/env python3
"""Model-check a TLA+ module with every TLC configuration next to it.

Each configuration <Module>*.cfg states its expected outcome on its first
line, as a TLC comment:

    \\* expect: pass
    \\* expect: violation <InvariantName>

A "pass" configuration must finish without an error. A "violation"
configuration must report that invariant as violated: it shows that the
check would catch the defect the variant introduces, so that a passing
model cannot pass vacuously.

The pinned tla2tools.jar is downloaded once and verified against its
SHA-256 digest. Only the Python standard library and a Java runtime
(11 or later) are needed.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import pathlib
import re
import subprocess
import sys
import tempfile
import time
import urllib.request

RELEASE_URL = "https://github.com/tlaplus/tlaplus/releases/download/v{version}/tla2tools.jar"

PASS_MESSAGE = "Model checking completed. No error has been found."
EXPECT_RE = re.compile(r"^\\\*\s*expect:\s*(pass|violation\s+(\w+))\s*$")
STATES_RE = re.compile(r"(\d[\d,]*) states generated, (\d[\d,]*) distinct states found")


class Expectation:
    def __init__(self, violation: str | None) -> None:
        self.violation = violation

    def __str__(self) -> str:
        return "pass" if self.violation is None else f"violation {self.violation}"


def parse_expectation(text: str) -> Expectation:
    """Return the expectation stated on the first line of a configuration."""
    first = text.splitlines()[0] if text else ""
    match = EXPECT_RE.match(first.strip())
    if not match:
        raise ValueError("first line must be '\\* expect: pass' or '\\* expect: violation <Name>'")
    return Expectation(match.group(2))


def outcome_matches(expectation: Expectation, returncode: int, output: str) -> bool:
    """Report whether a TLC run ended as the configuration expects."""
    if expectation.violation is None:
        return returncode == 0 and PASS_MESSAGE in output
    pattern = rf"Invariant {re.escape(expectation.violation)} is violated"
    return returncode != 0 and re.search(pattern, output) is not None and PASS_MESSAGE not in output


def sha256_of(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def ensure_jar(jar: pathlib.Path, version: str, sha256: str) -> None:
    if jar.exists() and sha256_of(jar) == sha256:
        return
    jar.parent.mkdir(parents=True, exist_ok=True)
    url = RELEASE_URL.format(version=version)
    print(f"downloading {url}", flush=True)
    fd, tmp = tempfile.mkstemp(dir=jar.parent, suffix=".part")
    try:
        with os.fdopen(fd, "wb") as out, urllib.request.urlopen(url, timeout=120) as resp:
            while chunk := resp.read(1 << 20):
                out.write(chunk)
        got = sha256_of(pathlib.Path(tmp))
        if got != sha256:
            raise SystemExit(f"tla2tools.jar {version}: SHA-256 {got}, want {sha256}")
        os.replace(tmp, jar)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def run_config(java: str, jar: pathlib.Path, module: pathlib.Path, cfg: pathlib.Path,
               workers: str) -> tuple[int, str, float]:
    with tempfile.TemporaryDirectory(prefix="tlc-") as metadir:
        cmd = [java, "-XX:+UseParallelGC", "-cp", str(jar), "tlc2.TLC",
               "-workers", workers, "-deadlock", "-cleanup", "-metadir", metadir,
               "-config", cfg.name, module.name]
        start = time.monotonic()
        proc = subprocess.run(cmd, cwd=module.parent, capture_output=True, text=True)
        return proc.returncode, proc.stdout + proc.stderr, time.monotonic() - start


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("module", type=pathlib.Path, help="the .tla file to check")
    parser.add_argument("--jar", type=pathlib.Path, required=True)
    parser.add_argument("--version", required=True, help="tla2tools release, e.g. 1.7.4")
    parser.add_argument("--sha256", required=True, help="SHA-256 of that tla2tools.jar")
    parser.add_argument("--java", default=os.environ.get("JAVA", "java"))
    parser.add_argument("--workers", default="auto")
    parser.add_argument("--only", help="check only this configuration file name")
    args = parser.parse_args(argv)

    module = args.module.resolve()
    configs = sorted(module.parent.glob(module.stem + "*.cfg"))
    if args.only:
        configs = [c for c in configs if c.name == args.only]
    if not configs:
        print(f"no configuration matches {module.stem}*.cfg", file=sys.stderr)
        return 1
    ensure_jar(args.jar.resolve(), args.version, args.sha256)

    failed = []
    for cfg in configs:
        expectation = parse_expectation(cfg.read_text())
        code, output, seconds = run_config(args.java, args.jar.resolve(), module, cfg, args.workers)
        states = STATES_RE.findall(output)
        found = f"{states[-1][1]} distinct states" if states else "no state count"
        ok = outcome_matches(expectation, code, output)
        print(f"{'ok  ' if ok else 'FAIL'} {cfg.name}: expected {expectation}, {found}, {seconds:.1f}s",
              flush=True)
        if not ok:
            failed.append(cfg.name)
            print("\n".join(output.splitlines()[-80:]), file=sys.stderr)
    if failed:
        print(f"unexpected TLC outcome: {', '.join(failed)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
