#!/usr/bin/env python3
#
# Copyright © 2021 DataSQRL (contact@datasqrl.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Runs the documented shell commands verbatim, so the docs cannot drift from the product.

Opt a fenced code block in by adding a `docs-test` attribute to its info string:

    ```bash docs-test=no-key
    docker run --rm -v $PWD:/workspace datasqrl/cmd init api messenger
    ```

Blocks with the same suite name run in document order, in a fresh temporary directory
per suite. The working directory carries over between the blocks of a suite, so a
documented `cd` applies to the following blocks. Commands run in a pseudo terminal, so
documented `docker run -it` commands work in CI as well.

Long-running commands (e.g. `run`) additionally declare a readiness URL:

    ```bash docs-test=no-key docs-test-background=http://localhost:8888/v1/graphiql/
    docker run -it --rm -p 8888:8888 -v $PWD:/workspace datasqrl/cmd run package.json
    ```

The command is started in the background, the step succeeds once the URL responds,
and every container started by the step is stopped afterwards.

To test a release candidate, `--image datasqrl/cmd=ghcr.io/datasqrl/cmd:pr-123` replaces
the documented image reference, leaving the rest of the commands untouched.
"""

from __future__ import annotations

import argparse
import os
import re
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path

FENCE_PATTERN = re.compile(r"^(?P<indent>\s*)(?P<fence>`{3,}|~{3,})(?P<info>.*)$")
ATTR_PATTERN = re.compile(r"""([\w-]+)=("[^"]*"|'[^']*'|\S+)""")


@dataclass
class Block:
    file: Path
    line: int
    suite: str
    code: str
    background_url: str | None


def parse_blocks(path: Path) -> list[Block]:
    blocks = []
    lines = path.read_text(encoding="utf-8").splitlines()
    i = 0
    while i < len(lines):
        match = FENCE_PATTERN.match(lines[i])
        if not match:
            i += 1
            continue

        fence, info = match.group("fence"), match.group("info").strip()
        start = i
        body = []
        i += 1
        while i < len(lines) and not lines[i].strip().startswith(fence):
            body.append(lines[i])
            i += 1
        i += 1  # skip closing fence

        lang = info.split(maxsplit=1)[0] if info else ""
        attrs = {k: v.strip("\"'") for k, v in ATTR_PATTERN.findall(info)}
        if lang in ("bash", "sh", "shell") and "docs-test" in attrs:
            blocks.append(
                Block(
                    file=path,
                    line=start + 1,
                    suite=attrs["docs-test"],
                    code="\n".join(body),
                    background_url=attrs.get("docs-test-background"),
                )
            )
    return blocks


def pty_command(script: Path) -> list[str]:
    """Wraps the script so it runs in a pseudo terminal (required by `docker run -it`)."""
    if sys.platform == "darwin":
        return ["script", "-q", "/dev/null", "bash", str(script)]
    return ["script", "-qefc", f"bash {shlex.quote(str(script))}", "/dev/null"]


def running_containers() -> set[str]:
    out = subprocess.run(["docker", "ps", "-q"], capture_output=True, text=True, check=False)
    return set(out.stdout.split())


def wait_for_url(url: str, proc: subprocess.Popen, timeout: int) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            return False
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                if resp.status < 400:
                    return True
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            pass
        time.sleep(3)
    return False


def replace_images(code: str, images: dict[str, str]) -> str:
    for documented, actual in images.items():
        # Only replace the untagged image reference, not e.g. datasqrl/cmd-foo or a tagged one
        code = re.sub(rf"(?<![\w./-]){re.escape(documented)}(?![\w./:-])", actual, code)
    return code


def run_block(
    block: Block, cwd: Path, state_dir: Path, timeout: int, images: dict[str, str]
) -> Path:
    cwd_file = state_dir / "cwd"
    script = state_dir / "step.sh"
    script.write_text(
        "set -euo pipefail\n"
        f"cd {shlex.quote(str(cwd))}\n"
        f"{replace_images(block.code, images)}\n"
        f"pwd > {shlex.quote(str(cwd_file))}\n",
        encoding="utf-8",
    )

    print(f"::: {block.file}:{block.line} [{block.suite}]", flush=True)
    print("\n".join(f"$ {line}" for line in block.code.splitlines()), flush=True)

    if block.background_url is None:
        result = subprocess.run(pty_command(script), stdin=subprocess.DEVNULL, timeout=timeout)
        if result.returncode != 0:
            raise RuntimeError(f"exited with code {result.returncode}")
        return Path(cwd_file.read_text(encoding="utf-8").strip())

    before = running_containers()
    proc = subprocess.Popen(pty_command(script), stdin=subprocess.DEVNULL, start_new_session=True)
    try:
        if not wait_for_url(block.background_url, proc, timeout):
            if proc.poll() is not None:
                raise RuntimeError(
                    f"exited with code {proc.returncode} before {block.background_url} was available"
                )
            raise RuntimeError(f"{block.background_url} did not become available")
        print(f"::: {block.background_url} is available, stopping", flush=True)
    finally:
        started = running_containers() - before
        if started:
            subprocess.run(["docker", "stop", *started], capture_output=True, check=False)
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGTERM)
            try:
                proc.wait(timeout=60)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
    return cwd  # background commands cannot change the working directory


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("files", nargs="+", type=Path, help="Markdown files to scan")
    parser.add_argument("--suite", action="append", help="Only run the given suite(s)")
    parser.add_argument("--list", action="store_true", help="List the blocks without running")
    parser.add_argument("--timeout", type=int, default=900, help="Timeout per block (seconds)")
    parser.add_argument(
        "--image",
        action="append",
        default=[],
        metavar="DOCUMENTED=ACTUAL",
        help="Run the commands with another image, e.g. datasqrl/cmd=datasqrl/cmd:dev",
    )
    args = parser.parse_args()
    images = dict(image.split("=", 1) for image in args.image)

    suites: dict[str, list[Block]] = {}
    for file in args.files:
        for block in parse_blocks(file):
            if not args.suite or block.suite in args.suite:
                suites.setdefault(block.suite, []).append(block)

    if not suites:
        print("No docs-test blocks found", file=sys.stderr)
        return 1

    if args.list:
        for suite, blocks in suites.items():
            for block in blocks:
                print(f"{suite}\t{block.file}:{block.line}")
        return 0

    failures = []
    for suite, blocks in suites.items():
        print(f"=== Suite '{suite}' ({len(blocks)} blocks)", flush=True)
        tmp = Path(tempfile.mkdtemp(prefix=f"docs-test-{suite}-"))
        try:
            workspace = tmp / "workspace"
            state_dir = tmp / "state"
            workspace.mkdir()
            state_dir.mkdir()
            cwd = workspace
            for block in blocks:
                try:
                    cwd = run_block(block, cwd, state_dir, args.timeout, images)
                except (RuntimeError, subprocess.TimeoutExpired) as e:
                    failures.append(f"{block.file}:{block.line} [{suite}]: {e}")
                    print(f"!!! FAILED: {e}", flush=True)
                    break  # later blocks of the suite depend on this one
        finally:
            # Containers may leave root-owned files behind, which must not fail the run
            shutil.rmtree(tmp, ignore_errors=True)

    if failures:
        print("\nFailing documented commands:", file=sys.stderr)
        for failure in failures:
            print(f"  {failure}", file=sys.stderr)
        return 1

    print(f"\nAll {sum(len(b) for b in suites.values())} documented commands passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
