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

"""Checks that SECURITY.md and the compatibility table in the docs cover the releases.

The release line to check depends on what is checked out:
  --branch main          the latest release line, from the X.Y.Z tags of the remote
  --branch release-X.Y   the X.Y line
  --tag X.Y.Z            the X.Y line
An older line is only checked while SECURITY.md lists it as supported.

SECURITY.md must list the release line as supported, and "< oldest supported version" as unsupported.

The compatibility table must have a row for the release line and for each supported line. The Flink
SQL, Flink Runtime, Postgres and Iceberg versions in a row must match the build files of the line:
the checked-out ones on its release branch or tag, otherwise the ones of its latest tag, or the
checked-out ones while it has no tag yet. pgvector and Kafka are not checked, no build file defines them.

Run from the repository root.
"""

import argparse
import os
import re
import subprocess
import sys

SECURITY = "SECURITY.md"
COMPATIBILITY = "documentation/docs/compatibility.md"

errors = []


def error(path, message):
    errors.append((path, message))


def git(*args):
    return subprocess.run(["git", *args], check=True, capture_output=True, text=True).stdout


def to_line(match):
    """The release line (X, Y) of a regex match with X and Y as its first two groups."""
    return int(match[1]), int(match[2])


def name(line):
    return f"{line[0]}.{line[1]}"


def latest_tags(remote):
    """The latest X.Y.Z tag of each release line on the remote, e.g. {(0, 12): "0.12.4"}."""
    versions = []
    for ref in git("ls-remote", "--tags", "--refs", remote).split():
        match = re.fullmatch(r"refs/tags/(\d+)\.(\d+)\.(\d+)", ref)
        if match:
            versions.append(tuple(int(number) for number in match.groups()))
    return {(major, minor): f"{major}.{minor}.{patch}" for major, minor, patch in sorted(versions)}


def fetch_tag(remote, tag):
    """Fetches a tag unless it is already there, e.g. in a shallow CI checkout."""
    exists = subprocess.run(["git", "rev-parse", "--verify", "--quiet", f"refs/tags/{tag}^{{commit}}"], capture_output=True)
    if exists.returncode != 0:
        git("fetch", "--quiet", "--no-tags", "--depth=1", remote, f"+refs/tags/{tag}:refs/tags/{tag}")


def read(path, tag=None):
    """Reads a file from the checked-out files, or from a tag."""
    if tag is None:
        with open(path, encoding="utf-8") as file:
            return file.read()
    return git("show", f"refs/tags/{tag}:{path}")


def markdown_table(text):
    """The rows of the first Markdown table in the text, as dicts keyed by the header cells."""
    table_lines = []
    for text_line in text.splitlines():
        if text_line.lstrip().startswith("|"):
            table_lines.append(text_line)
        elif table_lines:
            break
    header, _separator, *rows = [[cell.strip() for cell in line.strip().strip("|").split("|")] for line in table_lines]
    return [dict(zip(header, row)) for row in rows]


def build_versions(tag, source):
    """The compatibility table cells that the build files of a tag, or the checked-out ones, define."""

    def find(pattern, path):
        match = re.search(pattern, read(path, tag), re.MULTILINE)
        if not match:
            sys.exit(f"Cannot find {pattern} in {path} of {source}")
        return match[1]

    flink = find(r"<flink.version>(.+?)</flink.version>", "pom.xml")
    postgres = find(r"^ARG POSTGRES_VERSION=(\S+)", "sqrl-cli/Dockerfile.base")
    iceberg = find(r"<iceberg.version>(.+?)</iceberg.version>", "pom.xml")
    return {
        "Flink SQL": ".".join(flink.split(".")[:2]) + ".x",
        "Flink Runtime": f"{flink} tested",
        "Postgres": f"{postgres} tested",
        "Iceberg": f"{iceberg} tested",
    }


def check_security(line, supported_versions, unsupported_versions, supported):
    for version in supported_versions:
        if not re.fullmatch(r"\d+\.\d+\.x", version):
            error(SECURITY, f"must list supported versions as X.Y.x, found '{version}'")
    if line not in supported:
        error(SECURITY, f"must list {name(line)}.x as supported")
    if supported and f"< {name(min(supported))}" not in unsupported_versions:
        error(SECURITY, f"must list < {name(min(supported))} as unsupported, {name(min(supported))}.x is the oldest supported version")


def check_row(table, line, tag, source):
    """Checks the compatibility table row of a release line against the build files of a tag, or the checked-out ones."""
    expected = build_versions(tag, source)
    row = next((row for row in table if row["DataSQRL"] == f"{name(line)}.x"), None)
    if row is None:
        cells = ", ".join(f"{column} {version}" for column, version in expected.items())
        error(COMPATIBILITY, f"has no row for {name(line)}.x; add one for {source} with {cells}")
        return
    for column, version in expected.items():
        # A cell is a version, or a minimum and the tested version: "2.3+ <br /> 2.3.0 tested"
        cell = row.get(column, "")
        if cell.split("<br />")[-1].strip() != version:
            error(COMPATIBILITY, f"{name(line)}.x row has {column} '{cell}', but {source} uses {version}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    checked_out = parser.add_mutually_exclusive_group(required=True)
    checked_out.add_argument("--branch", help="the checked-out branch: main or release-X.Y")
    checked_out.add_argument("--tag", help="the checked-out tag")
    parser.add_argument("--remote", default="origin", help="the remote to read the release tags from")
    args = parser.parse_args()

    tags = latest_tags(args.remote)
    if not tags:
        sys.exit(f"No X.Y.Z release tags found on {args.remote}")
    latest = max(tags)

    # The release line to check, and the tag or branch whose build files are checked out for it
    if args.tag:
        match = re.fullmatch(r"(\d+)\.(\d+)\.\d+", args.tag)
        if not match:
            print(f"{args.tag} is not a release tag, nothing to check")
            return 0
        line, local_source = to_line(match), args.tag
    elif match := re.fullmatch(r"release-(\d+)\.(\d+)", args.branch):
        line, local_source = to_line(match), args.branch
    else:
        line, local_source = latest, None

    security = markdown_table(read(SECURITY)) if os.path.exists(SECURITY) else []
    supported_versions = [row["Version"] for row in security if ":white_check_mark:" in row["Supported"]]
    unsupported_versions = [row["Version"] for row in security if ":x:" in row["Supported"]]
    supported = []
    for version in supported_versions:
        if match := re.fullmatch(r"(\d+)\.(\d+)\.x", version):
            supported.append(to_line(match))

    if line < latest and line not in supported:
        print(f"{name(line)} is neither the latest release line ({name(latest)}) nor supported, nothing to check")
        return 0

    print(f"Checking {SECURITY} and {COMPATIBILITY} for the {name(line)} release line")
    check_security(line, supported_versions, unsupported_versions, supported)
    table = markdown_table(read(COMPATIBILITY))
    for row_line in sorted({line, *supported}, reverse=True):
        if row_line == line and local_source:
            check_row(table, row_line, None, local_source)
        elif row_line in tags:
            fetch_tag(args.remote, tags[row_line])
            check_row(table, row_line, tags[row_line], tags[row_line])
        else:
            check_row(table, row_line, None, f"the upcoming {name(row_line)} release")

    for path, message in errors:
        print(f"::error file={path}::{message}" if os.environ.get("GITHUB_ACTIONS") else f"ERROR {path}: {message}")
    if errors:
        print(f"{len(errors)} problem(s) found, see RELEASE.md")
        return 1
    print("SECURITY.md and the compatibility table are up to date")
    return 0


if __name__ == "__main__":
    sys.exit(main())
