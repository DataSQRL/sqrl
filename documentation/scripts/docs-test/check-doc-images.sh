#!/usr/bin/env bash
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

# Verifies that every DataSQRL Docker image referenced in the given files (default: README
# and current docs) can be pulled anonymously, the way a first-time user would.
#
# Usage: check-doc-images.sh [file-or-dir...]

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
if [ "$#" -eq 0 ]; then
  set -- "$ROOT/README.md" "$ROOT/documentation/docs" "$ROOT/documentation/src/pages" "$ROOT/agent/README.md"
fi

# Files that mention DataSQRL images, without the root files of the stdlib-docs submodule
# (README, RELEASE, etc. of the Flink SQL Runner repository), which the docs site excludes too
FILES=()
while IFS= read -r file; do
  FILES+=("$file")
done < <(grep -rlE --include='*.md' --include='*.mdx' --include='*.tsx' 'datasqrl/' "$@" \
  | grep -vE '/documentation/docs/stdlib-docs/[^/]+$' || true)

if [ "${#FILES[@]}" -eq 0 ]; then
  echo "No documented images found"
  exit 0
fi

# Match e.g. datasqrl/cmd, datasqrl/cmd:0.11.6, ghcr.io/datasqrl/adv-agent:latest. Stop before
# path-like continuations (datasqrl/sqrl/tree/...) and skip GitHub URLs.
IMAGES=$(grep -hoE \
    '(^|[^/.[:alnum:]])(ghcr\.io/)?datasqrl/[a-z0-9][a-z0-9._-]*(:[A-Za-z0-9._-]+)?([^/[:alnum:]]|$)' "${FILES[@]}" \
  | grep -oE '(ghcr\.io/)?datasqrl/[a-z0-9][a-z0-9._-]*(:[A-Za-z0-9._-]+)?' \
  | sed -E 's/[.]+$//' \
  | awk '/:/ { print; next } { print $0 ":latest" }' \
  | sort -u)

# Repositories under github.com/DataSQRL that are not images
NOT_IMAGES='^datasqrl/(sqrl|datasqrl-examples|datasqrl-plugin|flink-sql-runner):latest$'

# Documented images that are not released yet. They are reported, but don't fail the check.
# Remove an entry once the image is released, the check reports when that is the case.
UNRELEASED_IMAGES=(
  datasqrl/core-agent:latest # published with the next release
)

is_unreleased() {
  local image
  for image in "${UNRELEASED_IMAGES[@]}"; do
    [ "$image" = "$1" ] && return 0
  done
  return 1
}

# Use an empty Docker config, so no stored credentials are used
DOCKER_CONFIG="$(mktemp -d)"
export DOCKER_CONFIG
trap 'rm -rf "$DOCKER_CONFIG"' EXIT

failed=()
checked=0
pending=0
for image in $IMAGES; do
  if [[ "$image" =~ $NOT_IMAGES ]]; then
    continue
  fi
  checked=$((checked + 1))
  if docker manifest inspect "$image" > /dev/null 2>&1; then
    echo "OK      $image"
    if is_unreleased "$image"; then
      echo "NOTICE  $image is released now, remove it from UNRELEASED_IMAGES in $0"
    fi
  elif is_unreleased "$image"; then
    echo "PENDING $image (not released yet)"
    pending=$((pending + 1))
  else
    echo "MISSING $image"
    failed+=("$image")
  fi
done

if [ "${#failed[@]}" -gt 0 ]; then
  echo
  echo "These documented images cannot be pulled anonymously:" >&2
  for image in "${failed[@]}"; do
    echo "  $image" >&2
    grep -HnF "${image%:latest}" "${FILES[@]}" \
      | sed 's/^/    /' >&2 || true
  done
  exit 1
fi

echo
echo "All $((checked - pending)) released documented images can be pulled anonymously ($pending not released yet)"
