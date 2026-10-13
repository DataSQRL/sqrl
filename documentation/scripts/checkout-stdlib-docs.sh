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

# Checks out the stdlib-docs submodule (the function docs of the Flink SQL Runner) at the runner
# version the build uses, i.e. the `flinkrunner.version` of the root pom.xml:
#   - a release version checks out the runner tag of that version
#   - a SNAPSHOT version checks out the runner's main branch
# Only the stdlib-docs folder of the runner is checked out.
#
# Usage: checkout-stdlib-docs.sh [repo-dir]
# repo-dir defaults to the repository this script belongs to.

set -euo pipefail

REPO_DIR="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
SUBMODULE_PATH="documentation/docs/stdlib-docs"

version="$(
  sed -n 's:.*<flinkrunner\.version>\([^$<]*\)</flinkrunner\.version>.*:\1:p' "${REPO_DIR}/pom.xml" \
    | head -n 1
)"
if [ -z "${version}" ]; then
  echo "Could not determine flinkrunner.version from ${REPO_DIR}/pom.xml" >&2
  exit 1
fi

if [[ "${version}" == *-SNAPSHOT ]]; then
  ref="main"
else
  ref="refs/tags/${version}"
fi
echo "Checking out stdlib-docs at ${ref} (flinkrunner.version ${version})"

git -C "${REPO_DIR}" submodule update --init --depth 1 "${SUBMODULE_PATH}"
git -C "${REPO_DIR}/${SUBMODULE_PATH}" sparse-checkout set --cone stdlib-docs
git -C "${REPO_DIR}/${SUBMODULE_PATH}" fetch --depth 1 origin "${ref}"
git -C "${REPO_DIR}/${SUBMODULE_PATH}" checkout --quiet --detach FETCH_HEAD
