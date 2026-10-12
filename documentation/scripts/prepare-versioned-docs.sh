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

# Prepares the versioned documentation (versions.json, versioned_docs/ and versioned_sidebars/)
# from the release branches, so that a subsequent `npm run build` builds a versioned site:
#   - the docs of each major release vX are taken from its latest release-X.Y branch
#   - before 1.0, each minor release v0.Y is a version of its own, taken from its release-0.Y
#     branch, starting with 0.11; earlier releases predate versioning and are skipped
#   - the latest major release is the default docs version, served at /docs/
#   - main is the unreleased docs version, served at /docs/main/
#   - older releases are served at /docs/vX/ (or /docs/v0-Y/)
# Everything else (homepage, blog, community) is not versioned and always comes from main.
#
# Must be run from a checkout of main with the release branches available as
# ${REMOTE}/release-X.Y refs. Without release branches, the docs are left unversioned.

set -euo pipefail

REMOTE="${REMOTE:-origin}"
FIRST_VERSIONED_ZERO_MINOR=11

DOCS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_DIR="$(cd "${DOCS_DIR}/.." && pwd)"
WORK_DIR="$(mktemp -d)"

cleanup() {
  for wt in "${WORK_DIR}"/*; do
    [ -d "${wt}" ] && git -C "${REPO_DIR}" worktree remove --force "${wt}" || true
  done
  rm -rf "${WORK_DIR}"
}
trap cleanup EXIT

# Latest release branch of each major (each minor before 1.0), newest first
release_branches="$(
  git -C "${REPO_DIR}" for-each-ref --format='%(refname:short)' "refs/remotes/${REMOTE}/release-*" \
    | sed -n "s:^${REMOTE}/release-\([0-9][0-9]*\)\.\([0-9][0-9]*\)$:\1 \2:p" \
    | sort -k1,1nr -k2,2nr \
    | awk -v min="${FIRST_VERSIONED_ZERO_MINOR}" \
        '($1 > 0 && !seen[$1]++) || ($1 == 0 && $2 >= min) { print "release-" $1 "." $2 }'
)"

rm -rf "${DOCS_DIR}/versioned_docs" "${DOCS_DIR}/versioned_sidebars" "${DOCS_DIR}/versions.json"
mkdir -p "${DOCS_DIR}/versioned_docs" "${DOCS_DIR}/versioned_sidebars"

versions=()
for branch in ${release_branches}; do
  # vX for major versions, v0.Y for minor versions before 1.0
  version="v${branch#release-}"
  [[ "${version}" == v0.* ]] || version="${version%%.*}"
  wt="${WORK_DIR}/${branch}"
  echo "Preparing docs ${version} from ${branch}"

  git -C "${REPO_DIR}" worktree add --detach "${wt}" "${REMOTE}/${branch}"
  # Use the stdlib docs pinned by the release branch
  git -C "${wt}" submodule update --init --depth 1 documentation/docs/stdlib-docs
  git -C "${wt}/documentation/docs/stdlib-docs" sparse-checkout set --cone stdlib-docs

  # Let Docusaurus snapshot the release branch's docs and sidebars as a version
  (
    cd "${wt}/documentation"
    npm ci --no-audit --no-fund
    npm run generate-docs
    npx docusaurus docs:version "${version}"
  )

  cp -R "${wt}/documentation/versioned_docs/version-${version}" "${DOCS_DIR}/versioned_docs/"
  rm -f "${DOCS_DIR}/versioned_docs/version-${version}/stdlib-docs/.git"
  cp "${wt}/documentation/versioned_sidebars/version-${version}-sidebars.json" "${DOCS_DIR}/versioned_sidebars/"
  versions+=("\"${version}\"")
done

if [ "${#versions[@]}" -gt 0 ]; then
  (IFS=,; echo "[${versions[*]}]") > "${DOCS_DIR}/versions.json"
  echo "Prepared docs versions: $(cat "${DOCS_DIR}/versions.json")"
else
  rmdir "${DOCS_DIR}/versioned_docs" "${DOCS_DIR}/versioned_sidebars"
  echo "No release branches found, the docs stay unversioned"
fi
