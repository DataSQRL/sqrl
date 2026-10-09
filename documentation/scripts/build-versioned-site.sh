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

# Builds the versioned documentation site:
#   - the latest major release is built from its latest release-X.Y branch and served at /
#   - main is built from the current checkout and served at /main/
#   - every older major release is built from its latest release-X.Y branch and served at /vX/
# The blog is always taken from main and published at /blog. Until there is a release, main is
# served at / instead.
#
# Must be run from a checkout of main with the release branches available as
# ${REMOTE}/release-X.Y refs. The output is written to documentation/build.

set -euo pipefail

REMOTE="${REMOTE:-origin}"

DOCS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_DIR="$(cd "${DOCS_DIR}/.." && pwd)"
OUT_DIR="${DOCS_DIR}/build"
WORK_DIR="$(mktemp -d)"

cleanup() {
  for wt in "${WORK_DIR}"/worktrees/*; do
    [ -d "${wt}" ] && git -C "${REPO_DIR}" worktree remove --force "${wt}" || true
  done
  rm -rf "${WORK_DIR}"
}
trap cleanup EXIT

# Latest release branch of each major, newest major first
release_branches="$(
  git -C "${REPO_DIR}" for-each-ref --format='%(refname:short)' "refs/remotes/${REMOTE}/release-*" \
    | sed -n "s:^${REMOTE}/release-\([0-9][0-9]*\)\.\([0-9][0-9]*\)$:\1 \2:p" \
    | sort -k1,1nr -k2,2nr \
    | awk '!seen[$1]++ { print "release-" $1 "." $2 }'
)"

major_of() {
  local version="${1#release-}"
  echo "${version%%.*}"
}

latest_branch="$(echo "${release_branches}" | head -1)"
latest_label="v$(major_of "${latest_branch:-release-0}")"

# Prints the path a version is served at
path_of() {
  local label="$1"
  if [ "${label}" = "${latest_label}" ] || [ -z "${latest_branch}" ]; then
    echo "/"
  elif [ "${label}" = "main" ]; then
    echo "/main/"
  else
    echo "/${label}/"
  fi
}

versions="[{\"label\":\"main\",\"path\":\"$(path_of main)\"}"
for branch in ${release_branches}; do
  label="v$(major_of "${branch}")"
  versions+=",{\"label\":\"${label}\",\"path\":\"$(path_of "${label}")\"}"
done
versions+="]"
echo "Building documentation versions: ${versions}"

# Builds the site in the given documentation directory and copies it to the version's path
build_site() {
  local docs_dir="$1" label="$2" kind="$3"
  local base_url
  base_url="$(path_of "${label}")"
  (
    cd "${docs_dir}"
    npm ci
    DOCS_BASE_URL="${base_url}" \
      DOCS_VERSION_LABEL="${label}" \
      DOCS_VERSION_KIND="${kind}" \
      DOCS_LATEST_LABEL="${latest_label}" \
      DOCS_VERSIONS="${versions}" \
      npm run build
  )
  mkdir -p "${WORK_DIR}/sites"
  rm -rf "${WORK_DIR}/sites/${label}"
  mv "${docs_dir}/build" "${WORK_DIR}/sites/${label}"
}

# Checks out the release branch into a worktree and sets docs_dir to its documentation directory
checkout_release() {
  local branch="$1"
  local wt="${WORK_DIR}/worktrees/${branch}"
  git -C "${REPO_DIR}" worktree add --detach "${wt}" "${REMOTE}/${branch}"
  # Use the stdlib docs pinned by the release branch
  git -C "${wt}" submodule update --init --depth 1 documentation/docs/stdlib-docs
  git -C "${wt}/documentation/docs/stdlib-docs" sparse-checkout set --cone stdlib-docs
  docs_dir="${wt}/documentation"
}

echo "Building main from the current checkout"
build_site "${DOCS_DIR}" "main" "$([ -n "${latest_branch}" ] && echo main)"

for branch in ${release_branches}; do
  label="v$(major_of "${branch}")"
  echo "Building ${label} from ${branch}"
  checkout_release "${branch}"
  if [ "${label}" = "${latest_label}" ]; then
    # The root site publishes the blog, which is maintained on main
    rm -rf "${docs_dir}/blog"
    cp -R "${DOCS_DIR}/blog" "${docs_dir}/blog"
    build_site "${docs_dir}" "${label}" "latest"
  else
    build_site "${docs_dir}" "${label}" "older"
  fi
done

# Assemble the site, starting with the version served at the root
rm -rf "${OUT_DIR}"
for site in "${WORK_DIR}/sites/"*; do
  label="$(basename "${site}")"
  if [ "$(path_of "${label}")" = "/" ]; then
    mv "${site}" "${OUT_DIR}"
  fi
done
for site in "${WORK_DIR}/sites/"*; do
  label="$(basename "${site}")"
  target="${OUT_DIR}$(path_of "${label}")"
  mv "${site}" "${target%/}"
done
