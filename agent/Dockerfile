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

# Build from the repository root. Pass the locally built CLI image when
# developing both images together:
# docker build --build-arg SQRL_IMAGE=datasqrl/cmd:local -t core-agent -f agent/Dockerfile .
ARG SQRL_IMAGE=datasqrl/cmd:dev
ARG NODE_IMAGE=node:22-bookworm-slim

FROM ${NODE_IMAGE} AS node

# Generates the documentation of this checkout as a single Markdown file (llms-full.txt, see
# documentation/scripts/generate-llms-txt.mjs). Requires the stdlib-docs submodule:
# git submodule update --init documentation/docs/stdlib-docs
FROM node AS docs
# Path of these docs on docs.datasqrl.com: `main` for the unreleased docs
ARG DOCS_VERSION_PATH=main
WORKDIR /documentation
COPY documentation/package-lock.json /tmp/package-lock.json
# The function docs generator is the only one with a dependency, install it at its locked version
RUN npm install --no-save --no-package-lock --ignore-scripts --no-audit --no-fund \
      "js-yaml@$(node -p 'require("/tmp/package-lock.json").packages["node_modules/js-yaml"].version')"
COPY documentation/docs/ docs/
COPY documentation/sidebars.ts ./
COPY documentation/scripts/ scripts/
RUN test -f docs/stdlib-docs/stdlib-docs/system-functions.yml \
      || { echo "Missing function docs, run: git submodule update --init documentation/docs/stdlib-docs" >&2; exit 1; } \
    && node scripts/generate-function-docs.js \
    && node --disable-warning=MODULE_TYPELESS_PACKAGE_JSON scripts/generate-llms-txt.mjs /out \
      --current-path "${DOCS_VERSION_PATH}" \
    && cp "/out/docs${DOCS_VERSION_PATH:+/${DOCS_VERSION_PATH}}/llms-full.txt" /llms-full.txt

FROM ${SQRL_IMAGE}

ARG PI_PACKAGE=@earendil-works/pi-coding-agent@1.3.0

USER root

# Install Pi before copying agent content so this layer remains cached when
# only instructions or skills change.
COPY --from=node /usr/local /usr/local
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl git python3 ripgrep \
    && npm install -g --ignore-scripts "${PI_PACKAGE}" \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# /workspace is the supported mount point of the DataSQRL CLI image. The
# command wrapper redirects an empty inherited /build directory to it at
# runtime, while preserving a real user-supplied /build mount.
RUN mkdir -p /workspace /opt/agent /root/.pi/agent \
    && rm -rf /build \
    && (test ! -e /build || rmdir /build)

# Pi rejects --provider without --model, so the entrypoint looks up the default
# model of the selected provider in Pi's own defaults. They are not part of
# Pi's public API, so this fails the build if a Pi upgrade moves them.
RUN node --input-type=module -e ' \
      const { defaultModelPerProvider } = await import(process.argv[1]); \
      for (const [provider, model] of Object.entries(defaultModelPerProvider)) console.log(`${provider}\t${model}`);' \
      "$(npm root -g)/@earendil-works/pi-coding-agent/dist/core/model-resolver.js" \
      > /opt/agent/pi-default-models.tsv \
    && test -s /opt/agent/pi-default-models.tsv

COPY README.md /root/.pi/agent/README_SQRL.md
COPY agent/AGENTS.md /root/.pi/agent/AGENTS.md
COPY agent/CLI_REFERENCE.md /root/.pi/agent/CLI_REFERENCE.md
COPY agent/skills/ /root/.pi/agent/skills/
COPY --from=docs /llms-full.txt /root/.pi/agent/docs/llms-full.txt
COPY --chmod=0755 agent/cmd.sh /opt/agent/cmd.sh
COPY --chmod=0755 agent/entrypoint.sh /opt/agent/entrypoint.sh
# HS256 test-token helper taught by the design-api skill.
COPY --chmod=0755 agent/bin/jwt /usr/local/bin/jwt

WORKDIR /workspace

ENV AGENT_HOME=/opt/agent \
    WORKSPACE_DIR=/workspace

ENTRYPOINT ["/opt/agent/entrypoint.sh"]
CMD []
