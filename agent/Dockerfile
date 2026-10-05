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
FROM ${SQRL_IMAGE}

ARG PI_PACKAGE=@earendil-works/pi-coding-agent@1.0.3

USER root

# Install Pi before copying agent content so this layer remains cached when
# only instructions or skills change.
COPY --from=node /usr/local /usr/local
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl git ripgrep \
    && npm install -g --ignore-scripts "${PI_PACKAGE}" \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# /workspace is the supported mount point of the DataSQRL CLI image. The
# command wrapper redirects an empty inherited /build directory to it at
# runtime, while preserving a real user-supplied /build mount.
RUN mkdir -p /workspace /opt/agent /root/.pi/agent \
    && rm -rf /build \
    && (test ! -e /build || rmdir /build)

COPY README.md /root/.pi/agent/README_SQRL.md
COPY agent/AGENTS.md /root/.pi/agent/AGENTS.md
COPY agent/CLI_REFERENCE.md /root/.pi/agent/CLI_REFERENCE.md
COPY agent/skills/ /root/.pi/agent/skills/
COPY --chmod=0755 agent/cmd.sh /opt/agent/cmd.sh
COPY --chmod=0755 agent/entrypoint.sh /opt/agent/entrypoint.sh

WORKDIR /workspace

ENV AGENT_HOME=/opt/agent \
    WORKSPACE_DIR=/workspace

ENTRYPOINT ["/opt/agent/entrypoint.sh"]
CMD []
