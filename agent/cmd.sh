#!/bin/bash
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

#
# Wrapper script for /opt/sqrl/entrypoint.sh that cleans up Postgres and Kafka
# data directories before each invocation to ensure a fresh state.
#
# Only one instance of this script can run at a time. Other callers will
# block until the lock is available.

set -e

# Older CLI images always use /build, while newer ones prefer it whenever the
# directory exists. When it is only the inherited empty directory, point it at
# the agent's /workspace mount. A user-supplied /build mount cannot be removed
# and retains the CLI's backwards-compatible behavior.
if [ -d /workspace ] && [ -d /build ]; then
    if rmdir /build 2>/dev/null; then
        ln -s /workspace /build
    fi
fi

# DataSQRL copies recognized data files into build/ on each compile, test, or
# run. Reject oversized inputs early so a normal development command does not
# spend its time copying data rather than compiling the project.
case "${1:-}" in
    compile|test|run)
        if [ "${SQRL_ALLOW_LARGE_DATA:-}" != "1" ]; then
            _thresh_mb="${SQRL_LARGE_DATA_THRESHOLD_MB:-10}"
            # find -size rejects non-integers; fall back to the default.
            case "$_thresh_mb" in ''|*[!0-9]*) _thresh_mb=10 ;; esac
            _ws="${WORKSPACE_DIR:-/workspace}"
            # Prune compiler output, VCS, and tooling directories at every
            # depth. -mindepth 1 keeps a project named `build` in scope.
            _big=$(find "$_ws" -mindepth 1 \
                \( -type d \( -name build -o -name 'build-*' -o -name .git -o -name snapshots \
                              -o -name node_modules -o -name .claude \) -prune \) -o \
                \( -type f -size +"${_thresh_mb}"M \
                   \( -iname '*.csv' -o -iname '*.tsv' -o -iname '*.json' -o -iname '*.jsonl' \
                      -o -iname '*.ndjson' -o -iname '*.parquet' -o -iname '*.orc' \
                      -o -iname '*.avro' -o -iname '*.gz' -o -iname '*.zip' \) \
                   -not -iname '*.bak' -print \) \
                2>/dev/null || true)
            if [ -n "$_big" ]; then
                echo "LARGE_DATA_BLOCKED: DataSQRL copies every data file into build/ on each ${1}."
                echo "These large files would make the run very slow / exceed the test timeout:"
                printf '%s\n' "$_big" | while IFS= read -r _f; do
                    [ -n "$_f" ] && echo "  - $_f ($(du -h "$_f" 2>/dev/null | cut -f1))"
                done
                echo "Sample or exclude the files, then re-run."
                echo "(Intentional large-data run? set SQRL_ALLOW_LARGE_DATA=1.)"
                echo "COMPILE_DONE: exit_code=3"
                exit 3
            fi
        fi
        ;;
esac

# Acquire an exclusive lock so engine state is not shared by simultaneous runs.
LOCKFILE="/var/lock/sqrl-entrypoint.lock"
exec 200>"$LOCKFILE"
if ! flock -n 200; then
    echo "COMPILE_QUEUED: another compile is already running. This call will start automatically when it completes — do NOT retry or launch another compile."
    flock 200
    echo "COMPILE_QUEUED_DONE: lock acquired, starting now."
fi

# Kill lingering processes from previous runs and start from fresh local state.
pkill -u postgres -f postgres >/dev/null 2>&1 || true
pkill -f redpanda >/dev/null 2>&1 || true
sleep 1
rm -rf /data/postgres 2>/dev/null || true
rm -rf /data/redpanda 2>/dev/null || true

# Increase PostgreSQL connections after initdb creates its configuration. The
# worker is detached and bounded so it cannot hold a caller's output pipe open
# when PostgreSQL is not part of this particular project.
(
    exec 200>&- 1>/dev/null 2>&1
    SECONDS=0
    while [ ! -f /data/postgres/postgresql.conf ]; do
        [ "$SECONDS" -ge 600 ] && exit 0
        sleep 0.05
    done
    echo "max_connections = 500" >> /data/postgres/postgresql.conf
    SECONDS=0
    while ! pg_isready -q 2>/dev/null; do
        [ "$SECONDS" -ge 120 ] && exit 0
        sleep 0.1
    done
    pg_ctl reload -D /data/postgres 2>/dev/null || true
) &

/opt/sqrl/entrypoint.sh "$@"
EXIT_CODE=$?
echo "COMPILE_DONE: exit_code=$EXIT_CODE"
exit $EXIT_CODE
