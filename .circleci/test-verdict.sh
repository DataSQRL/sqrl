#!/usr/bin/env bash
# Computes the run verdict from the sharding coordinator's session read model.
#
# Pass rule (coverage, never shard exit codes and never queue emptiness):
#   PASS <=> HTTP 200
#         && registered_count > 0
#         && count(state in {PASSED, SKIPPED, ABORTED}) == registered_count
#
# registered_count > 0 guards a coordinator that replayed its append log into an empty
# session, where 0 == 0 would go green. Printing the counts and the full skipped/aborted
# lists on every run is a hard requirement: it is what makes a single green readable, and
# the discriminator between a cutover bug (wrong registered count, 404, INCOMPLETE) and
# ordinary suite redness.
set -euo pipefail

: "${SHARD_COORDINATOR_URL:?SHARD_COORDINATOR_URL is required}"
: "${SHARD_SESSION_ID:?SHARD_SESSION_ID is required}"
# Asserted, never optional. The coordinator requires a bearer on this endpoint, so an unset
# context variable would otherwise degrade into an unauthenticated request, come back 401,
# and send the reader hunting a credential mismatch that does not exist.
: "${SHARD_COORDINATOR_SECRET:?SHARD_COORDINATOR_SECRET is required}"
command -v jq >/dev/null || { echo "test-verdict: jq is required" >&2; exit 1; }
RETRY_WINDOW_SECONDS=300

BODY_FILE=$(mktemp)
trap 'rm -f "$BODY_FILE"' EXIT

# One logical GET, retried over a bounded window because a coordinator restart can land on
# this single call. Falling back to ANDing the shard jobs' results when the coordinator is
# unreachable is rejected outright: that is the hole this job exists to close.
DEADLINE=$(( $(date +%s) + RETRY_WINDOW_SECONDS ))
HTTP_STATUS="000"
while :; do
  HTTP_STATUS=$(curl -sS -o "$BODY_FILE" -w '%{http_code}' --max-time 30 \
    -H "Authorization: Bearer ${SHARD_COORDINATOR_SECRET}" \
    "${SHARD_COORDINATOR_URL}/sessions/${SHARD_SESSION_ID}") || HTTP_STATUS="000"
  case "$HTTP_STATUS" in
    200|404) break ;;
    401|403)
      echo "test-verdict: coordinator rejected the credentials (HTTP ${HTTP_STATUS}) - not retryable; check that SHARD_COORDINATOR_SECRET in the datasqrl context matches a value the coordinator accepts" >&2
      exit 1
      ;;
  esac
  if [ "$(date +%s)" -ge "$DEADLINE" ]; then
    echo "test-verdict: coordinator unreachable (last status ${HTTP_STATUS}) after ${RETRY_WINDOW_SECONDS}s retry window" >&2
    exit 1
  fi
  echo "Coordinator returned ${HTTP_STATUS}; retrying..."
  sleep 5
done

if [ "$HTTP_STATUS" = "404" ]; then
  echo "test-verdict: session ${SHARD_SESSION_ID} is unknown to the coordinator - no shard ever registered. Rerun the whole workflow." >&2
  exit 1
fi

jq -e '.registeredCount != null and (.tests | type == "array")' "$BODY_FILE" >/dev/null || {
  echo "test-verdict: coordinator response is not a session read model" >&2
  cat "$BODY_FILE"
  exit 1
}

# One jq pass over the read model. Thirteen of them re-parsing the same file meant the
# non-trivial selectors were written twice and had to be kept in sync by hand.
eval "$(jq -r '
  def absorbing: ["PASSED","SKIPPED","ABORTED"];
  def terminal: absorbing + ["FAILED"];
  def flaky: .tests[] | select(.state == "PASSED")
             | select([.records[]? | select(.outcome == "FAILED")] | length > 0);
  @sh "TESTS=\([.tests | length])",
  @sh "REGISTERED=\(.registeredCount)",
  @sh "PASSED=\([.tests[] | select(.state == "PASSED")] | length)",
  @sh "SKIPPED=\([.tests[] | select(.state == "SKIPPED")] | length)",
  @sh "ABORTED=\([.tests[] | select(.state == "ABORTED")] | length)",
  @sh "FAILED=\([.tests[] | select(.state == "FAILED")] | length)",
  @sh "TERMINAL_OK=\([.tests[] | select(.state | IN(absorbing[]))] | length)",
  @sh "NOT_TERMINAL=\([.tests[] | select(.state | IN(terminal[]) | not)] | length)",
  @sh "FLAKY=\([flaky] | length)",
  @sh "VANISHED=\([.nacks[]? | select(.vanished == true)] | length)",
  @sh "UNEXPLAINED=\([.nacks[]? | select(.vanished != true)] | length)",
  @sh "NACKS_DROPPED=\(.nacksDropped // 0)",
  @sh "STALE=\(.staleResults | length)",
  @sh "STALE_DROPPED=\(.staleResultsDropped // 0)",
  @sh "PER_SHARD=\(if (.shards | type) == "array" then ([.shards[] | "\(.shard):\(.completed)"] | join(" ")) else "(unavailable - no shards array; coordinator schema drift?)" end)"
' "$BODY_FILE")"

echo "session       ${SHARD_SESSION_ID}"
echo "registered    ${REGISTERED}"
echo "passed        ${PASSED}"
echo "not passed    $(( REGISTERED - PASSED ))"
echo "  skipped     ${SKIPPED}"
echo "  aborted     ${ABORTED}"
echo "  failed      ${FAILED}"
echo "  no verdict  ${NOT_TERMINAL}"
echo "  flaky       ${FLAKY}"
echo "per shard     ${PER_SHARD}"
echo ""

# A test that failed and then passed is a flake, not a pass and not a failure. Only the
# coordinator can tell the difference: the shard that failed it reported honestly and moved
# on, and the shard that passed it never saw the failure. A run that is green because three
# tests were retried is not the same run as one green outright.
if [ "$FLAKY" != "0" ]; then
  echo "== FLAKY tests (${FLAKY}) - passed, but only after failing at least once =="
  jq -r '.tests[]
    | select(.state == "PASSED")
    | select([.records[]? | select(.outcome == "FAILED")] | length > 0)
    | "  \(.testId)\n" + ([.records[]? | "    attempt \(.attempt) on shard \(.shard): \(.outcome)"] | join("\n"))' \
    "$BODY_FILE"
fi

echo "== SKIPPED tests (${SKIPPED}) =="
jq -r '.tests[] | select(.state == "SKIPPED") | "  \(.testId)\n    reason: \(.reason // "(none)")"' "$BODY_FILE"
echo "== ABORTED tests (${ABORTED}) =="
jq -r '.tests[] | select(.state == "ABORTED") | "  \(.testId)\n    reason: \(.reason // "(none)")"' "$BODY_FILE"

# Every distributed parameterized method is handed one cardinality probe past its recorded
# plan, and the probe not materialising is the expected answer confirming the count -
# printing that as an anomaly would flag every healthy run. Anything else a shard hands back
# unexplained is worth reading. Both lists are capped by the coordinator, so the overflow
# counters are printed too: a truncated list that does not say it was truncated is worst
# exactly when it matters most.
echo "vanished leases      ${VANISHED} (cardinality probes and dropped positions; expected)"
if [ "$UNEXPLAINED" != "0" ]; then
  echo "== Unexplained NACKed leases (${UNEXPLAINED}) - a shard was granted work it could not"
  echo "   reconcile to an outcome. Not routine. =="
  jq -r '.nacks[]? | select(.vanished != true) | "  \(.testId)\n    reason: \(.reason)"' "$BODY_FILE"
fi
if [ "$NACKS_DROPPED" != "0" ]; then
  echo "   (${NACKS_DROPPED} further NACK(s) dropped past the coordinator's diagnostic cap)"
fi
if [ "$STALE" != "0" ]; then
  echo "== Stale (fenced-off) results (${STALE}) - a shard went zombie or was reclaimed =="
  jq -r '.staleResults[] | "  \(.testId) shard \(.shard) \(.outcome)"' "$BODY_FILE"
fi
if [ "$STALE_DROPPED" != "0" ]; then
  echo "   (${STALE_DROPPED} further stale result(s) dropped past the diagnostic cap)"
fi

# registeredCount counts claimable units after parameterized expansion; .tests is the same
# set rendered. If they ever disagree the pass rule below fires with nothing to print, so
# say so rather than emitting a bare FAIL with no diagnosis.
if [ "$TESTS" -ne "$REGISTERED" ]; then
  echo "test-verdict: the read model lists ${TESTS} test(s) but reports registeredCount=${REGISTERED} - coordinator schema drift; the counts below cannot be trusted" >&2
fi

if [ "$REGISTERED" -le 0 ]; then
  echo "test-verdict: registered_count is ${REGISTERED} - the session registered nothing; treating as a cutover or coordinator bug, never green" >&2
  exit 1
fi

if [ "$TERMINAL_OK" -ne "$REGISTERED" ]; then
  jq -r '.tests[] | select(.state | IN("PASSED","SKIPPED","ABORTED") | not) |
    "FAIL \(.testId) state=\(.state) attempts=\(.records | length) last_failed_on_shard=\(.records | map(select(.outcome == "FAILED")) | last | if . == null then "none recorded" else "\(.shard)" end) reason=\(.reason // "(none)")"' \
    "$BODY_FILE" >&2
  if [ "$NOT_TERMINAL" -gt 0 ]; then
    echo "test-verdict: INCOMPLETE - ${NOT_TERMINAL} registered test(s) never reached a terminal state. Rerun the whole workflow rather than a single job: a lone rerun shard rejoins the same session and only picks up what is still outstanding, which is right, but a rerun after the coordinator has garbage-collected the session starts an empty one and is red for a reason that has nothing to do with the tests." >&2
  fi
  echo "Verdict: FAIL (${TERMINAL_OK}/${REGISTERED})" >&2
  exit 1
fi

echo "Verdict: PASS (${TERMINAL_OK}/${REGISTERED})"
