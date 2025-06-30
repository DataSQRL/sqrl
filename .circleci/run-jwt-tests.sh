#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/.."

cd "${PROJECT_ROOT}/sqrl-testing/sqrl-integration-tests/src/test/resources/usecases/jwt"

docker run \
  -v "$PWD:/build" \
  -e TZ="UTC" \
  "datasqrl/cmd-test-image:${CIRCLE_SHA1}" \
  compile jwt.sqrl

docker run -d --name sqrl-server \
  -v "$PWD/build/deploy/plan/vertx.json:/opt/sqrl/vertx.json" \
  -v "$PWD/build/deploy/plan/vertx-config.json:/opt/sqrl/vertx-config.json" \
  -p 8888:8888 \
  "datasqrl/sqrl-server-test-image:${CIRCLE_SHA1}"

printf "⏳ Waiting for server"
for i in {1..30}; do
  if curl -s http://localhost:8888/ > /dev/null 2>&1; then
    echo " – ready"
    break
  fi
  sleep 1
  printf "."
  [[ $i == 30 ]] && { echo " ❌  server never came up"; docker logs sqrl-server; exit 1; }
done

if curl -s -o /dev/null -w '%{http_code}' \
      -X POST http://localhost:8888/graphql \
      -H 'Content-Type: application/json' \
      --data '{"query":"query { __typename }"}' \
      | grep -q '^401$'; then
  echo "✅  Unauthorized without token – as expected"
else
  echo "❌  Expected 401 without token but got something else"
  docker logs sqrl-server
  exit 1
fi

SECRET='testSecret'
ISSUER='my-test-issuer'
AUDIENCE='my-test-audience'
EXP=$(( $(date +%s) + 3600 ))

b64() { openssl base64 -A | tr '+/' '-_' | tr -d '='; }

HEADER=$(printf '{"alg":"HS256","typ":"JWT"}' | b64)
PAYLOAD=$(printf '{"iss":"%s","aud":"%s","exp":%d}' "$ISSUER" "$AUDIENCE" "$EXP" | b64)
SIGNATURE=$(printf '%s.%s' "$HEADER" "$PAYLOAD" \
             | openssl dgst -binary -sha256 -hmac "$SECRET" | b64)

TOKEN="${HEADER}.${PAYLOAD}.${SIGNATURE}"

if curl -s -X POST http://localhost:8888/graphql \
        -H 'Content-Type: application/json' \
        -H "Authorization: Bearer ${TOKEN}" \
        --data '{"query":"query { __typename }"}' \
        | grep -q '"__typename"'; then
  echo "✅  Request authorised – token works"
else
  echo "❌  Authorised request failed"
  docker logs sqrl-server
  exit 1
fi

docker rm -f sqrl-server >/dev/null