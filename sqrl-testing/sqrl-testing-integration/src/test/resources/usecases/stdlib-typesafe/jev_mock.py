#!/usr/bin/env python3
"""
Mock TypeSafe AI Jev Server for the stdlib-typesafe Use Case

A Python port of TypeSafeTestExtension
(sqrl-testing/sqrl-testing-integration/src/test/java/com/datasqrl/tests/TypeSafeTestExtension.java)
to run this use case outside the Maven test harness, e.g. with the DataSQRL Docker image. The
snapshots depend on its answers, so keep both implementations in sync.

REQUIREMENTS:
    Python 3 (standard library only)

USAGE:
    python3 jev_mock.py [port] [host]

    Serves POST /v1/systemone on localhost:8765 by default and logs every request with its answers.
"""

import json
import math
import sys
from decimal import ROUND_FLOOR, Decimal
from http.server import BaseHTTPRequestHandler, HTTPServer

API_KEY = "test-key"
MAX_STATE_LENGTH = 800
CHOSEN_PROBABILITY = 0.8


def probability(chosen, count):
    """The chosen answer gets CHOSEN_PROBABILITY, the others share the rest evenly."""
    if chosen:
        return CHOSEN_PROBABILITY
    # Round half up to four decimals, like Java's Math.round
    return math.floor((1 - CHOSEN_PROBABILITY) / (count - 1) * 10_000 + 0.5) / 10_000.0


def java_double(value):
    """Formats a float like the Java mock, which writes doubles with Jackson's fast double writer."""
    if value == 0 or 1e-3 <= abs(value) < 1e7:
        return repr(value)
    sign, digits, exponent = Decimal(repr(value)).normalize().as_tuple()
    if len(digits) == 1:
        # Java picks the closest round-tripping decimal of one or two digits, which differs from
        # the one-digit decimal only for subnormals
        exact = Decimal(abs(value))
        step = Decimal(1).scaleb(exact.adjusted() - 1)
        low = (exact / step).to_integral_value(rounding=ROUND_FLOOR) * step
        candidates = [Decimal(digits[0]).scaleb(exponent), low, low + step]
        best = min((c for c in candidates if float(c) == abs(value)), key=lambda c: abs(c - exact))
        _, digits, exponent = best.normalize().as_tuple()
    fraction = "".join(map(str, digits[1:])) or "0"
    return f"{'-' if sign else ''}{digits[0]}.{fraction}E{exponent + len(digits) - 1}"


def to_json(node):
    """Serializes compactly like the Java mock's MAPPER.writeValueAsString."""
    if isinstance(node, dict):
        return "{" + ",".join(f"{to_json(k)}:{to_json(v)}" for k, v in node.items()) + "}"
    if isinstance(node, list):
        return "[" + ",".join(to_json(v) for v in node) + "]"
    if isinstance(node, float):
        return java_double(node)
    return json.dumps(node, ensure_ascii=False)


def as_text(node):
    """Converts a JSON value to text like Jackson's JsonNode.asText()."""
    return node if isinstance(node, str) else to_json(node)


def java_length(text):
    """Counts UTF-16 code units, like Java's String.length()."""
    return len(text.encode("utf-16-le")) // 2


def answer(question, state):
    qtype = question.get("type")
    criteria = question.get("criteria")
    out = {"type": qtype}
    if qtype == "noul":
        out["noul"] = 0.9 if "verified" in state else 0.1
    elif qtype == "choice":
        options = list(criteria.keys())
        choice = next((o for o in options if o.replace("_", " ") in state), options[-1])
        out.update(
            choice=choice,
            confidence=CHOSEN_PROBABILITY,
            probabilities={o: probability(o == choice, len(options)) for o in options},
        )
    elif qtype == "score":
        levels = len(criteria)
        score = next((i for i in range(levels) if as_text(criteria[i]).lower() in state), 0)
        out.update(
            score=float(score),
            confidence=CHOSEN_PROBABILITY,
            probabilities={str(i): probability(i == score, levels) for i in range(levels)},
        )
    else:
        raise ValueError(f"Unknown question type: {qtype}")
    return out


def respond(authorization, request):
    if authorization != f"Bearer {API_KEY}":
        return 401, error("invalid_api_key")
    state = to_json(request.get("state"))
    if java_length(state) > MAX_STATE_LENGTH:
        return 422, error("token_limit_exceeded")
    text = state.lower()
    answers = {key: answer(q, text) for key, q in request.get("questions", {}).items()}
    return 200, {
        "model": request.get("model"),
        "answers": answers,
        "usage": {"input_tokens": java_length(state), "output_tokens": 10},
    }


def error(code):
    return {"error": {"code": code, "message": "Rejected by the mock Jev server"}}


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
        status, payload = respond(self.headers.get("Authorization"), body)
        print(
            f"{status} questions={list(body.get('questions', {}))} "
            f"-> {json.dumps(payload.get('answers', payload.get('error')))}",
            flush=True,
        )
        data = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8765
    host = sys.argv[2] if len(sys.argv) > 2 else "localhost"
    print(f"Mock Jev server listening on http://{host}:{port}/v1/systemone", flush=True)
    HTTPServer((host, port), Handler).serve_forever()
