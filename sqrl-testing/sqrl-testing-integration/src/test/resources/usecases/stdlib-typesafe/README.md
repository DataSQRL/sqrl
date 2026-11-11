# TypeSafe Classify Use Case

End-to-end test of the `typesafe_classify` process table function (PTF) from `stdlib.typesafe`
(the `stdlib-typesafe` module of the Flink SQL Runner). The function sends each row's state to
TypeSafe AI's Jev model and returns typed answers to the questions declared as columns of the
input table.

## Files

| File | Purpose |
|------|---------|
| `package.json` | Passes the API key and the mock's endpoint, `http://localhost:8765/v1/systemone`, to the function as Flink global job parameters; `delay-sec: -1` waits until the pipeline is idle before running the tests. |
| `underwriting/sources.sqrl` | Declares the `Applications` source table, with `submitted_at` as the event time. |
| `underwriting/applications.jsonl` | Six loan applications, each built to exercise one behavior (see below). |
| `stdlib-typesafe.sqrl` | The pipeline under test. |
| `snapshots/*.snapshot` | Expected results of the two test tables. |
| `jev_mock.py` | Standalone mock Jev server for running the test with the Docker image (see below). |

## The pipeline

1. `_UnderwritingInput` declares one question of each kind as a typed column:
   * `q_income_verified` (**noul**): do the verification notes confirm employment and income? The
     criteria ROW has the fields `true` and `false`. The answer is a probability.
   * `q_purpose` (**choice**): the field names of the criteria ROW are the options
     (`debt_consolidation`, `home_improvement`, `vehicle`, `business`, `other`). The question is
     NULL when the application states no loan purpose, so it is skipped for that row only.
   * `q_scrutiny` (**score**): the criteria ARRAY lists four ordinal levels, from
     `No concerns` (0) to `Serious red flags` (3).
2. `UnderwritingJudgments` calls `typesafe_classify` with `case_file` as the state the model reads,
   `on_error => 'NULL'` so failed requests yield NULL answers plus an `_error` message instead of
   failing the job, and `on_time => DESCRIPTOR(submitted_at)` so the output carries a `rowtime`.
   Each question produces a typed `<question>_answer` column. Because the choice options are
   field names, `q_purpose_answer.probabilities.business` is checked at compile time.
3. `DailyUnderwriting` aggregates the judgments in one-day tumbling windows over `rowtime`,
   which shows that the function's output works with event-time windows.

## The mock Jev server

[`TypeSafeTestExtension`](../../../java/com/datasqrl/tests/TypeSafeTestExtension.java) starts an
HTTP server on `localhost:8765`, the endpoint set in `package.json`, while this use case runs. It
answers from keywords in the request's state (matched case-insensitively), so every answer in the
snapshots can be traced to the input:

| Question | Answer |
|----------|--------|
| noul | `0.9` if the state contains `verified`, `0.1` otherwise. |
| choice | The first option whose name, with underscores read as spaces, appears in the state; the last option otherwise. |
| score | The first level whose text appears in the state; level `0` otherwise. |

The chosen option or level gets probability `0.8` and the others share the remaining `0.2`
evenly, rounded to four decimals: `0.05` for the five purpose options, `0.0667` for the four
scrutiny levels. Like the real API, the mock rejects a state longer than 800 characters with
`422 token_limit_exceeded` and a request without `Bearer test-key` with `401`.

## Input rows

| Row | What is special about it | Exercises |
|-----|--------------------------|-----------|
| `app-1` | Income verified, debt consolidation, "No concerns" | All three question kinds answered |
| `app-2` | Employer unreachable, home improvement, "Minor concerns" | A low probability and different choice and score |
| `app-3` | Loan purpose is `null` | A NULL question is left out of the request for that row only |
| `app-4` | `case_file` is `null` | No state means no request: NULL answers and no error |
| `app-5` | Very long underwriter notes | An API rejection (422) becomes NULL answers plus an `_error`, and the job continues |
| `app-6` | Submitted a day later; mentions both a vehicle and a business; "Serious red flags" | A second window, and reading one choice probability by option name |

## Snapshots

### `UnderwritingJudgmentsTest.snapshot`

One entry per application, ordered by `application_id`:

| Field | Meaning |
|-------|---------|
| `income_verified` | Noul answer: probability that the income is verified. |
| `purpose` | Choice answer: the chosen option name. |
| `purpose_confidence` | Probability of the chosen option. |
| `business_probability` | Probability of the `business` option, read by field name from the typed answer. |
| `scrutiny` | Score answer: the chosen level (0 to 3). |
| `scrutiny_probabilities` | Probability of each level, indexed by level. |
| `model` | The model the function was configured with (`jev-1.13.0`). |
| `error` | The error message when the request failed, NULL otherwise. |

| Row | `income_verified` | `purpose` | `scrutiny` | `error` |
|-----|-------------------|-----------|------------|---------|
| `app-1` | 0.9 | `debt_consolidation` | 0 | NULL |
| `app-2` | 0.1 | `home_improvement` | 1 | NULL |
| `app-3` | 0.9 | NULL (question skipped) | 2 | NULL |
| `app-4` | NULL | NULL | NULL | NULL (no request made) |
| `app-5` | NULL | NULL | NULL | `HTTP_422: request rejected by the API (token_limit_exceeded)` |
| `app-6` | 0.9 | `vehicle` | 3 | NULL |

`app-6` mentions both a vehicle and a business. `vehicle` is chosen because it comes first in the
option order, so `business_probability` is `0.05`.

### `DailyUnderwritingTest.snapshot`

One entry per day:

| Window | `applications` | `errors` | `verified_income` |
|--------|----------------|----------|-------------------|
| 2026-01-05 | 5 (`app-1` to `app-5`) | 1 (`app-5`) | 2 (`app-1`, `app-3`) |
| 2026-01-06 | 1 (`app-6`) | 0 | 1 (`app-6`) |

`errors` counts rows with a non-NULL `error`, so `app-4` is not an error. `verified_income`
counts rows with `income_verified > 0.5`.

## Running the test

### With Maven

`FullUseCaseIT` runs every use case. To run only this one, point `specificUseCaseProvider` in
`FullUseCaseIT` at `stdlib-typesafe` and run:

```bash
mvn -B verify -pl sqrl-testing/sqrl-testing-integration -am \
  -Dtest=none -Dsurefire.failIfNoSpecifiedTests=false \
  -Dit.test='FullUseCaseIT#specificUseCase' -Dfailsafe.failIfNoSpecifiedTests=false \
  -Djunit.jupiter.conditions.deactivate='org.junit.*DisabledCondition'
```

### With the Docker image

Outside the Maven test harness, `jev_mock.py` stands in for the Jev endpoint. It is a Python port
of `TypeSafeTestExtension` that gives the same answers. Start it from this folder:

```bash
python3 jev_mock.py
```

Inside the container, the `localhost:8765` endpoint in `package.json` refers to the container
itself, so point the function at the mock on the host with a second package file that overrides
the endpoint. In a second terminal, create it in this folder:

```bash
cat > endpoint-override.json <<'EOF'
{
  "version": "1",
  "engines": {
    "flink": {
      "config": {
        "pipeline.global-job-parameters.typesafe.endpoint": "http://host.docker.internal:8765/v1/systemone"
      }
    }
  }
}
EOF
```

Then run the test with both package files. The image must include the `stdlib-typesafe` functions
of the Flink SQL Runner:

```bash
docker run --rm -v "$PWD":/workspace datasqrl/cmd:local test package.json endpoint-override.json
```

Delete `endpoint-override.json` afterwards, as it is not part of the use case. Don't name it
`package*.json`, or `UseCaseCompileTest` picks it up as a separate use case.

The mock logs every request: five for six applications. `app-3` is sent two of the three
questions, `app-4` sends no request, and `app-5` is rejected with a 422.

On Linux, start the mock with `python3 jev_mock.py 8765 0.0.0.0` and add
`--add-host=host.docker.internal:host-gateway` to the `docker run` command, so the container can
reach it.

To regenerate a snapshot after an intended change, delete the `.snapshot` file and rerun the test.
The new snapshot is verified with a second pipeline run.
