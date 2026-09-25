---
slug: p3-testing-framework
title: "Standard Integration Tests Obfuscate Common Errors"
authors: [matthias]
tags: [technical, DataSQRL]
---

<head>
  <meta property="og:image" content="/img/blog/harness_p3_test_simulator.jpeg" />
  <meta name="twitter:image" content="/img/blog/harness_p3_test_simulator.jpeg" />
</head>

# Standard Integration Tests Obfuscate Common Errors

*Part 3 of 4: How a data engineering harness eliminates the AI coding agent errors that survive standard testing.*

## Introduction

"Why do I need that? Isn't Claude Code good enough?"

<img src="/img/blog/harness_p3_test_simulator.jpeg" alt="An event-time simulator replays data pipeline events at their original timestamps to test time-dependent behavior >|" width="50%"/>

We get that question a lot. We are building an [open-source data engineering harness](https://docs.datasqrl.com), the tooling and guardrails that a coding agent uses to build data pipelines. Claude Code, Codex, and OpenCode already write plausible data pipeline code. So what is the harness for?

Parts 1 and 2 were about generating the pipeline correctly and validating it. Part 3 is about the accomplice in every failure so far: the test that passed.

An agent writes a test, runs it against a fixed snapshot of data, sees green, and ships. But a pipeline's hardest bugs only exist in motion which standard tests don't catch, leaving you to troubleshoot in production.

<!-- truncate -->

## 1. Testing on a static snapshot with nothing updating mid-run

The default agent test loads a fixed dataset, runs the pipeline once, and checks the output. Every bug that requires data to change during execution is invisible. This is the largest blind spot, because it makes time-dependent correctness untestable by construction.

*Example:* the time-dependent join from Part 2, where a transaction gets enriched with a later version of an account, cannot reproduce on a static snapshot. Nothing updates while the test runs, so the naive join and the correct join produce identical output. The test proves the wrong code right.

## 2. Tests that depend on the wall clock

Agents write tests against the system time, so the result depends on how fast the machine ran and when the test happened to execute. It passes locally, flakes in CI, and certifies nothing.

*Example:* a windowed aggregation tested against the current timestamp produces different bucket boundaries on every run. The snapshot never matches twice, so the agent "stabilizes" it by loosening the assertion until it no longer tests anything.

## 3. No late or out-of-order data in the test set

Real streams deliver records late and out of order. An agent's test data is conveniently sorted and on time, so lateness handling and retraction logic never get exercised.

*Example:* an event that arrives after its window closed should either update the result or be dropped, depending on the policy. The test never includes one, so a pipeline that quietly discards late data passes while losing records in production.

## 4. Races between interleaved streams

When two streams feed a join, correctness depends on how they interleave. An agent testing each stream in isolation never sees the race.

*Example:* a transaction arrives in the same instant an account flips from "active" to "frozen," and which value the enrichment picks decides whether a fraud check fires. The agent's test loads every account before any transaction, so the race never happens and the bug ships.

## 5. Idle sources and stalled time

A source that goes quiet stalls the progress of event time and can freeze a join indefinitely. An agent's test keeps every source busy, so the stall never appears.

*Example:* in production, one low-volume source stops emitting overnight. Time stops advancing and the joined output silently halts. An always-busy test set cannot produce that condition.

## 6. Updates and deletes never tested

Updates and deletes propagate through a streaming pipeline as retractions of earlier results. An agent that only tests inserts never checks that a later update or delete correctly replaces what came before.

*Example:* a record is inserted, then deleted upstream. A pipeline tested only on inserts keeps emitting the deleted entity, and the API serves a row that no longer exists.

## 7. Production bugs that cannot be turned into a test

When something breaks in production, the fix starts with reproducing it. Without timestamp-accurate replay, a time-dependent failure cannot be reconstructed, so the fix is a guess.

*Example:* a customer reports numbers that drifted last Tuesday. The agent cannot recreate Tuesday's exact sequence of events, patches speculatively, and never confirms the fix worked.

## 8. The rare scenarios only time control can construct

The scenarios most likely to cause an outage are a leap in event time, a burst, a duplicate replay, a backfill colliding with live data. None of them can be built out of static fixtures, so they go untested until they happen.

*Example:* a backfill of historical data is replayed alongside the live stream, and the pipeline double-counts because it was never tested against overlapping time ranges.

## Summary

The thread through all eight is time. The bugs that matter in a data pipeline are time-dependent, and an agent testing on static, on-time, single-stream snapshots is testing the one regime where time cannot hurt it.

A simulator changes what the test runs against. It executes the real generated deployment assets in a container and replays events at their original timestamps, so time-consistent semantics hold and the same input always produces the same output. This allows testing those things standard integration tests cannot: mid-run updates, late and out-of-order data, interleavings and races, idle sources, retraction sequences, and faithful replay of a production incident. Tests run deterministically, on a laptop, in a tight loop with the agent before anything is proposed for deployment.

A race condition that would take weeks to surface in production becomes a test case you write on purpose.

The event-time simulator is part of the open source data engineering harness, so you can replay your own scenarios and break the pipeline yourself: [DataSQRL on GitHub](https://github.com/DataSQRL/sqrl).
