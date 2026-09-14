---
slug: p2-validator-relational-introspection
title: "The Bugs That Aren't in the Text: What Deep Relational Introspection Catches"
authors: [matthias]
tags: [technical, DataSQRL]
---

<head>
  <meta property="og:image" content="/img/blog/harness_p2_deep_introspection.jpeg" />
  <meta name="twitter:image" content="/img/blog/harness_p2_deep_introspection.jpeg" />
</head>

# The Bugs That Aren't in the Text: What Deep Relational Introspection Catches

*Part 2 of 4: How a data engineering harness eliminates the AI coding agent errors that survive review.*

## Introduction

"Why do I need that? Isn't Claude Code good enough?"

<img src="/img/blog/harness_p2_deep_introspection.jpeg" alt="Deep introspection of the logic behind a data pipeline can reveal logical flaws >|" width="50%"/>

We get that question a lot. We are building an [open-source data engineering harness](https://docs.datasqrl.com), the tooling and guardrails that a coding agent uses to build data pipelines. Claude Code, Codex, and OpenCode already write plausible data pipeline code. So what is the harness for?

Part 1 was about the seams between systems, and how a transpiler generates them deterministically. Part 2 is about a harder problem.

Some bugs have no correctness condition in the query text at all. The condition lives in the relationship between a query, the data it reads, and how that data changes over time. An agent reads SQL/code as text, and at the text level these bugs are invisible until the production deployment fails.

<!-- truncate -->

## 1. No indexes for the paths the API actually queries

An agent generating a database schema writes the table definitions and stops. It never reasons about which columns the API filters and sorts on, so the serving database has no index matching its access paths. The pipeline is fast on a thousand test rows and collapses into full table scans at production volume.

*Example:* an endpoint that looks up a customer by email scans every customer row on every call. Nothing about the query is wrong. Tail latency goes from milliseconds to seconds as the table grows.

## 2. The wrong primary key, or none at all

Moving a changing table into the database needs two things. A primary key that matches the upstream key, and a write mode that replaces rather than appends. Agents omit the key, pick the wrong columns, or append what is logically a changelog. You get duplicate rows that accumulate forever, or the last writer winning at the wrong grain.

*Example:* an accounts table built from a feed of database changes is written with no primary key. Every account update inserts a new row instead of replacing the previous one, and the API starts returning three conflicting records for the same account.

## 3. Treating a changelog as an event stream, or the reverse

The most consequential property of a source is what kind of table it is. An append-only stream of events, a changelog where later rows replace earlier ones, and a lookup table are three different things, and which operations are valid depends entirely on which one you have. An agent that cannot see the distinction ingests a stream of updates as if each were a new event, or collapses an event stream that was never meant to be deduplicated.

*Example:* the agent reads an account update feed as a plain append stream. Every status change becomes another "account," and the count of active accounts inflates by the number of edits each account has ever received.

## 4. Joins whose answer depends on when they run

Join a stream to a table that changes over time with a plain join, and the result depends on when the join executed rather than on the state of the world when the event happened. It also changes retroactively whenever the other table updates. The query reads correctly. The time semantics are wrong.

*Example:* transactions are joined to accounts with a regular join, so each transaction is enriched with whatever account classification exists at processing time. A Tuesday transaction gets labeled with Thursday's account type, and enrichments already written quietly change after the fact.

## 5. Broken time attributes and stalled watermarks

Stream processing needs every source to carry an event timestamp and a watermark, the signal that tells the engine how far event time has advanced. That time attribute then has to survive joins and aggregations. Agents leave the watermark off a source, fall back to processing time, or break the attribute partway through. Temporal joins stall, windows never fire, or results stop being reproducible.

*Example:* one of three joined sources is declared with an incorrect watermark. The join waits forever for a time that never advances and the pipeline emits nothing. There is no error, just an empty result that reads as "no matching data."

## 6. NOT NULL contracts a nullable source cannot honor

NOT NULL is a contract that has to hold across every system. An agent declares a column non-null downstream of a source or a join that can legitimately produce null. One unmatched row then fails an insert or aborts a batch.

*Example:* an enriched column is declared NOT NULL in the database, but the outer join that produces it leaves the value null for unmatched transactions. The first unmatched row throws a constraint violation and halts the writer.

## 7. Ordering that does not actually order anything

Deduplication and "latest version" logic depend on an ordering column that really does increase over time. Stable results depend on an order being defined at all. An agent that deduplicates on a column which can move in either direction, or that relies on an implicit order, produces results that change between runs and keeps the wrong version of a row.

*Example:* the agent deduplicates products by ordering on price, meaning "the latest one." Price is not monotonic, so the row that survives is whichever one carries the highest price rather than the most recent update. The current product record is simply wrong.

## 8. State that grows forever

Streaming aggregations and joins hold state. An agent that groups on an ever-growing key, omits a retention bound, or aggregates at the wrong grain produces a pipeline whose memory grows without limit until it falls over. That happens long after the demo passed.

*Example:* the agent keys a running aggregation by transaction id. State grows by one entry per transaction forever, and the job degrades and then crashes weeks into production.

## Summary

None of these bugs is visible in the query text. They live in the semantics of the data flow: how data changes over time, what its key is, when its timestamp is valid, which system can run which operator, how much state a step accumulates. An agent reviewing that text approves every one of them. So does a human reviewing them.

A validator doesn't review text. It parses the pipeline into a relational plan and traverses it the way a query optimizer would, and every fix above falls out of that one traversal. It infers keys and pushes them into the write configuration. It tracks timestamps and watermarks through each join and aggregation. It classifies every table as a stream, a changelog, or a lookup, and checks each operator against that. It propagates nullability, verifies that ordering columns actually increase, and reads the query workload to choose indexes. It rejects any plan that puts an operation on a system that cannot run it, or serves data that was never materialized.

When something fails, the validator names the table, the column, and a suggested fix. That matters more than it sounds. In our testing, an agent handed structured feedback fixes the problem far more reliably than one reasoning backward from an opaque runtime error.

The validator is open source, and you can add your own rules: [DataSQRL on GitHub](https://github.com/DataSQRL/sqrl).