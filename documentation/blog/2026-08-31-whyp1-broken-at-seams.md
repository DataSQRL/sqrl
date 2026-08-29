---
slug: p1-broken-at-seams
title: "The Pipeline Compiles, the Demo Works, and the Seams Are Quietly Broken"
authors: [matthias]
tags: [agentic, data-engineering]
---

<head>
  <meta property="og:image" content="/img/diagrams/blog/harness_p1_broken_seams.jpeg" />
  <meta name="twitter:image" content="/img/diagrams/blog/harness_p1_broken_seams.jpeg" />
</head>

# The Pipeline Compiles, the Demo Works, and the Seams Are Quietly Broken

*Part 1 of 4: How a data engineering harness eliminates the AI coding agent errors that survive review.*

## Introduction

"Why do I need that? Isn't Claude Code good enough?"

We get that question a lot. We are building an [open-source data engineering harness](https://docs.datasqrl.com), the tooling and guardrails that a coding agent uses to build data pipelines. Claude Code, Codex, and OpenCode already write plausible data pipeline code. So what is the harness for?

This series answers that. It looks at where general purpose coding harnesses fail on data engineering work, and why those failures survive code review.

Part 1 is about the seams. A pipeline spans multiple data systems, and the same fact (a type, a name, an encoding) has to be restated in each one. Restating facts consistently across systems is strict rule-following. The thing doing the restating is a probabilistic model. It gets most of them right. The ones it gets wrong compile cleanly, pass the demo, and surface in production as overflowed numbers and fields that were never there.

A transpiler removes that entire class of error. It derives every boundary asset from one logical model, so the systems cannot disagree. The agent also writes less code, which means fewer tokens and fewer duplicated schema definitions cluttering its context.

## 1. The same column, typed three different ways

A column in a pipeline exists at least three times. Once in the stream processor, once in the database, once in the API schema. Every system has its own type system, and an agent hand-maps between them one file at a time. A 64-bit integer becomes a 32-bit one at the API. A timestamp with a time zone becomes a bare local timestamp. A fixed-precision decimal lands as a floating point number. Nothing errors. The numbers are just wrong at the edges.

*Example:* an agent maps a transaction amount, stored as a 64-bit count of cents, to a 32-bit API type. Every transaction above roughly $21M wraps to a negative number. Every test built on small sample values passes.

A transpiler infers the type once from the logical model and generates the matching type for each system, so the three representations cannot drift apart.

## 2. Query dialects that look the same and mean different things

Every engine speaks its own dialect. They share a vocabulary and disagree in the details: function names, null handling, division, casts, date arithmetic. An agent writes a transformation in one dialect and a serving query in another. Both run, neither complains, but they compute different answers.

*Example:* an integer division floors in the database and returns a fraction in the stream engine. A "share of total" computed in the stream layer and recomputed in a database view disagree for the same row, and nothing in either system flags it.

A transpiler generates each engine's dialect from one logical plan. A single declared computation is translated consistently instead of agent-written twice.

## 3. A field added in one place but not everywhere

Adding one output column is a multi-system edit. The transformation, the connector that moves the data, the database table definition, the API type, and every endpoint that exposes it all have to gain the field together. An agent updates two or three of them and forgets the rest. The column either never reaches the database or arrives and stays invisible to every consumer.

*Example:* you ask for a merchant category on an enriched transaction stream. The agent updates the transformation and the database table but not the API type. The field does not exist for any consumer, and there is no error to point at.

With a transpiler you add the column once. Every downstream asset is regenerated from that one definition.

## 4. Connector configuration where two systems meet

Wherever two systems meet, a connection has to be configured: names, formats, key fields, write mode. This is hand-written glue, and an agent gets it wrong in ways that never fail loudly. A key field that does not match the table's key. An append-only write for something that is logically an update. A missing time attribute.

*Example:* the agent configures the write from the stream layer into the database but never aligns the update key. Concurrent updates race, and the stored row flickers between values.

A transpiler treats every cut between systems as a generated connection. Both sides of every boundary derive from the same source of truth.

## 5. An API contract the data model cannot honor

When the API schema is hand-written separately from the queries behind it, the contract drifts away from the data. A field marked as always present that can return nothing. A list marked non-empty that can be empty. An argument whose type does not match the query parameter. The API now advertises guarantees the pipeline cannot keep, and consumers get errors where they were promised values.

*Example:* the agent declares a customer's order summary as always non-empty. A customer with zero orders violates the contract and fails the whole query.

A transpiler builds the API schema from the data model. When you supply your own schema, it raises a compile error wherever schema and model disagree, rather than letting the mismatch reach consumers.

## 6. Internal columns leaking into the public interface

Tables carry columns the system fills in itself: generated ids, ingest timestamps, computed values. An interface that accepts writes must ask clients for the columns they actually supply and nothing else. An agent hand-writing that interface routinely includes the internal ones, so valid writes are rejected, or omits a real one, so writes arrive incomplete.

*Example:* the write interface for an event asks for the generated id and the ingest timestamp. Every well-formed client insert is rejected for sending fields the server computes on its own.

A transpiler derives the input contract from the table definition and excludes system-populated columns, so the interface matches what a client is meant to send.

## 7. The same operation exposed three ways, three different shapes

Operations are usually published over more than one protocol. An agent hand-coding each one produces views that disagree. An operation present on one protocol and missing on another. A route using the wrong method for its payload. A path whose arguments do not match the query signature. Consumers on one protocol get a broken endpoint, or none at all.

*Example:* a query that takes a filter is exposed as a request that carries the filter in URL parameters. The filter is silently truncated, and every call returns the unfiltered result set.

A transpiler generates every protocol from one authoritative API model, so names, paths, and signatures stay aligned across all of them.

## 8. Identifier casing, quoting, and reserved words

Systems disagree about case sensitivity, quoting, and which words are reserved. An agent carrying a name across them by hand introduces mismatches that break the mapping from API field to stored column. A mixed-case name folded to lowercase by one system no longer matches the code looking for it. A name that is a reserved word parses on one engine and fails on the next.

*Example:* a column named `order` compiles in the stream layer and produces invalid table definitions in the database. The agent "fixes" it by quoting the name in one place and leaving it unquoted in another, and the API field resolves to nothing.

A transpiler applies one identifier mapping across every generated asset, so a logical name resolves to the right form on every system.

## Summary

We could keep going down our list of seam-breaking errors we have seen in agentic implementaitons, but you get the idea: Don't generate probabilistically what can be derived deterministically from a shared ground truth.

That's what the transpiler in the data engineering harness does. It takes one verified logical model as the ground truth and generates every boundary asset from it: the stream transformations, the database schema and queries, the connector configuration at each cut, the API schema, and every protocol endpoint. A type is inferred once and projected into each system. A field added once propagates to every asset. A name maps the same way everywhere. Both ends of a connection come from the same definition.

Because a compiler does that mapping instead of a language model, the entire category of "the systems disagree at the seam" does not need review to catch. It cannot be expressed. The agent decides what the pipeline computes. The transpiler guarantees the boundaries agree.

This has the additional benefit that there is a lot less code for the agent to generate at the seams. That means faster implementation, fewer tokens, less context pollution, and less technical debt.

Want to learn more about what exactly gets generated? The transpiler is part of the open source data engineering harness, so you can read exactly what gets generated: [DataSQRL on GitHub](https://github.com/DataSQRL/sqrl).
