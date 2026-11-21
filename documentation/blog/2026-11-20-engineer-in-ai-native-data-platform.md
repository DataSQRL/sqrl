---
slug: engineer-in-ai-native-data-platform
title: "The Engineer in the AI-Native Data Platform: You Decide, the Agents Build, Diagnose and Propose"
authors: [alp]
tags: [technical, DataSQRL]
---

<!--
<head>
  <meta property="og:image" content="/img/blog/TODO_post2_human_in_the_loop.jpeg" />
  <meta name="twitter:image" content="/img/blog/TODO_post2_human_in_the_loop.jpeg" />
</head>
-->

# The Engineer in the AI-Native Data Platform: You Decide, the Agents Build, Diagnose and Propose

*Part 2 of 2: What it takes for a data platform to be AI-native, and who stays in charge.*

## Introduction

A fast agent with a wrong assumption ships a wrong pipeline faster.

[Part 1](/blog/same-hard-problems-different-names) showed a coding agent building pipelines in different domains with the [DataSQRL harness](https://docs.datasqrl.com). It left one thing out: the engineer who gets paged when one of those pipelines breaks. If the agent writes everything, what does that engineer still decide?

<!-- 
TODO
<img src="/img/blog/TODO_post2_human_in_the_loop.jpeg" alt="A loop of five stages, requirements, plan, implementation, deployment and operation, with a person standing at the gate between each." width="50%"/> 
-->

More than before, as it turns out. The risk does not sit in the code the agent writes. It sits in the decisions the agent makes without telling anyone: what "realtime" means, which column is the key, what to do about the table nobody specified.

"Human in the loop" is easy to say and usually means a chat window with an approve button. We mean something narrower. Every decision the agent makes becomes a file or a prompt that a person reviews, at a point where the work stops until they do. Different agents do the work between those points. A person owns each one.

This post walks through the ways an agent-built pipeline gets away from its owner, and the stop that prevents each.

<!-- truncate -->

## 1. The requirement nobody wrote down

An agent builds what it is told. What it is not told, it guesses or halucinates. Most pipeline requests leave out the parts that decide the architecture: the key, the event time, how late data can be, what happens to a duplicate.

*Example:* the request is "ingest transcription events and aggregate them." The version that can actually be built reads: POST to `/webhooks/transcription`, JSON body, about 50 per minute at peak, at-least-once delivery, deduplicate on `meeting_id`, event time is `completed_at` in UTC, up to two minutes late. Every clause in the second version is a decision someone made. In the first version the agent makes all of them.

So the first stage is not building. It is a conversation that ends in a document. The agent reads the repository first, then your own systems, such as ticket trackers and schema registries, then public documentation for third-party payloads. It asks you last, in one batch, starting with the questions whose answers would change the design. The result is a requirements file in the repository, and it works from one rule: what the file specifies is what gets built, and what it leaves open becomes a recorded assumption.

## 2. Words that hide a decision

Some requirements sound complete and are not. "Realtime." "Scalable." "Handle errors gracefully." Each one hands a decision to whoever reads it next.

*Example:* a team asks for a realtime dashboard. The agent asks what would break if the numbers were five minutes old. Nothing would. The pipeline that gets built is simpler and cheaper to run, and nobody had to argue about it later.

The requirements stage pushes back on these words on purpose. Freshness, latency and volume are written as numbers. Every functional requirement gets an acceptance criterion a test can pass or fail. Requirements can also carry hard constraints, such as a Kafka cluster that already exists, a table format the analytics team depends on, or a region the data must not leave. The agent confirms each one with you and writes it into its own section of the file, so that all of them are settled before any planning or implementation starts. And the test data must contain the cases that break pipelines: a late arrival, a duplicate, a null, an out-of-order event, an empty group.

## 3. An answer that lives only in the chat

You answer the agent's question in the conversation. The conversation ends. The next agent, or the next engineer, never sees it.

*Example:* you tell the agent that failed sessions must never be invoiced. It is not in the requirements file. The planning agent, which runs separately and reads only the file, decides for itself.

Every question that stays open is written into the requirements with a recommended default and an impact rating, so you can see what the planner will assume if you say nothing. Every answer you give in chat is written into the file before anything else happens. The file is the actual handoff, and the chat is how you work on that file.

## 4. A plan that exists only in the agent's head

An agent that goes straight from the requirements to the code has no written plan. It may work out the steps in its thinking, or it may start with whichever part of the pipeline comes to mind first. Either way, the order of the steps, the choices, decisions about the data quality and observability, and the checks it decided to skip exist only in the agent's working memory.

Some of that thinking does end up in the logs. But a log is a trace of what the agent thought at each moment, in the order it thought it. It is not a plan. What an engineer needs for a review is the structured version: a small number of high-level steps, in order, together with the technical decisions behind them, the data quality checks and observability the pipeline will have, and the assumptions the agent made. All of it can then be read and corrected before any work starts.

*Example:* an agent starts building from the requirements. It decides on its own to skip a data-quality check, because it assumes the source is clean, and it forgets the step that sets up the test runner. Forty minutes in, the machine restarts. Half the files exist. Nobody can say which steps were finished and verified, so the next run starts again from the beginning.

So the plan is a file, and it is written before any code. It is short on purpose: a decomposition of the work that an engineer can review in a few minutes. A plan like this can have five parts.

- **The requirements it builds on,** by path, so that the plan and the requirements cannot drift apart.
- **The implementation checklist.** Numbered steps, each with the steps it depends on. A step that only applies in some cases, such as "if the tests fail", carries its condition.
- **Data quality and observability.** What the pipeline will check and count, what it deliberately will not, and what it reuses from a shared catalog.
- **Technical decisions.** The choices the requirements left open, such as the engine or the type of window, each with the constraint that decided it.
- **Assumptions.** Everything the planner had to guess. The next section is about those.

This is where the engineer catches what an unwritten plan would hide. A missing step. Two steps in the wrong order. A check that was skipped for a reason that does not hold. The engineer fixes it, either by telling the agent what to change or by editing the file by hand. Only then is it safe to start the implementation.

The same file has a second job. The checklist is the memory of the run. Every step is a checkbox with four states: not started, in progress, done, and skipped. The agent marks a step as in progress before it starts the work, and as done only after the result compiles and its tests pass. If the run stops for any reason, such as a power cut, a restart or a server that runs out of memory, the next run reads the checklist, continues at the step that was in progress, and does not redo a finished step.

Next to the plan, the run writes a log into the project: one line per event, each with the time since the run started. It is a plain text file. When a run stops, the log shows what the agent was doing at that moment, and the checklist shows where to continue.

## 5. Assumptions made in silence

No requirements document is complete, so the agent will fill gaps. It has to: a question that stays open would stop the work. The risk is not that the agent decides. The risk is that nobody finds out what it decided.

That is why it helps to have the assumptions as a part of the plan. It is also the part that deserves the closest look from the engineer. For every gap, the planner picks a concrete default and writes it down with the reason and an impact rating. The rating says how much of the work would change if the guess is wrong.

*Example:* we gave the agent a five-line request to complete the [finance data catalog](https://github.com/datasqrl-colab/finance-data-catalog-demo) from Part 1. Before it planned anything, it surveyed the repository. It found a whole line of business that was never wired in, a table named with a reserved word, three datasets nothing referenced, and an ontology without a single data-quality check. Its plan listed twelve assumptions. Three of them:

- The table named `Transaction` has to be renamed. Assumed `Account_Transaction`. Impact: high.
- The card transaction tables reference a debit card that no dataset in the catalog defines. Assumed that no such dataset should be invented, since "inventing a table nobody specified would add a fictitious contract to a governance artefact." The gap goes into the README instead. Impact: high.

The second one is the interesting one. The easy path was to generate the missing table. The plan declines and says why.

For the review, this gives the engineer an order to work in. Read the high-impact assumptions first, because those are the ones that are expensive to get wrong. If one of them is wrong, correct it in the plan, through the agent or by hand, before the implementation builds on it.

## 6. One agent doing everything

A single agent that gathers requirements, plans, writes the code, tests it and judges its own result has no boundary where a mistake gets caught. It also has write access to everything.

*Example:* asked to fix a failing test in a data product, the agent edits the shared catalog that three other teams depend on. The test passes.

The work is split. The coding agent you talk to gathers requirements and orchestrates. It never writes SQRL. Planning and implementation run in a separate, specialized agent that carries the SQRL knowledge, the validator and the test loop: implement, compile, test, judge, refine. That agent runs in a container that can write to the project folder and nothing else. A shared data catalog or a project are mounted read-only, which turns "do not modify" from an instruction into a property of the file system.

Not every change needs the full path. A small, clearly scoped change to an existing project, such as a new column or a corrected filter, runs as a patch. A patch has no need planning stage. The request itself is the requirement, and the agent makes the change, updates the tests and the documentation that the change affects, and runs the tests.

## 7. Changes that reach production on their own

The build succeeded and the tests pass. That is not the same as a decision to ship.

*Example:* an agent finishes an implementation, commits, pushes and replaces the running deployment, because the original request said "build and deploy."

The implementation ends with uncommitted changes and a report. The report includes the issues the reviewing step found, and says so when a check did not run. You read the diff and you commit. Deploying is a separate request, and it only deploys a commit you have pushed. A new deployment runs next to the live one and never replaces it. Promoting it is an another request. Before it happens you are shown the deployment that is currently live, and the confirmation has to name both.

## 8. A diagnosis that turns into an unreviewed fix

The pipeline is running and something goes wrong. An assistant that can read the logs is useful. An assistant that can also change the deployment is a second, unreviewed path into production.

*Example:* an alert fires for a stalled job. An assistant with write access restarts it with different settings. The symptom goes away and the cause stays.

In DataSQRL Cloud, the assistant agent reads the logs, the metrics, the compiled pipeline and the deployment history, and looks for the earliest error. Every tool it has on the platform is read-only. It cannot pause, resize, deploy or promote. When the cause is in the pipeline, it drafts a GitHub issue and asks whether to create it. You can accept, reject, or reject with a comment, and the comment goes back into the draft.

The issue then goes back to the beginning. The engineer hands it to the coding agent, it becomes requirements, a plan or a patch, a diff, a deployment. Every stop above applies again. There is one path into production, and the operational loop joins it at the start.

## Summary

None of this slows the agent down where it is good. It still writes the pipeline, the tests and the documentation, and it does so in minutes. What changes is that its decisions stop being invisible.

Requirements are a file, written with the person and reviewed by them. The plan is a file, with its assumptions listed and ranked. The implementation is a diff nobody committed for you without an approval. A deployment is isolated until someone promotes it. A production problem becomes an issue someone approved. Every one of those is something a person can read, edit and refuse, and each one is still there next month when someone asks why the pipeline works the way it does.

The engineer's job moves. Less typing, more deciding: what the pipeline must do, whether the plan to build it is sound, and what is allowed into production. Those were always the parts that needed an engineer.

The harness is open source, and the examples from both posts are public. If you try it on a pipeline of your own, we would like to hear how it went: [DataSQRL on GitHub](https://github.com/DataSQRL/sqrl).
