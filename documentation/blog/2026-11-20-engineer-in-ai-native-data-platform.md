---
slug: engineer-in-ai-native-data-platform
title: "Requirements, Plan, Build: How a Data Engineer Builds an Operational Data Pipeline with Agents"
authors: [alp]
image: /img/blog/building_pipeline_with_agent.jpeg
tags: [technical, DataSQRL]
---

# Requirements, Plan, Build: How a Data Engineer Builds an Operational Data Pipeline with Agents

*The three steps from a request to a tested pipeline, and what the data engineer decides in each one.*

## Introduction

In an AI-native platform for operational data engineering, agents do most of the building, and the data engineer operates them. Such a platform does not hand the whole job to one agent. It splits the work between agents, each with its own responsibility and its own restrictions. One works with the engineer to write the requirements down. One turns the requirements into a plan, with the technical decisions and the steps to follow, then builds the pipeline from that plan or patches a pipeline that already exists. One sits in the platform, troubleshoots the running pipeline and helps the user find their way around. This post focuses on the stages of building a data pipeline with the [DataSQRL harness](https://docs.datasqrl.com) and the agents built around it, and it shows the role of the data engineer in each stage.

The agents take over much of the work. They do not take over the job. The data engineer knows the business and what the organization needs, and that makes the engineer the operator of those agents: the one who makes the critical decisions.

<img src="/img/blog/building_pipeline_with_agent.jpeg" alt="Four panels show a data engineer and an agent. They write the requirements, review the plan, build the pipeline and look at the completed pipeline." style={{width: '100%'}}/>

The agents and the engineer work on those decisions together. An agent asks its questions in the chat, and it writes its open questions and its assumptions into files along the way. Many of them are about the decisions that shape the pipeline. How long do the sources keep their data? How stale may the results be before something breaks? How much data arrives at peak, and what may the pipeline cost to run? Who is allowed to see the results? The risk starts when an agent settles such a point without telling anyone. A fast agent with a wrong assumption, and no way to reach the person who decides, ships a wrong pipeline faster.

"Human in the loop" is easy to say and usually means a chat window with an approve button. We mean something narrower. Every question the agent has and every decision it makes becomes a file or a prompt that a person answers or reviews, at a point where the work stops until they do. Different agents do the work between those points. A person owns each one.

A complete, high-quality operational data pipeline is built in three steps: the requirements, the plan and the build. Each step below says what the agent does and what you, the data engineer, decide. A video at the end shows the three steps on a simple example.

<!-- truncate -->

## Step 1: The Requirements File Is the Contract

An agent builds what it is told. What it is not told, it guesses. That is true for any software, and it is why the first step is not building. The first step is a conversation that ends in a document.

*Example:* a team asks for a pipeline in one sentence:

> Ingest transcription events and aggregate them.

An agent can start from this sentence, but it has to fill every gap itself. A complete request leaves nothing to guess. It answers each of these questions:

- **How does the data arrive, and in which format?** POST to `/webhooks/transcription`, JSON body.
- **How much data arrives?** About 50 events per minute at peak.
- **Can an event arrive twice, and which field identifies the copy?** At-least-once delivery, deduplicate on `meeting_id`.
- **Which field says when the event happened?** Event time is `completed_at`, in UTC.
- **How long does the pipeline wait for a late event?** Up to two minutes.

The pipeline needs an answer to every one of these questions. If the request does not give the answers, the agent selects them itself. Nobody reviews those answers, unless the agent asks you about them or writes them down.

### Why a file, and not a chat

You and your coding agent build the requirements together. The agent is the one you already work with, extended with the [DataSQRL plugin](https://github.com/DataSQRL/datasqrl-plugin). It asks, you answer, and the result is saved as a file in the project. A file gives you three things that a chat cannot:

- **Nothing gets lost in the chat.** A conversation ends, and an answer that lives only there goes with it. The agent writes every answer you give into the file.
- **You can review and edit it.** You read all the requirements in one place. When a line is wrong, you change it by hand, or you tell the agent in the chat to change it.
- **It is permanent.** You commit the file next to the code. A year later it still says why and how the pipeline was built, on what ground and for which needs.

Over time, the committed files become the design history of the application.

### What a good requirements file contains, and how it guides the agent

- **Goal and scope.** What the pipeline is for, who consumes it, and what is out of scope.
- **Functional requirements.** Numbered statements, one behavior each, each with an acceptance criterion that a test can pass or fail.
- **Sources.** For each one: transport, format, a real sample payload, volume, delivery guarantee, and whether the records are events or updates.
- **Time semantics, entities and keys.** The event-time field, the lateness, the keys and the duplicate rule.
- **Results and API surface.** What the pipeline produces, the queries the consumers ask, and the authorization on each.
- **Test data.** Records that contain the cases that break pipelines: a late arrival, a duplicate, a null, an out-of-order event, an empty group.
- **Non-functional requirements.** Numbers, not adjectives: freshness, latency, workload, budget, reliability, retention, sensitive fields.
- **Hard constraints.** The requirements state what must be computed, not how. A technology is named only when it is not negotiable, such as a Kafka cluster that already exists or a region the data must not leave.
- **Open questions.** Everything that stays open, each with a recommended default and an impact rating.

For an operational data pipeline, the requirements do more than describe features. Some of them are design decisions. One answer changes the architecture that the agent builds and the pattern that it follows.

*Example:* a team asks for a dashboard with realtime numbers. Realtime needs a job that runs all the time, and such a job costs more to build and to run. The agent therefore asks one question first: what breaks if the numbers are one hour old? The team answers that nothing breaks. The requirement becomes "the numbers are at most one hour old", and the agent builds a job that runs once each hour. One answer changed the architecture.

### How you, the data engineer, and the agent build it

The agent gathers before it asks. It reads the repository first: existing scripts, schemas and shared projects, then it reads your own systems, then public documentation for third-party payloads. Then it asks you the questions whose answers change the design or have an important effect on how the pipeline is built. As the data engineer, you are the only source for what the agents you operate cannot find: the business rules, the freshness the consumers expect, the budget, and what "correct" looks like.

Then the agent writes the requirements file into the project. It shows you the open questions, with the default that the planner uses for each one if you say nothing. You answer them in the file or in the chat, or you accept the defaults. The next stage, planning, starts only after you approve.

## Step 2: The Plan Is the Blueprint

An agent that goes straight from the requirements to the code has no written plan. The order of the steps, the technical choices, the data quality checks it adds and the checks it skips exist only in its working memory. Some of that thinking ends up in the logs. But a log is a trace of what the agent thought at each moment. It is not a plan, and nobody can review it before the work starts.

*Example:* an agent starts to build from the requirements. It skips a data-quality check, because it assumes that the source is clean, and it forgets the step that sets up the test runner. Forty minutes in, the machine restarts. Half the files exist. Nobody can say which steps were finished and verified, so the next run starts again from the beginning.

A written plan solves both problems. It is short: a small number of steps, in order, with the technical decisions behind them, the data quality checks and the assumptions of the agent. You read it and correct it before any code exists.

### Who writes the plan

A different agent takes over here: the DataSQRL agent. Your coding agent starts it in planning mode and gives it the requirements file. The DataSQRL agent is a specialized agent. It carries the knowledge about SQRL, the engines and the pipeline designs, and it runs in a container.

Before it plans, the agent surveys the repository. It reads the shared data catalogs first, then the other projects. It learns which tables already exist, what the data catalog or the sibling projects guarantee about them, and which monitoring is already there. When the requirements leave something open, the agent makes an assumption and records it in the plan file, with its reason.

### What the plan contains

The plan is one file in the project, next to the requirements file. It has five parts:

- **The requirements it builds on.** The path of the requirements file, so that the plan and the requirements stay connected.
- **The implementation checklist.** Numbered steps in the order of the work. Each step names the steps it depends on, so a missing step or a wrong order shows in the list. The checklist is also the memory of the agent during the implementation. The agent marks a step as done only after the result compiles and its tests pass. After a stop, the next run continues at the step that was in progress. Without the checklist, nobody can predict or control the steps that the agent follows.
- **Data quality and observability.** What the pipeline checks and counts, what it does not, and what it reuses from a shared data catalog. The agent decides this for every pipeline, also when the requirements do not ask for it.
- **Technical decisions.** The choices that the requirements leave open, such as the engines or the type of window. Each one comes with the constraint that decided it.
- **Assumptions.** Each gap that the agent found, the default that it selected, and the reason. Each assumption has an impact rating: high, medium or low. The rating says how much of the work changes if the assumption is wrong.

The plan says what each step must achieve and why. It leaves the how, such as the names of tables and columns, to the implementation stage.

### What to check before you approve

When the plan is ready, your coding agent gives you a summary and waits. The review has an order:

1. **Read the high-impact assumptions.** A wrong one is the most expensive mistake in the plan.
2. **Read the technical decisions.** Check that each constraint is true for your organization.
3. **Read the checklist.** Look for a missing step, two steps in the wrong order, or a check that the agent skipped for a reason that does not hold.

When something is wrong, you correct it in the plan. You tell the agent in the chat what to change, or you edit the file by hand. The next stage, implementation, starts only after you approve the plan.

## Step 3: The Agent Builds, the Judges Review, You Read and Commit

An earlier series on this blog showed where a general-purpose coding agent fails at building data pipelines:

- **[The seams between the systems break.](/blog/p1-broken-at-seams)** A column exists in the stream processor, in the database and in the API. The agent maps it by hand, and one type does not match. The pipeline compiles, and the demo works.
- **[Some bugs are not in the text of the code.](/blog/p2-validator-relational-introspection)** A missing index, a wrong primary key, a join whose answer depends on when it runs. The SQL reads correctly.
- **[The test that passed proves little.](/blog/p3-testing-framework)** The agent tests on a fixed set of data, with no late event and no update. The bugs that need data in motion stay hidden.
- **[Nobody can review hundreds of files.](/blog/p4-human-understanding-one-sql-file)** The logic of a conventional pipeline is spread over many files in two or three languages. Each line is correct, and the meaning is wrong.

In each of these cases, every check that the agent had was green. The code compiled, the test passed, and the review found nothing. The checks were too weak for a data pipeline.

This is where a deterministic data engineering harness comes in. DataSQRL gives the agent a compiler, a validator and an event-time simulator, so the build, the tests and the review give the same answer on each run. The DataSQRL agent is built for this purpose, on top of these tools. Purpose-built agents of this type are the new frontier in operational data engineering.

### How the DataSQRL agent builds a data pipeline under the hood

After you approve the plan, your coding agent starts the DataSQRL agent again, now in implementation mode. The agent works through the checklist of the plan, and it repeats one loop until the result holds:

1. **Implement.** The agent writes the scripts, the connectors, the configuration, the API and the tests. For each type of work it first loads the instructions that DataSQRL provides for that type.
2. **Compile.** The DataSQRL compiler checks the full pipeline, from the sources to the API. An error sends the agent back to the code.
3. **Test.** The agent runs the tests on the test data from the requirements, with the late event, the duplicate and the null. A failed test sends the agent back to the code.
4. **Verify.** Two types of judges review the result. Deterministic judges are code, and they give the same answer on each run: they check the structure of the project and the configuration of each environment. LLM judges read the result as a reviewer does. Each one looks at one thing: the code, the compiled pipeline, the configuration or the tests. A pipeline in a domain such as finance gets one more LLM judge with the rules of that domain. Each issue that a judge finds has a severity, from critical to low. The two types together make the build stronger, and the pipeline is complete and ready to use.
5. **Refine.** The agent gets the issues in the order of their severity. It fixes an issue. Then the loop starts again.

The loop ends when the judges accept the result, or when it reaches the limit of refinements.

### What keeps the run under control

- **The agent writes only into your project.** It runs in a container that can read the full repository. A shared data catalog and the projects of other teams are read-only for it. "Do not modify" is then a property of the file system, and no longer an instruction that the agent can ignore.
- **The checklist records each step.** The agent marks a step as done only after the result compiles and its tests pass. After a stop, the next run continues at the step that was in progress.
- **You can follow the run.** The run writes a log into the project, with one line for each event. You can ask your coding agent at any time how far the run is, and you can tell it to stop the run.

### What you get, and what you decide

The run ends with two things: the changed files in your project, and a report. The agent commits nothing.

The report says if the last test run passed. It gives the number of refinements, and the issues that the judges found, counted by severity. It also says so when a check did not run. The project has a README with the commands to compile, test and run the pipeline, and a script that runs all the tests.

Then the work is yours again:

1. **Read the report.** An open issue of high severity is a reason to look at that part of the code first.
2. **Read the diff.** It contains everything that the agent changed, and nothing outside your project.
3. **Commit.** The commit is your decision that the pipeline is correct. A deployment is a separate request, and it starts from a commit that you made.

Not every change needs all three steps. A small and clear change to a project that already exists, such as a new column or a corrected filter, runs as a patch. The request is the requirement. The agent makes the change, updates the tests and the documentation that the change affects, and runs the tests.

## Summary

Building a complete operational data pipeline takes three steps. Each step leaves something that you can read, edit and refuse:

- **The requirements file is the contract.** You and your coding agent write it together. It says what the pipeline must do, in numbers and not in adjectives.
- **The plan is the blueprint.** The DataSQRL agent writes it. It shows the steps, the technical decisions and each assumption, before any code exists.
- **The build is a diff.** The DataSQRL agent implements, compiles, tests and verifies in a loop, and deterministic judges and LLM judges review the result. You read the report and the diff, and you commit.

The job of the data engineer moves. Less typing, more deciding: what the pipeline must do, whether the plan to build it is sound, and whether the result is correct. Those were always the parts that needed an engineer.

The harness is open source, and the finance data catalog example is public. If you try it on a pipeline of your own, we would like to hear how it went: [DataSQRL on GitHub](https://github.com/DataSQRL/sqrl).