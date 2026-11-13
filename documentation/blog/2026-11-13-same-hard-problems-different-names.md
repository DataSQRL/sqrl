---
slug: same-hard-problems-different-names
title: "Same Hard Problems, Different Names: One Language for Pipelines in Any Domain"
authors: [alp]
tags: [technical, DataSQRL]
---

# Same Hard Problems, Different Names: One Language for Pipelines in Any Domain

*Part 1 of 2: What it takes for a data platform to be AI-native, and who stays in charge.*

## Introduction

An AI-native data platform should give agents a clear way to build, check and change pipelines, while keeping the result understandable to the engineers responsible for it. That starts with how the pipeline is represented.

A pipeline can span a stream processor, a message log, a database, a table format and an API server. Each brings its own considerations: query semantics, data types, update behavior, connector settings and interface contracts. An agent working directly with these components needs instructions for each technology, along with the context to keep their definitions aligned as the pipeline changes.

In [DataSQRL](https://docs.datasqrl.com), SQRL brings the pipeline logic into one language. It is a SQL dialect for describing how data enters the pipeline, how it is transformed and how it is served. The underlying technologies still do the work. The compiler translates the shared definition into stream jobs, database schemas, connections between systems and API endpoints. The agent works with the pipeline as a whole, and the engineer reviews that same definition. Validation and testing give them feedback on it.

The [previous series](/blog/p1-broken-at-seams) explored that foundation: deterministic compilation, validation, testing and human understanding. This post does two things with it. It first describes what an AI-native data platform is and how it should work. It then looks at its reach across domains, through banking, EV charging, recommendations and oil well operations, followed by a satellite telemetry sketch.

Each domain brings its own requirements and business rules. Beneath those differences, recurring data problems appear: records arrive late, reference data changes, several consumers need consistent results, and access has to be controlled, so that each consumer sees only the data it is allowed to see. The examples show how those patterns can be expressed in the same language while keeping the rules of each domain visible.

They also show what a well-designed AI-native platform takes off the engineer's desk. The definitions for each system, the translation between technologies and the configuration that connects them are generated. That leaves the engineer free to focus on the parts that need judgment: the business rules, the contracts between teams, and what a correct result means in their domain. The next post in this series looks at that role in detail: how the engineer works with the agents and the platform.

<!-- truncate -->

## What makes a platform AI-native

The idea is simple to state. An agent is good at some parts of building a pipeline and unreliable at others. An AI-native platform is designed around that difference.

The previous series looked closely at where an agent is weaker:

- **Repeating a fact consistently across systems.** A column has a type in the stream processor, another in the database and another in the API. An agent that writes each by hand gets most of them right, and the ones it gets wrong still compile. ([Part 1](/blog/p1-broken-at-seams))
- **Seeing what is not in the query text.** A missing key, a changelog read as an event stream, or a join whose answer depends on when it runs all look correct on the page. The error is in how the data behaves over time. ([Part 2](/blog/p2-validator-relational-introspection))
- **Testing against time.** An agent tends to test on a fixed, ordered, on-time dataset. Late events, out-of-order events and updates during a run never occur in such a test, so the bugs they cause never show. ([Part 3](/blog/p3-testing-framework))
- **Keeping the meaning together.** When the logic is spread over many files and languages, each file can be correct while the stages disagree about a unit, a grain or a filter. Nobody, agent or person, reads enough of it at once to notice. ([Part 4](/blog/p4-human-understanding-one-sql-file))

So the platform gives the agent the work it is good at, takes over the work it is weak at, checks the result, and keeps the whole thing readable for the person who answers for it.

That leads to three principles.

**1. The pipeline is described once.** The agent states what the pipeline computes, one time, in one place. Everything that can be derived from that description is derived by a program, not written by the agent. A column type, a table definition or a connection between two systems is never typed twice, so two systems have nothing to disagree about.

*In DataSQRL:* the description is a SQRL script, and the compiler derives the rest. In an EV charging pipeline that an agent built in our tests, the agent wrote 338 lines of SQRL. The compiler generated 1,737 lines of deployment assets from them: Flink SQL, Postgres and Iceberg schemas, an OpenAPI specification, server configuration.

**2. The agent gets feedback it can act on.** An agent corrects its own work quickly when it is told exactly what is wrong. A platform built for agents can inspect the pipeline, run it, and answer in specific terms: which table, which column, which event, and what to change. A stack trace from a failed job an hour later is not that.

*In DataSQRL:* a validator reads the pipeline as a relational plan and names the table, the column and the fix. A simulator replays events at their original timestamps, so a test gives the same result on every run.

**3. The person reads what the agent wrote.** Someone is responsible for the pipeline, and that person has to be able to follow it. So AI-native does not mean built for agents only. The material the agent works on and the material the engineer reviews are the same thing. That only helps when the material is easy to read: short enough to go through in one sitting, and written in a language the engineer already knows.

*In DataSQRL:* that is the same SQRL script. It uses SQL syntax, so a data engineer reads it without learning a new language. And it is short, because it holds only the pipeline logic and none of the generated assets. The 338 lines of that EV charging pipeline are the whole pipeline.

In DataSQRL, these principles live in the data engineering harness: the SQRL language, the compiler, the validator and the simulator. The harness is deterministic. The same script always gives the same pipeline and the same test result.

The AI-native platform is the harness together with the agents built around it: a coding agent that turns requirements into SQRL where the harness turns it into a complete pipeline, and an assistant agent that helps operate and monitor the deployed pipeline. The agents work with what differs from one domain to the next: the vocabulary, the business rules, and the regulatory requirements that the engineer states for the pipeline. The harness handles what stays the same: the systems that run the pipeline, and the data problems that return in every domain, such as late records and reference data that changes.

That split is why an engineer can build a pipeline in a new domain with a single tool, the DataSQRL platform. The examples that follow show it in five domains. Each one puts a different SQRL feature in focus:

1. [Banking](#banking): many teams share one data catalog, and the catalog is the contract between them.
2. [EV charging](#ev-charging): a live operations view and a monthly billing run use the same numbers.
3. [Retail](#retail): recommendations from clickstream data and vector embeddings.
4. [Oil and gas](#oil-and-gas): well data served to an AI agent that acts on it.
5. [Space](#space): telemetry from one satellite, received by many ground stations.

## 1. Banking: many teams, one catalog {#banking}

A bank does not have one pipeline. It has lines of business, each with teams, each with datasets that other teams depend on. The hard part is not a join. It is the contract between the team that produces a dataset and the teams that consume it.

In our [public finance catalog demo](https://github.com/datasqrl-colab/finance-data-catalog-demo), that contract is a SQRL file. Each dataset states its grain, its classification, and how each column must be handled.

```sql
/** Primary customer record containing core identity and demographic attributes.
    Grain: one row per customer_id.

    # Columns:
    * customer_id: Unique identifier for the customer @pii
    * date_of_birth: Date of birth @spii @encrypt
    * ssn: Social Security Number @spii @tokenize @glba
    ...
```

Customer, Deposits & Payments, and Lending each own a folder. Around 200 assertions, each a query that must return no rows, state what the producers guarantee: no orphaned foreign keys, no status outside its allowed values. [Data products](https://github.com/datasqrl-colab/finance-demo) built by other teams import the catalog and build on those guarantees instead of re-checking them.

*Example:* the Lending 360 data product gives a customer one view of their mortgages, loans and cards. Its customer table carries no SSN, no tax id and no date of birth. The tags in the catalog told the consuming team, and the agent working for it, what those columns are.

One more detail. Every dataset has a test connector that reads local files and a production connector that reads Kafka or Iceberg. One setting switches every source in the import chain at once. The pipeline that passes its tests is the pipeline that runs in production, with different plumbing underneath.

*What carries over:* the domain grows by teams, not only by tables. The language does not change. A catalog is more SQRL, and the contract is something a compiler and a test runner can check.

## 2. EV charging: one set of numbers, two teams {#ev-charging}

A company collects data from charging stations run by several operators. The unit of everything is a session: one car plugged in, from plug-in to unplug. When a session closes, the operator sends one record for it. For each session the pipeline computes a few numbers, such as the energy delivered, the idle time and the average power.

Two teams use those numbers. Operations wants a view of each station that updates as sessions close, to see whether the station is healthy. Finance wants a billing run each month, with one invoice line per session.

The worry is easy to state: the numbers must not drift between the operations view and the invoice. If each team computes the energy of a session in its own code, the two results will differ sooner or later, and a customer will see one number on a screen and another on a bill.

Here is how the pipeline handles four problems.

**The same numbers for both teams.** The per-session numbers are computed in one script. The operations pipeline and the billing pipeline both start by importing it.

```sql
IMPORT session_features.*;
```

The operations pipeline is a streaming job with an API. The billing pipeline is a batch job that writes its result to Iceberg. They run on different engines, and they still share one definition of every number.

**The price that was valid at the time.** To price a session, the billing pipeline needs the tariff, which is the price list of the operator. Tariffs change. An operator raises its price per kWh on the first of April, and from then on the tariff feed carries a new version.

A session from the end of March must be billed at the March price. A plain join to the tariff table gets this wrong. It matches the session to whatever version is current when the billing run happens, so every March session is billed at the April price. The query runs, the invoice looks fine, and each line is slightly too high.

The pipeline keeps the versions of each tariff, and joins each session to the version that was valid when the session closed.

```sql
Tariff := DISTINCT sources.Tariff ON operator, connector_type ORDER BY valid_from DESC;

_Billed := SELECT s.session_id, s.total_kwh, s.idle_minutes,
                  s.total_kwh * t.price_per_kwh AS energy_cost
  FROM _SessionFeatures s
  JOIN Tariff FOR SYSTEM_TIME AS OF s.event_time t
    ON s.operator = t.operator AND s.connector_type = t.connector_type
  WHERE s.is_failed = FALSE;
```

The first statement says that a tariff is identified by its operator and connector type, and that it has versions ordered by `valid_from`. The `FOR SYSTEM_TIME AS OF s.event_time` in the join picks the version for the moment of each session. Which moment counts, the start of the session or its end, is a business decision. Here it is the end, and that decision sits in one visible place.

**A rule that applies to one team only.** Sometimes a car is plugged in and charging never starts. That is a failed session, and it delivers no energy. Operations wants to see these, because a station that fails a quarter of its sessions needs repair. Finance must never invoice one. The last line of the query above is that rule: `WHERE s.is_failed = FALSE`. The billing pipeline has it and the operations pipeline does not. The two differ in that one place, and a reviewer can see it.

**Sessions that vanish without an error.** The join above keeps a session only when it finds a tariff for it. If a new operator has no tariff yet, or a session is older than the first version of its tariff, the session drops out of the billing run. Nothing fails. The revenue is missing, and somebody notices three months later.

So the pipeline includes a test that counts those sessions.

```sql
UnmatchedSessionsTest := SELECT COUNT(*) AS unmatched_count
  FROM _SessionFeatures s
  LEFT JOIN Tariff FOR SYSTEM_TIME AS OF s.event_time t
    ON s.operator = t.operator AND s.connector_type = t.connector_type
  WHERE s.is_failed = FALSE AND t.operator IS NULL;
```

A left join keeps every session, with or without a tariff. The last condition selects the ones without. The expected count is stored with the tests, so the day a session shows up with no valid tariff, the count changes and the test fails. The team finds out before the invoices go out, not after.

*What carries over:* one definition shared by two consumers, reference data joined at the version that was valid at the time, and a business rule that applies on one path only, written on one line where a reviewer can see it.

## 3. Retail: recommendations from clicks and embeddings {#retail}

A content site wants two kinds of recommendations: pages that people tend to visit together, and pages that match what a particular user has been reading. The second one needs vector embeddings and a similarity search, which usually means another system to wire in.

In the [clickstream example](https://github.com/DataSQRL/datasqrl-examples/tree/main/clickstream-ai-recommendation), both recommenders are about thirty lines in one file. The first is a self-join over a ten-minute window. The second looks like this.

```sql
IMPORT stdlib.openai.vector_embed;

_ContentEmbedding := SELECT *, vector_embed(text, 'text-embedding-3-small') AS embedding FROM Content;

/*+vector_dim(embedding, 1536), index(VECTOR_COSINE, embedding) */
_DistinctContent := DISTINCT _ContentEmbedding ON url ORDER BY update_time DESC;

_UserInterest := SELECT userid, CENTER(embedding) AS interest FROM Clickstream click
                JOIN _DistinctContent FOR SYSTEM_TIME AS OF click.event_time content ON content.url = click.url
                GROUP BY userid;
```

Content gets edited, so the pipeline keeps the latest version of each page. A user's interest is the center of the pages they clicked. And the join uses the version of the page that existed when the user clicked it, not whatever the page says today.

The hint on the second statement is the whole vector index. The compiler turns it into the right index definition in the database. The agent does not write that definition, so it cannot get it wrong.

*What carries over:* an AI feature is one more function in a query. It is not a new system with its own seams.

## 4. Oil and gas: well data for an agent that acts on it {#oil-and-gas}

An operator runs wells that report flow rate, pressure and temperature. An AI agent on the operations side needs to look up a well, see its maintenance history, and be told when a flow rate drops.

The [oil and gas example](https://github.com/DataSQRL/datasqrl-examples/tree/main/oil-gas-agent-automation) has two projects over one shared catalog of wells, work orders and readings. It ingests readings through the API, enriches each one with the well it belongs to, and pushes the low ones out.

```sql
EnrichedFlowRate := SELECT f.*, a.asset_number, a.asset_name, a.description
                    FROM FlowRate f
                    JOIN Assets FOR SYSTEM_TIME AS OF f.event_time a ON f.assetId = a.asset_id;

LowFlowRate := SUBSCRIBE SELECT * FROM EnrichedFlowRate WHERE flowrate < 200;

/**
Returns the flowrate readings for a given well/asset within the specified range order by timestamp decreasing.
 */
FlowRateByRange(assetId BIGINT NOT NULL, minRate DOUBLE NOT NULL, maxRate DOUBLE NOT NULL) :=
               SELECT * FROM EnrichedFlowRate
                WHERE assetId = :assetId AND flowrate >= :minRate AND flowrate <= :maxRate
                ORDER BY event_time DESC LIMIT 100;
```

The join is the same construct as in the retail example. A well's record changes: it gets renamed, reassigned, serviced. Each reading is matched to the record that was valid at the time of the reading.

Look at the comment above the last query. That comment is the tool description the agent reads when the query is exposed over MCP. The query, its parameters and its description are one definition, served to people through GraphQL and REST and to agents through MCP. They cannot drift apart, because there is only one of them.

*What carries over:* the pipeline's consumers are now agents as well as people. They get the same contract from the same file.

## 5. Space: one satellite, many ground stations {#space}

A small satellite reports on its own health: battery voltage, temperature and so on. Each report is a short message called a frame. A frame carries a counter and the time on the spacecraft's clock.

The satellite can only be heard while it passes over a ground station, which takes a few minutes. Networks of ground stations, like the open [SatNOGS](https://satnogs.org/) network, receive the frames by radio and send them to a shared database. The operator wants to know two things: is the battery healthy, and how well is the satellite being heard?

Three things make this data hard to work with.

- **Duplicates.** When two stations are in range, both receive the same frame, and both upload it.
- **Late data.** A station often uploads after the pass is over. The time a station received a frame is not the time the spacecraft produced it.
- **Reference data that changes.** To interpret a frame you need to know where the satellite was. That comes from its orbital elements, the numbers that describe its orbit. Services like [CelesTrak](https://celestrak.org/) publish a new set every few hours, because orbits shift.

Here is how the pipeline handles each one.

**The same frame, heard twice.** The first step removes the duplicates. A frame is identified by the satellite, the kind of frame (`apid`), its counter and its spacecraft time. The pipeline keeps one row for each.

```sql
Frames := DISTINCT TelemetryFrames ON norad_cat_id, apid, seq_count, spacecraft_time ORDER BY received_at DESC;
```

`norad_cat_id` is the catalog number that identifies a satellite. The counter alone is not enough as a key, because it starts over when it reaches its maximum.

**The orbit that was valid at the time.** The pipeline keeps the versions of the orbital elements of each satellite, and joins each frame to the version that was valid when the spacecraft produced it.

```sql
Elements := DISTINCT OrbitalElements ON norad_cat_id ORDER BY epoch DESC;

EnrichedFrames := SELECT f.*, e.mean_motion, e.inclination
                  FROM Frames f
                  JOIN Elements FOR SYSTEM_TIME AS OF f.spacecraft_time e ON f.norad_cat_id = e.norad_cat_id;
```

`epoch` is the moment a set of elements describes. Note which clock the join uses: `spacecraft_time`, not the time the station received the frame. A frame that is uploaded twenty minutes late still gets the orbit of the moment it was produced.

**Battery health, and how well the satellite is heard.** For each ten-minute window, the pipeline gives the lowest battery voltage and the number of stations that heard the satellite.

```sql
BatteryHealth := SELECT norad_cat_id, window_time,
                        MIN(battery_v) AS min_battery_v,
                        COUNT(DISTINCT station_id) AS stations
                 FROM TUMBLE(TABLE TelemetryFrames, DESCRIPTOR(spacecraft_time), INTERVAL '10' MINUTE)
                 GROUP BY norad_cat_id, window_start, window_end, window_time;
```

This query reads the frames as they arrive, before the duplicates are removed, and that is on purpose. The copies are exactly what tells you how many stations were listening.

**An alert when the battery is low.**

```sql
LowBattery := SUBSCRIBE SELECT * FROM EnrichedFrames WHERE battery_v < 6.8;
```

The alert reads the frames after the duplicates are removed, so the alert is about a frame and not about each station that heard it.

On a small test set, two stations hear the same pass, and two of its three frames arrive twice. The pipeline returns three frames for that pass, counts two stations in that window, and gives the frames of a later pass the newer orbit.

*What carries over:* everything. Versions, the join at the valid version, deduplication, the window and the alert are the constructs of the first four domains, with new nouns.

## The same problems under different names

Put the five examples next to each other and the same problems return, each time under a new name.

**Keep the latest version, drop duplicates.** The same record often arrives more than once, and the pipeline has to decide which copy counts. The bank receives change events again when a feed is replayed. The content site receives a page again each time somebody edits it. The oil field receives a new record whenever a well is updated, and the satellite network receives one frame from every station that heard it.

**Join to the version that was valid at the time.** Reference data changes, so an event has to be matched with the version that existed when the event happened. A charging session is priced with the tariff of the day it closed. A click is matched with the page as the user saw it, a reading with the well record of that moment, and a frame with the orbit the satellite was on when it sent the frame.

**Events that arrive late or out of order.** The time a record reaches the pipeline is rarely the time the event took place. A charging session is reported only when it closes. In the bank's production environment, Kafka delivers events out of order. A ground station sends its frames after the pass is over.

**Aggregate over time.** Single readings mean little on their own, so the pipeline summarizes them in windows. The content site counts page visits that fall within ten minutes of each other. The oil field averages pressure over ten seconds, and the satellite pipeline takes the lowest battery voltage of every ten minutes.

**One definition, several consumers.** A number that is computed in two places will sooner or later have two values, so each pipeline defines it once and shares it. The charging company uses the same session numbers for live operations and for monthly billing. The bank builds many data products on one catalog, and the oil field runs two agent backends on one catalog of wells.

**A rule that holds on one path only.** Some rules apply to one consumer and not to another, and the pipeline has to show where. Operations can see failed charging sessions, and finance never invoices them. The customer's SSN is in the bank's catalog, and it is absent from the API that customers use.

**Push instead of poll.** When something needs attention, nobody should have to ask for it. The oil field pipeline sends out each reading with a low flow rate as it arrives, and the satellite pipeline does the same for a low battery voltage.

## Summary

A general-purpose coding agent can learn the vocabulary of your domain from a requirements document in a few seconds. What it cannot do reliably is restate the same type across four systems, pick the join that stays correct when the reference data changes, or notice that a filter exists on one path and not on another. Those are the hard parts, and they are the same hard parts in a bank, a charging network, a web shop, an oil field and a ground station network.

So the division of labor holds everywhere. The agent decides what the pipeline computes and writes it in one language. The compiler generates the systems underneath. The validator and the simulator check the parts that depend on data and time. And the engineer reads one file.

That last step is where the next post picks up. If the agent writes the pipeline, who decides what it should do, and who lets it into production?

The examples in this post are in [datasqrl-examples](https://github.com/DataSQRL/datasqrl-examples) and the [finance catalog demo](https://github.com/datasqrl-colab/finance-data-catalog-demo). The harness itself is here: [DataSQRL on GitHub](https://github.com/DataSQRL/sqrl).
