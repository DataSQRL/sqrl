# Key DataSQRL Concepts

This document explains some of the key concepts in streaming data processing.

## Time Handling

Time is the most important concept in DataSQRL because it determines how data streams are processed.

For stream processing, it is useful to think of time as a timeline that stretches from the past through the present
and into the future. The blue line in the picture below visualizes a timeline.

<img src="/img/docs/timeline.svg" alt="SQRL Timeline" width="100%" />

The blue vertical bar on the timeline represents the present. The present moment continuously advances on the timeline.

Each stream record processed by the system (shown as grey circles) is associated with a point on the timeline
before the present. We call this point in time the **event time** of a stream record.
The event time can be the time when an event occurred, a metric was observed, or a change happened.
It is a fixed point in time on the timeline and it must be older than the present.

However, records may come in out of order. Or we might be processing records for multiple different systems
(or different partitions of the same system) and hence the records can have different event time.

To synchronize the disparate event times, we need another concept: the watermark. The watermark is shown as the orange
line on the timeline and it provides a simple but powerful guarantee: there will be no more records older than the watermark.
Hence, the watermark is always smaller than the present. It may be significantly older.
The watermark allows the streaming processor to make progress in time, because it is guaranteed to not encounter
older records than the watermark which would require it to revise previously computed data.

That's why it is important to carefully define the watermark. In most cases, the watermark is defined relative
to records that have been received, for instance setting the watermark to 5 minutes older than the newest
record's event time. This is called a bounded out-of-orderedness watermark, which means that while we cannot guarantee
that records arrive in order we can guarantee that the out of order records will be at most 5 minutes older.

Watermarks play a key role in synchronizing data across joins, closing time windows, and combining streams.

In DataSQRL, you define the watermark for external sources.
For tables exposed as mutations in the API, DataSQRL handles the watermarks for you.
