# Enriching Data Streams

A common requirement in stream processing is to enrich a **STREAM** of events with dimensional data in a time-consistent manner.
This pattern is particularly useful when you need to join real-time events with slowly changing dimensional data while maintaining temporal consistency.

## Use Case: Transaction Enrichment

Suppose we want to enrich transaction events with the account balance that was valid **at the time of the transaction**.
This ensures we get consistent, point-in-time data for analysis.

## Defining Source Tables

First, define your data sources.
These can be internal tables (managed by DataSQRL) or external tables with connector configuration:

```sql
-- Transaction events stream (STREAM type)
/*+ engine(kafka) */
CREATE TABLE Transaction (
    `txid` BIGINT NOT NULL,
    `accountid` BIGINT NOT NULL,
    `amount` DECIMAL(10,2) NOT NULL,
    `timestamp` TIMESTAMP_LTZ(3) NOT NULL,
    WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '0.001' SECOND
);

-- Account balance updates stream (STREAM type)  
/*+ engine(kafka) */
CREATE TABLE AccountBalanceUpdates (
    `accountid` BIGINT NOT NULL,
    `balance` DECIMAL(15,2) NOT NULL,
    `lastUpdated` TIMESTAMP_LTZ(3) NOT NULL,
    WATERMARK FOR `lastUpdated` AS `lastUpdated` - INTERVAL '0.001' SECOND
);
```

> **Note:** These examples assume a streaming source with a monotonic event-time column. For **file-based sources** (`filesystem`/`iceberg`/`hudi`/`deltalake`), do not blindly watermark a data column — see "File-based sources: watermarks & snapshot aggregation" in [SKILL.md](../SKILL.md) before choosing a `WATERMARK`.

## Creating a Versioned State Table

The `Transaction` table is a **STREAM** of immutable transaction events. The `AccountBalanceUpdates` table is also a **STREAM** representing changes to account balances over time.

To perform temporal joins, we need to convert the balance updates stream into a **VERSIONED_STATE** table that tracks the current and historical values for each account:

```sql
AccountBalance := DISTINCT AccountBalanceUpdates ON accountid ORDER BY lastUpdated DESC;
```

This `DISTINCT` operation:
- Converts the append-only stream into a **VERSIONED_STATE** table
- Deduplicates on the `accountid` primary key  
- Orders by `lastUpdated DESC` to ensure the most recent balance is kept for each account
- Maintains the version history for temporal lookups

## Temporal Join for Stream Enrichment

Now we can perform a temporal join to enrich each transaction with the account balance that was valid at the transaction timestamp:

```sql
EnrichedTransaction := SELECT t.*, a.balance, a.lastUpdated as balance_timestamp
                       FROM Transaction t
                       JOIN AccountBalance FOR SYSTEM_TIME AS OF t.`timestamp` AS a 
                       ON a.accountid = t.accountid;
```

The `FOR SYSTEM_TIME AS OF` syntax ensures that:
- Each transaction gets the account balance that was valid at `t.timestamp`
- If no balance record exists at that time, the join returns no result for that transaction
- The join is temporally consistent and deterministic

## Carrying the Whole Dimension Row

When the user needs the dimension record itself rather than a few of its columns, select the relation alias as one nested value (see "Relation alias as ROW" in [SKILL.md](../SKILL.md)):

```sql
EnrichedTransaction := SELECT t.*, a AS account
                       FROM Transaction t
                       JOIN AccountBalance FOR SYSTEM_TIME AS OF t.`timestamp` AS a
                       ON a.accountid = t.accountid;
```

Each transaction now carries an `account` object holding every column of `AccountBalance` as it was at transaction time.
The API exposes it as a nested object, and an `EXPORT` of the table carries it in every record.

Choose between the three ways of attaching dimension data by what the consumer needs:

| Need | Pattern | What the consumer gets |
|------|---------|------------------------|
| The dimension as it was at event time, as one object; the record leaves through an export or feeds a feature vector | `a AS account` on a temporal join: `JOIN AccountBalance FOR SYSTEM_TIME AS OF t.\`timestamp\` AS a`, as in the example above | A snapshot per event, stored as one JSON column and read with no join at query time. The object carries no relationships or filter arguments. |
| A few dimension values the API filters or sorts by | Scalar columns: `a.balance, a.lastUpdated AS balance_timestamp` | Indexable columns usable in `query_by_*` hints and `ORDER BY` |
| The dimension's current state, navigable onward in the API | A relationship: `Transaction.account := SELECT * FROM AccountBalance a WHERE a.accountid = this.accountid;` | A lookup per request that always shows the latest version and can carry its own relationships and arguments |

Combine the first two when both apply: carry the row and also copy the columns the API filters by.

## Key Benefits

This approach provides:

1. **Temporal Consistency**: Each transaction is enriched with the balance that existed at transaction time
2. **Late Data Handling**: The watermark configuration allows for slightly out-of-order events
3. **Efficient Processing**: The VERSIONED_STATE table enables fast temporal lookups
4. **Scalability**: The pattern works with high-volume streams and frequent balance updates

## Variations

For different requirements, you might adjust the pattern:

- **Left Join**: Use `LEFT JOIN` to include transactions even when no balance is available
- **Multiple Dimensions**: Join with multiple VERSIONED_STATE tables for comprehensive enrichment  
- **Window-based Enrichment**: Combine with time windows for aggregated enrichment data
