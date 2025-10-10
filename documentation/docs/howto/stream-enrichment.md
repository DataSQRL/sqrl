# Enriching Data Streams

A common requirement is to enrich a stream of events with dimensional data associated with those events in a time-consistent manner.

For example, suppose we want to enrich transactions with user account balances:

```sql
CREATE TABLE Transaction (
    `txid` BIGINT NOT NULL,
    `accountid` BIGINT NOT NULL,
    `amount` DECIMAL(10,2) NOT NULL,
    `timestamp` TIMESTAMP_LTZ(3) NOT NULL,
    WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '0.001' SECOND
) WITH (...);

CREATE TABLE AccountBalanceUpdates (
    `accountid` BIGINT NOT NULL,
    `balance` DECIMAL(15,2) NOT NULL,
    `lastUpdated` TIMESTAMP_LTZ(3) NOT NULL,
    WATERMARK FOR `lastUpdated` AS `lastUpdated` - INTERVAL '0.001' SECOND
) WITH (...);
```

Those can be internal or external table sources with or without connector configuration.
The important piece is that Transaction is a stream of transaction events and AccountBalanceUpdates is a changelog stream for the AccountBalance entity related to the transaction stream by accountid.

To join in the account balance to the transaction, we want to ensure that we get the balance **at the time of the transaction** for the join to be consistent in time.

To accomplish this, we first have to convert the append-only AccountBalanceUpdates stream to a versioned state table by distincting the stream on primary key:

```sql
AccountBalance := DISTINCT AccountBalanceUpdates ON accountid ORDER BY lastUpdated DESC;
```

We can then join the versioned state table to the stream with a temporal join:

```sql
EnrichedTransaction := SELECT t.*, a.* FROM Transaction t
                       JOIN AccountBalance FOR SYSTEM_TIME AS OF t.`timetamp` a ON a.accountid = t.accountid;

```