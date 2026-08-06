# Apache Pinot Engine Configuration

Apache Pinot is a real-time OLAP datastore designed for low-latency analytical queries at scale. DataSQRL writes to Pinot OFFLINE tables by building and uploading segments via the Pinot controller REST API.

## Connector Options

| Option | Required | Default | Description |
|---|---|---|---|
| `controller.url` | yes | — | HTTP URL of the Pinot controller, e.g. `http://pinot-controller:9000` |
| `table.name` | yes | — | Name of the target OFFLINE table (without the `_OFFLINE` suffix) |
| `segment.flush.rows` | no | `500000` | Number of rows to buffer before flushing a segment; also flushes on every Flink checkpoint |

## DDL Example

```sql
CREATE TABLE OrderMetrics (
  order_id   BIGINT,
  customer   STRING,
  total      DECIMAL(10, 2),
  ordered_at TIMESTAMP_LTZ(3)
) WITH (
  'connector'          = 'pinot',
  'controller.url'     = 'http://pinot-controller:9000',
  'table.name'         = 'OrderMetrics',
  'segment.flush.rows' = '500000'
);
```

## Prerequisites

The target Pinot schema and OFFLINE table must exist before the pipeline starts. Create them once via the Pinot controller REST API or the Pinot console — DataSQRL does not create Pinot schemas or tables automatically.

## Delivery Guarantee

At-least-once. Segments are uploaded on every Flink checkpoint and when `segment.flush.rows` is reached. Enable Pinot's built-in deduplication if exactly-once semantics are required.

## Usage Notes

- Only **sink** (write) is supported; Pinot cannot be used as a Flink source in DataSQRL
- Column names in the DDL must match the field names in your Pinot schema
- `TIMESTAMP_LTZ` columns are stored as epoch milliseconds (`LONG`) in Pinot; define the corresponding field with `dataType: LONG` in the Pinot schema
- `DECIMAL` columns are converted to `DOUBLE`
- The connector JAR (`pinot-connector-<version>.jar`) must be present in the Flink `lib/` directory or on the job classpath
