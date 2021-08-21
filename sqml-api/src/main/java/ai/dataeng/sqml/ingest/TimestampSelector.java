package ai.dataeng.sqml.ingest;

import ai.dataeng.sqml.source.SourceRecord;

import java.io.Serializable;
import java.time.Instant;

public interface TimestampSelector extends Serializable {

    static final TimestampSelector SOURCE_TIME = new TimestampSelector() {
        @Override
        public Instant getTimestamp(SourceRecord record) {
            return record.getSourceTime();
        }
    };

    static final TimestampSelector INGEST_TIME = new TimestampSelector() {
        @Override
        public Instant getTimestamp(SourceRecord record) {
            return record.getIngestTime();
        }
    };

    Instant getTimestamp(SourceRecord record);

}
