package ai.dataeng.sqml.ingest.shredding;

import ai.dataeng.sqml.db.DestinationTableSchema;
import ai.dataeng.sqml.source.SourceRecord;
import ai.dataeng.sqml.type.DateTimeType;

import java.io.Serializable;
import java.time.Instant;

public interface RecordProjection<T> extends Serializable {

    DestinationTableSchema.Field getField();

    T getData(SourceRecord record);

    abstract class TimestampProjection implements RecordProjection<Instant> {

        private final String name;

        public TimestampProjection(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        @Override
        public DestinationTableSchema.Field getField() {
            return DestinationTableSchema.Field.simple(name, DateTimeType.INSTANCE);
        }

    }

    RecordProjection DEFAULT_TIMESTAMP = new TimestampProjection("__ingesttime") {
        @Override
        public Instant getData(SourceRecord record) {
            return record.getIngestTime();
        }
    };

}
