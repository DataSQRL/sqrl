package ai.dataeng.sqml.execution.flink.ingest.shredding;

import ai.dataeng.sqml.execution.flink.process.DestinationTableSchema;
import ai.dataeng.sqml.io.sources.SourceRecord;
import ai.dataeng.sqml.type.basic.DateTimeType;
import ai.dataeng.sqml.tree.name.Name;

import java.io.Serializable;
import java.time.Instant;

public interface RecordProjection<T> extends Serializable {

    DestinationTableSchema.Field getField();

    T getData(SourceRecord<Name> record);

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

    RecordProjection INGEST_TIMESTAMP = new TimestampProjection("__ingesttime") {
        @Override
        public Instant getData(SourceRecord record) {
            return record.getIngestTime();
        }
    };

}
