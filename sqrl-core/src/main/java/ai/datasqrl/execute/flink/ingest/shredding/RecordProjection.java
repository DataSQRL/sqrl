package ai.datasqrl.execute.flink.ingest.shredding;

import ai.datasqrl.execute.flink.process.DestinationTableSchema;
import ai.datasqrl.io.sources.SourceRecord;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.schema.type.basic.DateTimeType;
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
