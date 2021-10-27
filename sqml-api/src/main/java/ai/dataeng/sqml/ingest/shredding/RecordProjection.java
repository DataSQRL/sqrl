package ai.dataeng.sqml.ingest.shredding;

import ai.dataeng.sqml.db.DestinationTableSchema;
import ai.dataeng.sqml.ingest.source.SourceRecord;
import ai.dataeng.sqml.schema2.basic.DateTimeType;
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

    RecordProjection DEFAULT_TIMESTAMP = new TimestampProjection("__ingesttime") {
        @Override
        public Instant getData(SourceRecord record) {
            return record.getIngestTime();
        }
    };

}
