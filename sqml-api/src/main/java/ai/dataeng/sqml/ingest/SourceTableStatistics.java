package ai.dataeng.sqml.ingest;

import ai.dataeng.sqml.source.SourceRecord;
import ai.dataeng.sqml.type.Type;
import lombok.ToString;
import lombok.Value;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Value
public class SourceTableStatistics implements Serializable {

    RelationStats relationStats;

    public SourceTableSchema getSchema() {
        SourceTableSchema.Builder builder = SourceTableSchema.build();
        relationStats.collectSchema(builder);
        return builder.build();
    }

    @ToString
    public static class Accumulator implements org.apache.flink.api.common.accumulators.Accumulator<SourceRecord, SourceTableStatistics> {

        //TODO: add statistics for ingest and source datetime
        RelationStats.Accumulator relationAccum;

        public Accumulator() {
            this.relationAccum = new RelationStats.Accumulator();
        }

        private Accumulator(RelationStats.Accumulator relationAccum) {
            this.relationAccum = relationAccum;
        }

        public long getCount() {
            return relationAccum.getCount();
        }

        @Override
        public void add(SourceRecord sourceRecord) {
            relationAccum.add(sourceRecord.getData());
        }

        @Override
        public SourceTableStatistics getLocalValue() {
            return new SourceTableStatistics(relationAccum.getLocalValue());
        }

        @Override
        public void resetLocal() {
            relationAccum.resetLocal();
        }

        @Override
        public void merge(org.apache.flink.api.common.accumulators.Accumulator<SourceRecord, SourceTableStatistics> accumulator) {
            Accumulator acc = (Accumulator) accumulator;
            relationAccum.merge(acc.relationAccum);
        }

        @Override
        public Accumulator clone() {
            return new Accumulator(relationAccum.clone());
        }
    }
}
