package ai.dataeng.sqml.ingest.stats;

import ai.dataeng.sqml.ingest.DatasetRegistration;
import ai.dataeng.sqml.ingest.schema.SchemaConversionError;
import ai.dataeng.sqml.ingest.schema.SourceTableSchema;
import ai.dataeng.sqml.ingest.source.SourceRecord;
import ai.dataeng.sqml.schema2.basic.ConversionError;
import lombok.ToString;

@ToString
public class SourceTableStatistics implements Accumulator<SourceRecord, SourceTableStatistics> {


    final RelationStats relation;
    final DatasetRegistration registration;

    public SourceTableStatistics(DatasetRegistration registration) {
        this.relation = new RelationStats(registration.getCanonicalizer());
        this.registration = registration;
    }

    public SourceTableSchema getSchema() {
        SourceTableSchema.Builder builder = SourceTableSchema.build();
        relation.collectSchema(builder);
        return builder.build();
    }

    public ConversionError.Bundle<StatsIngestError> validate(SourceRecord sourceRecord) {
        ConversionError.Bundle<StatsIngestError> errors = new ConversionError.Bundle<>();
        RelationStats.validate(sourceRecord.getData(),DocumentPath.ROOT,errors, registration.getCanonicalizer());
        return errors;
    }

    @Override
    public void add(SourceRecord sourceRecord) {
        //TODO: Analyze timestamps on record
        relation.add(sourceRecord.getData());
    }

    @Override
    public void merge(SourceTableStatistics accumulator) {
        relation.merge(accumulator.relation);
    }

    public long getCount() {
        return relation.getCount();
    }
}
