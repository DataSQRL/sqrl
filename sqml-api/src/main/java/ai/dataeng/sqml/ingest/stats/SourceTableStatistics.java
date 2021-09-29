package ai.dataeng.sqml.ingest.stats;

import ai.dataeng.sqml.ingest.DatasetRegistration;
import ai.dataeng.sqml.ingest.schema.FlexibleDatasetSchema;
import ai.dataeng.sqml.ingest.source.SourceRecord;
import ai.dataeng.sqml.schema2.basic.ConversionError;
import ai.dataeng.sqml.schema2.name.Name;
import lombok.NonNull;
import lombok.ToString;

@ToString
public class SourceTableStatistics implements Accumulator<SourceRecord, SourceTableStatistics> {


    final RelationStats relation;
    final DatasetRegistration registration;

    public SourceTableStatistics(DatasetRegistration registration) {
        this.relation = new RelationStats(registration.getCanonicalizer());
        this.registration = registration;
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
