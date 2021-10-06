package ai.dataeng.sqml.ingest.stats;

import ai.dataeng.sqml.ingest.DatasetRegistration;
import ai.dataeng.sqml.ingest.schema.FlexibleDatasetSchema;
import ai.dataeng.sqml.ingest.source.SourceRecord;
import ai.dataeng.sqml.schema2.basic.ConversionError;
import ai.dataeng.sqml.schema2.name.Name;
import lombok.NonNull;
import lombok.ToString;

import java.io.Serializable;
import java.util.Map;

@ToString
public class SourceTableStatistics implements Accumulator<SourceRecord<String>, SourceTableStatistics, DatasetRegistration> {


    final RelationStats relation;

    public SourceTableStatistics() {
        this.relation = new RelationStats();
    }

    public ConversionError.Bundle<StatsIngestError> validate(SourceRecord<String> sourceRecord, DatasetRegistration dataset) {
        ConversionError.Bundle<StatsIngestError> errors = new ConversionError.Bundle<>();
        RelationStats.validate(sourceRecord.getData(),DocumentPath.ROOT,errors, dataset.getCanonicalizer());
        return errors;
    }

    @Override
    public void add(SourceRecord<String> sourceRecord, DatasetRegistration dataset) {
        //TODO: Analyze timestamps on record
        relation.add(sourceRecord.getData(), dataset.getCanonicalizer());
    }

    @Override
    public void merge(SourceTableStatistics accumulator) {
        relation.merge(accumulator.relation);
    }

    public long getCount() {
        return relation.getCount();
    }
}
