package ai.datasqrl.io.sources.stats;

import ai.datasqrl.io.sources.SourceRecord;
import ai.datasqrl.io.sources.dataset.SourceDataset;
import ai.datasqrl.config.error.ErrorCollector;
import lombok.ToString;

@ToString
public class SourceTableStatistics implements Accumulator<SourceRecord<String>, SourceTableStatistics, SourceDataset.Digest> {


    final RelationStats relation;

    public SourceTableStatistics() {
        this.relation = new RelationStats();
    }

    public ErrorCollector validate(SourceRecord<String> sourceRecord, SourceDataset.Digest dataset) {
        ErrorCollector errors = ErrorCollector.root();
        RelationStats.validate(sourceRecord.getData(),errors, dataset.getCanonicalizer());
        return errors;
    }

    @Override
    public void add(SourceRecord<String> sourceRecord, SourceDataset.Digest dataset) {
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
