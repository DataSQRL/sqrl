package ai.datasqrl.io.sources.stats;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.io.sources.SourceRecord;
import ai.datasqrl.io.sources.dataset.SourceDataset;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import com.google.common.base.Preconditions;
import lombok.ToString;

@ToString
public class SourceTableStatistics implements
    Accumulator<SourceRecord<String>, SourceTableStatistics, SourceDataset.Digest> {

  final RelationStats relation;

  public SourceTableStatistics() {
    this.relation = new RelationStats();
  }

  public ErrorCollector validate(SourceRecord<String> sourceRecord, SourceDataset.Digest dataset) {
    ErrorCollector errors = ErrorCollector.root();
    RelationStats.validate(sourceRecord.getData(), errors, dataset.getCanonicalizer());
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

  public RelationStats getRelationStats(NamePath path) {
    RelationStats current = relation;
    for (int i = 0; i < path.size(); i++) {
      Name n = path.get(i);
      FieldStats field = current.fieldStats.get(n);
      if (field == null) return RelationStats.EMPTY;
      Preconditions.checkNotNull(field,"Could not find nested table: %s",n);
      current = field.types.values().stream()
              .filter(fts -> fts.nestedRelationStats!=null)
              .map(fts -> fts.nestedRelationStats)
              .reduce((a,b) -> {throw new IllegalStateException("Expected single RelationStats for nested");})
              .orElse(RelationStats.EMPTY);
    }
    return current;
  }
}
