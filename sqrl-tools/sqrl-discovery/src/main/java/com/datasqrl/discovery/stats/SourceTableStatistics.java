/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.discovery.stats;

import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.google.common.base.Preconditions;
import java.util.Map;
import lombok.ToString;

@ToString
public class SourceTableStatistics implements
    Accumulator<Map<String,Object>, SourceTableStatistics, Void>,
    Metric<SourceTableStatistics> {

  final RelationStats relation;

  public SourceTableStatistics() {
    this.relation = new RelationStats();
  }


  public ErrorCollector validate(Map<String, Object> data,
      ErrorCollector errors) {
    RelationStats.validate(data, errors, NameCanonicalizer.SYSTEM);
    return errors;
  }

  @Override
  public void add(Map<String, Object> data, Void context) {
    //TODO: Analyze timestamps on record
    relation.add(data, NameCanonicalizer.SYSTEM);
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
      if (field == null) {
        return RelationStats.EMPTY;
      }
      Preconditions.checkNotNull(field, "Could not find nested table: %s", n);
      current = field.types.values().stream()
          .filter(fts -> fts.nestedRelationStats != null)
          .map(fts -> fts.nestedRelationStats)
          .reduce((a, b) -> {
            throw new IllegalStateException("Expected single RelationStats for nested");
          })
          .orElse(RelationStats.EMPTY);
    }
    return current;
  }
}
